import asyncio
import os
from typing import TYPE_CHECKING
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import KafkaConnectionError, UnknownTopicOrPartitionError
from styx.common.logging import logging

from worker.egress.base_egress import BaseEgress

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord, RecordMetadata
    from styx.common.types import K, OperatorPartition, V

KAFKA_URL: str = os.getenv("KAFKA_URL", None)
EPOCH_INTERVAL_MS: int = int(os.getenv("EPOCH_INTERVAL_MS", "1"))


class StyxKafkaBatchEgress(BaseEgress):
    worker_id: int

    def __init__(
        self,
        topic_partition_output_offsets: dict[OperatorPartition, int],
        restart_after_failure: bool = False,
        dedup_output_offsets: dict[OperatorPartition, int] | None = None,
    ) -> None:
        self.kafka_egress_producer: AIOKafkaProducer | None = None
        # operator: partition: output offset — only for THIS worker's partitions
        self.topic_partition_output_offsets: dict[OperatorPartition, int] = topic_partition_output_offsets
        # Flat set of request_ids sent before recovery (partition-agnostic).
        # Cross-partition dedup is needed because migration can change which
        # output partition a response is written to.
        self.messages_sent_before_recovery: set = set()
        self.restart_after_failure = restart_after_failure
        # ALL output offsets from the global snapshot — used for dedup scanning
        # so that every worker scans ALL output partitions (not just its own).
        # This is essential because after recovery a request may be replayed on
        # a different worker than the one that originally sent the output.
        self.dedup_output_offsets: dict[OperatorPartition, int] = dedup_output_offsets or {}
        self.batch: list = []
        self.started: asyncio.Event = asyncio.Event()
        # --- debug counters ---
        self._dedup_suppressed: int = 0
        self._dedup_sent: int = 0

    def clear_messages_sent_before_recovery(self) -> None:
        self.messages_sent_before_recovery: set = set()

    async def start(self, worker_id: int) -> None:
        self.worker_id = worker_id
        self.kafka_egress_producer = AIOKafkaProducer(
            bootstrap_servers=[KAFKA_URL],
            client_id=f"W{worker_id}_{uuid.uuid4()}",
            acks=1,
        )
        while True:
            try:
                await self.kafka_egress_producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        if not self.restart_after_failure:
            # Non-recovery path: mark started immediately.
            # Recovery path: started is set later by run_dedup_scan_and_mark_started()
            # after the ReadyAfterRecovery barrier ensures all workers flushed.
            self.started.set()

    async def run_dedup_scan_and_mark_started(self) -> None:
        """Run the dedup scan (if needed) and then mark the egress as started.

        This must be called AFTER all workers have stopped their old protocols
        (i.e. after the ReadyAfterRecovery barrier) so that the dedup scan
        captures all messages flushed by surviving workers.
        """
        if self.restart_after_failure:
            logging.warning(
                f"[DEDUP] Getting messages sent before recovery. "
                f"Scanning ALL output partitions: {self.dedup_output_offsets}",
            )
            await self.get_messages_sent_before_recovery()
            logging.warning(
                f"[DEDUP] Done scanning. Dedup set size: {len(self.messages_sent_before_recovery)}",
            )
        self.started.set()

    async def stop(self) -> None:
        await self.send_batch()
        if self._dedup_suppressed > 0 or self._dedup_sent > 0:
            logging.warning(
                f"[DEDUP] W{self.worker_id} final stats: "
                f"suppressed={self._dedup_suppressed}, sent={self._dedup_sent}, "
                f"remaining_dedup_set_size={len(self.messages_sent_before_recovery)}",
            )
        await self.kafka_egress_producer.stop()

    async def send(
        self,
        key: bytes,
        value: bytes,
        operator_name: str,
        partition: int,
    ) -> None:
        if not self.started.is_set():
            await self.started.wait()
        if key not in self.messages_sent_before_recovery:
            self.batch.append(
                await self.kafka_egress_producer.send(
                    operator_name + "--OUT",
                    key=key,
                    value=value,
                    partition=partition,
                ),
            )
            self._dedup_sent += 1
        else:
            self.messages_sent_before_recovery.discard(key)
            self._dedup_suppressed += 1
            logging.warning(
                f"[DEDUP] W{getattr(self, 'worker_id', '?')} SUPPRESSED key={key!r} "
                f"target_partition={operator_name}--OUT/{partition}",
            )

    async def send_message_to_topic(
        self,
        key: bytes,
        message: bytes,
        topic: str,
    ) -> RecordMetadata:
        if not self.started.is_set():
            await self.started.wait()
        res: RecordMetadata = await self.kafka_egress_producer.send_and_wait(
            topic,
            key=key,
            value=message,
        )
        return res

    async def send_immediate(
        self,
        key: K,
        value: V,
        operator_name: str,
        partition: int,
    ) -> None:
        if key not in self.messages_sent_before_recovery:
            res: RecordMetadata = await self.kafka_egress_producer.send_and_wait(
                operator_name + "--OUT",
                key=key,
                value=value,
                partition=partition,
            )
            self.topic_partition_output_offsets[(operator_name, partition)] = max(
                self.topic_partition_output_offsets[operator_name, partition],
                res.offset,
            )
            self._dedup_sent += 1
        else:
            self.messages_sent_before_recovery.discard(key)
            self._dedup_suppressed += 1
            logging.warning(
                f"[DEDUP] W{getattr(self, 'worker_id', '?')} SUPPRESSED (immediate) key={key!r} "
                f"target_partition={operator_name}--OUT/{partition}",
            )

    async def send_batch(self) -> None:
        if self.batch:
            # send the batch and get the max offset per topic partition in that batch
            for res in await asyncio.gather(*self.batch):
                operator_name = res.topic[:-5]
                key = (operator_name, res.partition)
                self.topic_partition_output_offsets[key] = max(
                    self.topic_partition_output_offsets[key],
                    res.offset,
                )
            self.batch = []

    async def get_messages_sent_before_recovery(self) -> None:
        # Scan ALL output partitions from the global snapshot (not just this
        # worker's).  This ensures cross-worker dedup: a request originally
        # processed on worker A may be replayed on worker B after recovery.
        all_topic_partitions = [TopicPartition(op_name + "--OUT", part) for op_name, part in self.dedup_output_offsets]
        if not all_topic_partitions:
            return
        kafka_output_consumer = AIOKafkaConsumer(
            bootstrap_servers=[KAFKA_URL],
            enable_auto_commit=False,
        )
        kafka_output_consumer.assign(all_topic_partitions)
        while True:
            try:
                await kafka_output_consumer.start()
                for tp in all_topic_partitions:
                    offset = self.dedup_output_offsets[(tp.topic[:-5], tp.partition)] + 1
                    kafka_output_consumer.seek(tp, offset)
                    logging.warning(f"[DEDUP] Seeking {tp} to offset {offset}")
            except UnknownTopicOrPartitionError, KafkaConnectionError:
                await asyncio.sleep(1)
                logging.warning(
                    f"Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second",
                )
                continue
            break
        try:
            current_offsets = await kafka_output_consumer.end_offsets(all_topic_partitions)
            logging.warning(
                f"[DEDUP] End offsets: {current_offsets}",
            )
            all_partitions_done: dict[TopicPartition, bool] = {
                tp: current_offsets[tp] == self.dedup_output_offsets[(tp.topic[:-5], tp.partition)] + 1
                for tp in all_topic_partitions
            }
            logging.warning(f"[DEDUP] Initial partitions_done: {all_partitions_done}")
            per_partition_count: dict[TopicPartition, int] = dict.fromkeys(all_topic_partitions, 0)
            while not all(all_partitions_done.values()):
                result = await kafka_output_consumer.getmany(timeout_ms=EPOCH_INTERVAL_MS)
                for messages in result.values():
                    for msg in messages:
                        per_partition_count[TopicPartition(msg.topic, msg.partition)] += 1
                    self.process_messages_sent_before_recovery(messages, current_offsets, all_partitions_done)
                    if all(all_partitions_done.values()):
                        break
            logging.warning(
                f"[DEDUP] Scan complete. Messages read per partition: {per_partition_count}. "
                f"Total dedup set size: {len(self.messages_sent_before_recovery)}",
            )
        finally:
            await kafka_output_consumer.stop()

    def process_messages_sent_before_recovery(
        self,
        messages: list[ConsumerRecord],
        current_offsets: dict[TopicPartition, int],
        all_partitions_done: dict[TopicPartition, bool],
    ) -> None:
        for message in messages:
            tp = TopicPartition(message.topic, message.partition)
            self.messages_sent_before_recovery.add(message.key)
            if message.offset >= current_offsets[tp] - 1:
                all_partitions_done[tp] = True

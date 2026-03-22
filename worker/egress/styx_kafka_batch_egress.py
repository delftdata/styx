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
        extra_dedup_partitions: list[OperatorPartition] | None = None,
    ) -> None:
        self.kafka_egress_producer: AIOKafkaProducer | None = None
        # operator: partition: output offset
        self.topic_partition_output_offsets: dict[OperatorPartition, int] = topic_partition_output_offsets
        # Flat set of request_ids sent before recovery (partition-agnostic).
        # Cross-partition dedup is needed because migration can change which
        # output partition a response is written to.
        self.messages_sent_before_recovery: set = set()
        self.restart_after_failure = restart_after_failure
        # Extra partitions to scan from offset 0 for dedup only (not tracked
        # in topic_partition_output_offsets).  These are shadow partitions that
        # may have briefly received responses during a migration window.
        self.extra_dedup_partitions: list[OperatorPartition] = extra_dedup_partitions or []
        self.batch: list = []
        self.started: asyncio.Event = asyncio.Event()

    def clear_messages_sent_before_recovery(self) -> None:
        self.messages_sent_before_recovery: set = set()

    async def start(self, worker_id: int) -> None:
        if self.restart_after_failure:
            logging.warning(
                f"Getting messages sent before recovery: {self.topic_partition_output_offsets}",
            )
            await self.get_messages_sent_before_recovery()
            logging.warning("Got messages sent before recovery")
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
        self.started.set()

    async def stop(self) -> None:
        await self.send_batch()
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
        else:
            self.messages_sent_before_recovery.discard(key)

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
        else:
            self.messages_sent_before_recovery.discard(key)

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

    def _build_scan_partitions(
        self,
    ) -> tuple[list[TopicPartition], set[TopicPartition]]:
        """Build the list of output TopicPartitions to scan and the subset that are extra (shadow) partitions."""
        normal = [TopicPartition(op_name + "--OUT", part) for op_name, part in self.topic_partition_output_offsets]
        extra = {TopicPartition(op_name + "--OUT", part) for op_name, part in self.extra_dedup_partitions}
        return normal + list(extra), extra

    def _partition_scan_offset(self, tp: TopicPartition, extra_set: set[TopicPartition]) -> int:
        """Return the offset from which to start scanning a partition."""
        if tp in extra_set:
            return 0
        return self.topic_partition_output_offsets[(tp.topic[:-5], tp.partition)] + 1

    async def get_messages_sent_before_recovery(self) -> None:
        all_topic_partitions, extra_set = self._build_scan_partitions()
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
                    kafka_output_consumer.seek(tp, self._partition_scan_offset(tp, extra_set))
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
                f"Reading from output from: {self.topic_partition_output_offsets} + 1 to {current_offsets}"
                f" (extra dedup partitions: {self.extra_dedup_partitions})",
            )
            all_partitions_done: dict[TopicPartition, bool] = {
                tp: current_offsets[tp] == self._partition_scan_offset(tp, extra_set) for tp in all_topic_partitions
            }
            while not all(all_partitions_done.values()):
                result = await kafka_output_consumer.getmany(timeout_ms=EPOCH_INTERVAL_MS)
                for messages in result.values():
                    self.process_messages_sent_before_recovery(messages, current_offsets, all_partitions_done)
                    if all(all_partitions_done.values()):
                        break
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

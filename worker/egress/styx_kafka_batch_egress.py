import asyncio
from collections import defaultdict
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
    ) -> None:
        self.kafka_egress_producer: AIOKafkaProducer | None = None
        # operator: partition: output offset
        self.topic_partition_output_offsets: dict[OperatorPartition, int] = topic_partition_output_offsets
        # (operator, partition): replied request_ids
        self.messages_sent_before_recovery: dict[TopicPartition, set] = defaultdict(set)
        self.restart_after_failure = restart_after_failure
        self.batch: list = []
        self.started: asyncio.Event = asyncio.Event()

    def clear_messages_sent_before_recovery(self) -> None:
        self.messages_sent_before_recovery: dict[TopicPartition, set] = defaultdict(set)

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
        tp = TopicPartition(operator_name + "--OUT", partition)
        if key not in self.messages_sent_before_recovery[tp]:
            self.batch.append(
                await self.kafka_egress_producer.send(
                    operator_name + "--OUT",
                    key=key,
                    value=value,
                    partition=partition,
                ),
            )
        else:
            self.messages_sent_before_recovery[tp].remove(key)

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
        tp = TopicPartition(operator_name + "--OUT", partition)
        if key not in self.messages_sent_before_recovery[tp]:
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
            self.messages_sent_before_recovery[tp].remove(key)

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
        kafka_output_consumer = AIOKafkaConsumer(
            bootstrap_servers=[KAFKA_URL],
            enable_auto_commit=False,
        )
        output_topic_partitions = [
            TopicPartition(operator_name + "--OUT", partition)
            for operator_name, partition in self.topic_partition_output_offsets
        ]
        kafka_output_consumer.assign(output_topic_partitions)
        while True:
            # start the kafka consumer
            try:
                await kafka_output_consumer.start()
                for topic_partition in output_topic_partitions:
                    kafka_output_consumer.seek(
                        topic_partition,
                        self.topic_partition_output_offsets[(topic_partition.topic[:-5], topic_partition.partition)]
                        + 1,
                    )
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(
                    f"Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second",
                )
                continue
            break
        try:
            # step 1 get current offset
            current_offsets: dict[
                TopicPartition,
                int,
            ] = await kafka_output_consumer.end_offsets(
                output_topic_partitions,
            )
            logging.warning(
                f"Reading from output from: {self.topic_partition_output_offsets} + 1 to {current_offsets}",
            )
            all_partitions_done: dict[TopicPartition, bool] = dict.fromkeys(
                output_topic_partitions,
                False,
            )
            for topic_partition, current_offset in current_offsets.items():
                if (
                    current_offset
                    == self.topic_partition_output_offsets[(topic_partition.topic[:-5], topic_partition.partition)] + 1
                ):
                    all_partitions_done[topic_partition] = True
            while not all(partition_is_done for partition_is_done in all_partitions_done.values()):
                result = await kafka_output_consumer.getmany(
                    timeout_ms=EPOCH_INTERVAL_MS,
                )
                for messages in result.values():
                    self.process_messages_sent_before_recovery(
                        messages,
                        current_offsets,
                        all_partitions_done,
                    )
                    if all(partition_is_done for partition_is_done in all_partitions_done.values()):
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
            if message.offset >= current_offsets[tp] - 1:
                self.messages_sent_before_recovery[tp].add(message.key)
                all_partitions_done[tp] = True
            else:
                self.messages_sent_before_recovery[tp].add(message.key)

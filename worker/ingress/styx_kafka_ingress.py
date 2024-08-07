import asyncio
import uuid

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager
from styx.common.run_func_payload import RunFuncPayload

from worker.ingress.base_ingress import BaseIngress
from worker.sequencer.sequencer import Sequencer


class StyxKafkaIngress(BaseIngress):

    def __init__(self,
                 networking: NetworkingManager,
                 sequencer: Sequencer,
                 worker_id: int,
                 kafka_url: str,
                 epoch_interval_ms: int,
                 sequence_max_size: int):
        self.worker_id = worker_id
        self.sequencer = sequencer
        self.networking = networking
        self.kafka_url = kafka_url
        self.epoch_interval_ms = epoch_interval_ms
        self.sequence_max_size = sequence_max_size

        self.started: asyncio.Event = asyncio.Event()

        self.kafka_consumer: AIOKafkaConsumer = ...
        self.kafka_ingress_task: asyncio.Task = ...

    async def start(self,
                    topic_partitions: list[TopicPartition],
                    topic_partition_offsets: dict[tuple[str, int], int]):
        self.kafka_ingress_task: asyncio.Task = asyncio.create_task(self.start_kafka_consumer(topic_partitions,
                                                                                              topic_partition_offsets))
        await self.started.wait()

    async def stop(self):
        self.kafka_ingress_task.cancel()
        try:
            await self.kafka_ingress_task
        except asyncio.CancelledError:
            logging.warning("kafka ingress coroutine restarting...")
        await self.kafka_consumer.stop()

    def handle_message_from_kafka(self, msg):
        logging.info(
            f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
            f"{msg.key} {msg.value} {msg.timestamp}"
        )
        message_type: int = self.networking.get_msg_type(msg.value)
        if message_type == MessageType.ClientMsg:
            message = self.networking.decode_message(msg.value)
            operator_name, key, fun_name, params, partition = message
            run_func_payload: RunFuncPayload = RunFuncPayload(request_id=msg.key, key=key, timestamp=msg.timestamp,
                                                              operator_name=operator_name, partition=partition,
                                                              function_name=fun_name, kafka_offset=msg.offset,
                                                              params=params)
            logging.info(f'SEQ FROM KAFKA: {run_func_payload.function_name} {run_func_payload.key}')
            self.sequencer.sequence(run_func_payload)
        else:
            logging.error(f"Invalid message type: {message_type} passed to KAFKA")

    async def start_kafka_consumer(self,
                                   topic_partitions: list[TopicPartition],
                                   topic_partition_offsets: dict[tuple[str, int], int]):
        logging.info(f'{self.worker_id} CREATED Kafka consumer for topic partitions: {topic_partitions}')
        # enable_auto_commit=False needed for exactly once
        self.kafka_consumer = AIOKafkaConsumer(bootstrap_servers=[self.kafka_url],
                                               enable_auto_commit=False,
                                               client_id=f"W{self.worker_id}_{uuid.uuid4()}")
        self.kafka_consumer.assign(topic_partitions)
        while True:
            # start the kafka consumer
            try:
                await self.kafka_consumer.start()
                for operator, offset in topic_partition_offsets.items():
                    self.kafka_consumer.seek(TopicPartition(operator[0], operator[1]), offset + 1)
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(f'Kafka at {self.kafka_url} not ready yet, sleeping for 1 second')
                continue
            break
        try:
            # Consume messages
            logging.info(f'{self.worker_id} STARTED Kafka consumer for topic partitions: {topic_partitions}')
            self.started.set()
            while True:
                async with self.sequencer.lock:
                    result = await self.kafka_consumer.getmany(timeout_ms=self.epoch_interval_ms,
                                                               max_records=self.sequence_max_size)
                    # start_seq = timer()
                    for _, messages in result.items():
                        if messages:
                            messages = sorted(messages, key=lambda msg: msg.offset)
                            for message in messages:
                                self.handle_message_from_kafka(message)
                    # end_seq = timer()
                    # self.sequencing_time += end_seq - start_seq
        finally:
            await self.kafka_consumer.stop()

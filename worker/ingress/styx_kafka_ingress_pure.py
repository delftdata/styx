import asyncio
import os

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.networking import NetworkingManager
from styx.common.run_func_payload import RunFuncPayload

from worker.ingress.base_ingress import BaseIngress

KAFKA_URL: str = os.getenv('KAFKA_URL', None)
EPOCH_INTERVAL_MS: int = int(os.getenv('EPOCH_INTERVAL_MS', 1))  # 1ms
SEQUENCE_MAX_SIZE: int = int(os.getenv('SEQUENCE_MAX_SIZE', 100))


class StyxKafkaIngressPure(BaseIngress):

    def __init__(self, networking: NetworkingManager, worker_id: int, n_workers: int, t_counter: int = 0):
        self.queue = asyncio.Queue()
        self.networking = networking

        self.kafka_consumer: AIOKafkaConsumer = ...
        self.kafka_ingress_task: asyncio.Task = ...
        self.worker_id = worker_id
        self.n_workers = n_workers
        self.t_counter = t_counter

    def start(self,
              topic_partitions: list[TopicPartition],
              topic_partition_offsets: dict[tuple[str, int], int]):
        self.kafka_ingress_task: asyncio.Task = asyncio.create_task(self.start_kafka_consumer(topic_partitions,
                                                                                              topic_partition_offsets))

    async def stop(self):
        self.kafka_ingress_task.cancel()
        try:
            await self.kafka_ingress_task
        except asyncio.CancelledError:
            logging.warning("kafka ingress coroutine restarting...")

    async def get_message(self):
        t_id = self.worker_id + self.t_counter * self.n_workers
        self.t_counter += 1
        msg = await self.queue.get()
        return msg, t_id

    def handle_message_from_kafka(self, msg):
        logging.info(
            f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
            f"{msg.key} {msg.value} {msg.timestamp}"
        )
        message_type: int = self.networking.get_msg_type(msg.value)
        if message_type == MessageType.ClientMsg:
            message = self.networking.decode_message(msg.value)
            operator_name, key, fun_name, params, partition = message
            run_func_payload: RunFuncPayload = RunFuncPayload(request_id=msg.key, key=key,
                                                              operator_name=operator_name, partition=partition,
                                                              function_name=fun_name, kafka_offset=msg.offset,
                                                              params=params)
            logging.info(f'SEQ FROM KAFKA: {run_func_payload.function_name} {run_func_payload.key}')
            self.queue.put_nowait(run_func_payload)
        else:
            logging.error(f"Invalid message type: {message_type} passed to KAFKA")

    async def start_kafka_consumer(self,
                                   topic_partitions: list[TopicPartition],
                                   topic_partition_offsets: dict[tuple[str, int], int]):
        logging.warning(f'CREATED Kafka consumer for topic partitions: {topic_partitions}')
        # enable_auto_commit=False needed for exactly once
        self.kafka_consumer = AIOKafkaConsumer(bootstrap_servers=[KAFKA_URL], enable_auto_commit=False)
        self.kafka_consumer.assign(topic_partitions)
        while True:
            # start the kafka consumer
            try:
                await self.kafka_consumer.start()
                for operator, offset in topic_partition_offsets.items():
                    self.kafka_consumer.seek(TopicPartition(operator[0], operator[1]), offset + 1)
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(f'Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second')
                continue
            break
        try:
            # Consume messages
            logging.warning(f'STARTED Kafka consumer for topic partitions: {topic_partitions}')
            while True:
                result = await self.kafka_consumer.getmany(timeout_ms=EPOCH_INTERVAL_MS,
                                                           max_records=SEQUENCE_MAX_SIZE)
                # start_seq = timer()
                for _, messages in result.items():
                    [self.handle_message_from_kafka(message) for message in messages if messages]
                # end_seq = timer()
                # self.sequencing_time += end_seq - start_seq
        finally:
            await self.kafka_consumer.stop()

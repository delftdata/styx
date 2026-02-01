import asyncio
import typing
import uuid

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.errors import KafkaConnectionError, UnknownTopicOrPartitionError
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import Serializer

from worker.ingress.base_ingress import BaseIngress

if typing.TYPE_CHECKING:
    from styx.common.base_state import BaseOperatorState
    from styx.common.operator import Operator
    from styx.common.tcp_networking import NetworkingManager
    from styx.common.types import OperatorPartition

    from worker.sequencer.sequencer import Sequencer


class StyxKafkaIngress(BaseIngress):
    def __init__(
        self,
        networking: NetworkingManager,
        sequencer: Sequencer,
        state: BaseOperatorState,
        registered_operators: dict[OperatorPartition, Operator],
        worker_id: int,
        kafka_url: str,
        epoch_interval_ms: int,
        sequence_max_size: int,
    ) -> None:
        self.worker_id = worker_id
        self.sequencer = sequencer
        self.state = state
        self.registered_operators = registered_operators
        self.networking = networking
        self.kafka_url = kafka_url
        self.epoch_interval_ms = epoch_interval_ms
        self.sequence_max_size = sequence_max_size
        self.send_message_tasks: list[typing.Coroutine] = []

        self.started: asyncio.Event = asyncio.Event()

        self.kafka_consumer: AIOKafkaConsumer = ...
        self.kafka_ingress_task: asyncio.Task = ...

    async def start(
        self,
        topic_partitions: list[TopicPartition],
        topic_partition_offsets: dict[OperatorPartition, int],
    ) -> None:
        self.kafka_ingress_task: asyncio.Task = asyncio.create_task(
            self.start_kafka_consumer(topic_partitions, topic_partition_offsets),
        )
        await self.started.wait()

    async def stop(self) -> None:
        self.kafka_ingress_task.cancel()
        try:
            await self.kafka_ingress_task
        except asyncio.CancelledError:
            logging.warning("kafka ingress coroutine suts down...")
        await self.kafka_consumer.stop()

    def handle_message_from_kafka(self, msg: ConsumerRecord) -> None:
        message_type: int = self.networking.get_msg_type(msg.value)
        if message_type == MessageType.ClientMsg:
            message = self.networking.decode_message(msg.value)
            operator_name, key, fun_name, params, partition = message
            run_func_payload: RunFuncPayload = RunFuncPayload(
                request_id=msg.key,
                key=key,
                operator_name=operator_name,
                partition=partition,
                function_name=fun_name,
                kafka_offset=msg.offset,
                params=params,
                kafka_ingress_partition=msg.partition,
            )
            if key is None or self.state.exists(key, operator_name, partition):
                # Message received in the correct partition (Normal operation)
                self.sequencer.sequence(run_func_payload)
            elif (
                true_partition := self.registered_operators[(operator_name, msg.partition)].which_partition(key)
            ) == partition:
                # Message received in the correct partition, but it was an insert operation (didn't exist in the state)
                self.sequencer.sequence(run_func_payload)
            else:
                # In flight message during migration, currently the state belongs to another partition
                dns = self.registered_operators[(operator_name, msg.partition)].dns
                operator_host = dns[operator_name][true_partition][0]
                operator_port = dns[operator_name][true_partition][2]
                if self.networking.in_the_same_network(operator_host, operator_port):
                    run_func_payload = RunFuncPayload(
                        request_id=msg.key,
                        key=key,
                        operator_name=operator_name,
                        partition=true_partition,
                        function_name=fun_name,
                        params=params,
                        kafka_ingress_partition=partition,
                    )
                    self.sequencer.sequence(run_func_payload)
                else:
                    payload = (
                        msg.key,
                        operator_name,
                        fun_name,
                        key,
                        true_partition,
                        msg.partition,
                        msg.offset,
                        params,
                    )
                    self.send_message_tasks.append(
                        self.networking.send_message(
                            operator_host,
                            operator_port,
                            msg=payload,
                            msg_type=MessageType.WrongPartitionRequest,
                            serializer=Serializer.MSGPACK,
                        ),
                    )
        else:
            logging.error(f"Invalid message type: {message_type} passed to KAFKA")

    async def start_kafka_consumer(
        self,
        topic_partitions: list[TopicPartition],
        topic_partition_offsets: dict[OperatorPartition, int],
    ) -> None:
        logging.warning(
            f"{self.worker_id} CREATED Kafka consumer for topic partitions: {topic_partitions}",
        )
        # enable_auto_commit=False needed for exactly once
        self.kafka_consumer = AIOKafkaConsumer(
            bootstrap_servers=[self.kafka_url],
            enable_auto_commit=False,
            client_id=f"W{self.worker_id}_{uuid.uuid4()}",
        )
        self.kafka_consumer.assign(topic_partitions)
        while True:
            # start the kafka consumer
            try:
                await self.kafka_consumer.start()
                for operator, offset in topic_partition_offsets.items():
                    self.kafka_consumer.seek(
                        TopicPartition(operator[0], operator[1]),
                        offset + 1,
                    )
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(
                    f"Kafka at {self.kafka_url} not ready yet, sleeping for 1 second",
                )
                continue
            break
        try:
            # Consume messages
            logging.warning(
                f"{self.worker_id} STARTED Kafka consumer from offsets (-1): {topic_partition_offsets}",
            )
            self.started.set()
            while True:
                async with self.sequencer.lock:
                    result = await self.kafka_consumer.getmany(
                        timeout_ms=self.epoch_interval_ms,
                        max_records=self.sequence_max_size,
                    )
                    for messages in result.values():
                        if messages:
                            s_messages = sorted(messages, key=lambda msg: msg.offset)
                            for message in s_messages:
                                self.handle_message_from_kafka(message)
                    if self.send_message_tasks:
                        await asyncio.gather(*self.send_message_tasks)
                        self.send_message_tasks = []
        finally:
            await self.kafka_consumer.stop()

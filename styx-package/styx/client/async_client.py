import asyncio
import logging
import sys
import uuid
from typing import Type

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError


from .base_client import BaseStyxClient
from .styx_future import StyxAsyncFuture
from ..common.base_operator import BaseOperator
from ..common.message_types import MessageType
from ..common.serialization import Serializer, msgpack_deserialization, cloudpickle_deserialization
from ..common.stateflow_graph import StateflowGraph
from ..common.tcp_networking import NetworkingManager


class AsyncStyxClient(BaseStyxClient):

    _kafka_producer: AIOKafkaProducer

    def __init__(self,
                 styx_coordinator_adr: str,
                 styx_coordinator_port: int,
                 kafka_url: str):
        super().__init__(styx_coordinator_adr, styx_coordinator_port)
        self._kafka_url = kafka_url
        self._futures: dict[bytes, StyxAsyncFuture] = {}
        self._result_consumer_task: asyncio.Task = ...
        self._result_consumer: AIOKafkaConsumer = ...
        self._metadata_consumer_task: asyncio.Task = ...
        self._metadata_consumer: AIOKafkaConsumer = ...
        self.graph_known_event: asyncio.Event = asyncio.Event()
        self._networking_manager: NetworkingManager = NetworkingManager(None)

    async def get_operator_partition(self, key, operator: BaseOperator) -> int:
        if not self.graph_known_event.is_set():
            await self.graph_known_event.wait()
        return self._current_active_graph.get_operator(operator).which_partition(key)

    async def close(self):
        await self.flush()
        await self._kafka_producer.stop()
        await self._result_consumer.stop()
        await self._metadata_consumer.stop()
        self._result_consumer_task.cancel()
        self._metadata_consumer_task.cancel()
        try:
            await self._result_consumer_task
            await self._metadata_consumer_task
        except asyncio.CancelledError:
            pass

    async def start_result_consumer_task(self):
        if not self.graph_known_event.is_set():
            await self.graph_known_event.wait()
        self._result_consumer: AIOKafkaConsumer = AIOKafkaConsumer(auto_offset_reset='earliest',
                                                                   value_deserializer=msgpack_deserialization,
                                                                   bootstrap_servers=[self._kafka_url],
                                                                   enable_auto_commit=False,
                                                                   fetch_min_bytes=1,
                                                                   group_id=str(uuid.uuid4()))
        await self._result_consumer.start()
        egress_topic_names: list[str] = self._current_active_graph.get_egress_topic_names()
        topics = await self._result_consumer.topics()
        wait_output_topics = True
        while wait_output_topics:
            wait_output_topics = False
            for topic in egress_topic_names:
                if topic not in topics:
                    wait_output_topics = True
            if not wait_output_topics:
                break
            await asyncio.sleep(1)
            topics = await self._result_consumer.topics()
        topics_to_subscribe = [topic for topic in topics if topic.endswith('--OUT')]
        logging.warning(f"Subscribed to topics: {topics_to_subscribe}")
        self._result_consumer.subscribe(topics_to_subscribe)
        # Consume messages
        while True:
            # poll every 10ms (this will add at least 10 ms latency to the futures, but it makes the client lightweight)
            data = await self._result_consumer.getmany(timeout_ms=10)
            for messages in data.values():
                for msg in messages:
                    if msg.key in self._futures:
                        self._futures[msg.key].set(response_val=msg.value,
                                                   out_timestamp=msg.timestamp)

    async def start_consuming_metadata(self):
        self._metadata_consumer: AIOKafkaConsumer = AIOKafkaConsumer(auto_offset_reset='earliest',
                                                                     bootstrap_servers=[self._kafka_url],
                                                                     enable_auto_commit=False,
                                                                     fetch_min_bytes=1,
                                                                     group_id=str(uuid.uuid4()))
        await self._metadata_consumer.start()
        topics = await self._metadata_consumer.topics()
        while 'styx-metadata' not in topics:
            await asyncio.sleep(1)
            topics = await self._metadata_consumer.topics()
        self._metadata_consumer.subscribe(['styx-metadata'])
        while True:
            # poll every 100ms
            data = await self._metadata_consumer.getmany(timeout_ms=100)
            for messages in data.values():
                for msg in messages:
                    metadata = cloudpickle_deserialization(msg.value)
                    if isinstance(metadata, StateflowGraph):
                        self._current_active_graph = metadata
                        logging.warning("Current active graph updated")
                        self.graph_known_event.set()


    async def open(self, consume: bool = True):
        self._metadata_consumer_task = asyncio.create_task(self.start_consuming_metadata())
        self._kafka_producer = AIOKafkaProducer(bootstrap_servers=[self._kafka_url],
                                                max_request_size=134217728,
                                                enable_idempotence=True,
                                                acks="all",
                                                linger_ms=0,
                                                client_id=str(uuid.uuid4()))
        while True:
            try:
                await self._kafka_producer.start()
            except KafkaConnectionError:
                sys.stderr.write("Cannot reach Kafka, sleeping for 1 second...")
                await asyncio.sleep(1)
                continue
            break
        if consume:
            self._result_consumer_task = asyncio.create_task(self.start_result_consumer_task())

    async def flush(self):
        await self._kafka_producer.flush()

    async def send_event(self,
                         operator: BaseOperator,
                         key,
                         function: Type | str,
                         params: tuple = tuple(),
                         serializer: Serializer = Serializer.MSGPACK) -> StyxAsyncFuture:
        request_id, serialized_value, partition = self._prepare_kafka_message(key,
                                                                              operator,
                                                                              function,
                                                                              params,
                                                                              serializer)
        self._futures[request_id] = StyxAsyncFuture(request_id=request_id)
        msg = await self._kafka_producer.send_and_wait(operator.name,
                                                       key=request_id,
                                                       value=serialized_value,
                                                       partition=partition)
        self._delivery_timestamps[request_id] = msg.timestamp
        self._futures[request_id].set_in_timestamp(msg.timestamp)
        return self._futures[request_id]

    async def send_batch_insert(self, operator: BaseOperator, partition: int, function: Type | str,
                                key_value_pairs: dict[any, any], serializer: Serializer = Serializer.MSGPACK) -> bytes:
        request_id, serialized_value, _ = self._prepare_kafka_message(None,
                                                                      operator,
                                                                      function,
                                                                      (key_value_pairs,),
                                                                      serializer,
                                                                      partition=partition)
        msg = await self._kafka_producer.send_and_wait(operator.name,
                                                       key=request_id,
                                                       value=serialized_value,
                                                       partition=partition)
        self._delivery_timestamps[request_id] = msg.timestamp
        return request_id

    async def submit_dataflow(self, stateflow_graph: StateflowGraph, external_modules: tuple = None):
        self._verify_dataflow_input(stateflow_graph, external_modules)
        self._current_active_graph = stateflow_graph
        self.graph_known_event.set()
        await self._networking_manager.send_message(self._styx_coordinator_adr,
                                                    self._styx_coordinator_port,
                                                    msg=(stateflow_graph, ),
                                                    msg_type=MessageType.SendExecutionGraph)

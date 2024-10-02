import asyncio
import sys
import time
import uuid
from typing import Type

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from .base_client import BaseStyxClient
from .styx_future import StyxAsyncFuture
from ..common.base_operator import BaseOperator
from ..common.message_types import MessageType
from ..common.serialization import Serializer, msgpack_deserialization
from ..common.stateflow_graph import StateflowGraph


class AsyncStyxClient(BaseStyxClient):

    _kafka_producer: AIOKafkaProducer

    def __init__(self,
                 styx_coordinator_adr: str,
                 styx_coordinator_port: int,
                 kafka_url: str):
        super().__init__(styx_coordinator_adr, styx_coordinator_port)
        self._kafka_url = kafka_url
        self._futures: dict[bytes, StyxAsyncFuture] = {}
        self._background_consumer_task: asyncio.Task = ...
        self._background_consumer: AIOKafkaConsumer = ...

    async def close(self):
        await self.flush()
        await self._kafka_producer.stop()
        await self._background_consumer.stop()
        self._background_consumer_task.cancel()
        try:
            await self._background_consumer_task
        except asyncio.CancelledError:
            pass

    async def start_consumer_task(self):
        self._background_consumer: AIOKafkaConsumer = AIOKafkaConsumer(auto_offset_reset='earliest',
                                                                       value_deserializer=msgpack_deserialization,
                                                                       bootstrap_servers=[self._kafka_url],
                                                                       enable_auto_commit=False,
                                                                       fetch_min_bytes=1,
                                                                       group_id=str(uuid.uuid4()))
        await self._background_consumer.start()
        topics = await self._background_consumer.topics()
        while 'styx-metadata' not in topics:
            topics = await self._background_consumer.topics()
            print("Awaiting egress topics to be created by the Styx coordinator")
            await asyncio.sleep(1)
        topics_to_subscribe = ['styx-metadata'] + [topic for topic in topics if topic.endswith('--OUT')]
        print(f"Subscribed to topics: {topics_to_subscribe}")
        self._background_consumer.subscribe(topics_to_subscribe)
        # Consume messages
        while True:
            data = await self._background_consumer.getmany(timeout_ms=10)
            for messages in data.values():
                for msg in messages:
                    if msg.key in self._futures:
                        self._futures[msg.key].set(response_val=msg.value,
                                                   out_timestamp=msg.timestamp)

    async def open(self):
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
                time.sleep(1)
                continue
            break
        self._background_consumer_task = asyncio.create_task(self.start_consumer_task())

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
        await self._networking_manager.send_message(self._styx_coordinator_adr,
                                                    self._styx_coordinator_port,
                                                    msg=(stateflow_graph, ),
                                                    msg_type=MessageType.SendExecutionGraph)

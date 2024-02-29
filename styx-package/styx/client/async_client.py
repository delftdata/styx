import sys
import time
from typing import Type

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from .base_client import BaseStyxClient
from ..common.base_operator import BaseOperator
from ..common.message_types import MessageType
from ..common.serialization import Serializer
from ..common.stateflow_graph import StateflowGraph


class AsyncStyxClient(BaseStyxClient):

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

    _kafka_producer: AIOKafkaProducer

    def __init__(self,
                 styx_coordinator_adr: str,
                 styx_coordinator_port: int,
                 kafka_url: str):
        super().__init__(styx_coordinator_adr, styx_coordinator_port)
        self.kafka_url = kafka_url

    async def close(self):
        await self.flush()
        await self._kafka_producer.stop()

    async def open(self):
        self._kafka_producer = AIOKafkaProducer(bootstrap_servers=[self.kafka_url],
                                                enable_idempotence=True)
        while True:
            try:
                await self._kafka_producer.start()
            except KafkaConnectionError:
                sys.stderr.write("Cannot reach Kafka, sleeping for 1 second...")
                time.sleep(1)
                continue
            break

    async def flush(self):
        await self._kafka_producer.flush()

    async def send_event(self,
                         operator: BaseOperator,
                         key,
                         function: Type | str,
                         params: tuple = tuple(),
                         serializer: Serializer = Serializer.MSGPACK) -> bytes:
        request_id, serialized_value, partition = self._prepare_kafka_message(key,
                                                                              operator,
                                                                              function,
                                                                              params,
                                                                              serializer)
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

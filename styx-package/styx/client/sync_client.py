import sys
import time
import uuid
import warnings
from typing import Type

import zmq
from confluent_kafka import Producer, KafkaException
from zmq import Socket

from .base_client import BaseStyxClient

from ..common.base_operator import BaseOperator
from ..common.serialization import Serializer
from ..common.stateflow_graph import StateflowGraph
from ..common.message_types import MessageType


class SyncStyxClient(BaseStyxClient):

    _kafka_producer: Producer

    def __init__(self,
                 styx_coordinator_adr: str,
                 styx_coordinator_port: int,
                 kafka_url: str):
        super().__init__(styx_coordinator_adr, styx_coordinator_port)
        self._kafka_url = kafka_url
        self._sync_socket_to_coordinator: Socket | None = None

    def close(self):
        self.flush()
        if self._sync_socket_to_coordinator is not None:
            self._sync_socket_to_coordinator.close()
            del self._sync_socket_to_coordinator
        del self._kafka_producer

    def open(self):
        conf = {"bootstrap.servers": self._kafka_url,
                "acks": "all",
                "enable.idempotence": True,
                "client.id": str(uuid.uuid4())}
        while True:
            try:
                self._kafka_producer = Producer(**conf)
                break
            except KafkaException:
                warnings.warn(f'Kafka at {self._kafka_url} not ready yet, sleeping for 1 second')
                time.sleep(1)

    def flush(self):
        queue_size = self._kafka_producer.flush()
        assert queue_size == 0

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write(f'%% Message failed delivery: {err}\n')
        else:
            self._delivery_timestamps[msg.key()] = msg.timestamp()[1]

    def send_event(self,
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
        self._kafka_producer.produce(operator.name,
                                     key=request_id,
                                     value=serialized_value,
                                     partition=partition,
                                     callback=self.delivery_callback
                                     )
        self._kafka_producer.poll(0)
        return request_id

    def send_batch_insert(self,
                          operator: BaseOperator,
                          partition: int,
                          function: Type | str,
                          key_value_pairs: dict[any, any],
                          serializer: Serializer = Serializer.MSGPACK) -> bytes:
        request_id, serialized_value, _ = self._prepare_kafka_message(None,
                                                                      operator,
                                                                      function,
                                                                      (key_value_pairs, ),
                                                                      serializer,
                                                                      partition=partition)
        self._kafka_producer.produce(operator.name,
                                     key=request_id,
                                     value=serialized_value,
                                     partition=partition,
                                     callback=self.delivery_callback
                                     )
        self._kafka_producer.poll(0)
        return request_id

    def submit_dataflow(self, stateflow_graph: StateflowGraph, external_modules: tuple = None):
        self._verify_dataflow_input(stateflow_graph, external_modules)
        msg = self._networking_manager.encode_message(msg=(stateflow_graph, ),
                                                      msg_type=MessageType.SendExecutionGraph,
                                                      serializer=Serializer.CLOUDPICKLE)
        if self._sync_socket_to_coordinator is None:
            self._sync_socket_to_coordinator = zmq.Context().socket(zmq.DEALER)
            self._sync_socket_to_coordinator.connect(f'tcp://{self._styx_coordinator_adr}'
                                                     f':{self._styx_coordinator_port}')
        self._sync_socket_to_coordinator.send(msg)

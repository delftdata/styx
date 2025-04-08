import logging
import threading
import time
import uuid
import warnings
import socket
from typing import Type

from confluent_kafka import Producer, KafkaException, Consumer, Message
from confluent_kafka.admin import ClusterMetadata

from .base_client import BaseStyxClient
from .styx_future import StyxFuture
from ..common.base_operator import BaseOperator
from ..common.serialization import Serializer, msgpack_deserialization, cloudpickle_deserialization
from ..common.stateflow_graph import StateflowGraph
from ..common.message_types import MessageType
from ..common.tcp_networking import NetworkingManager


class SyncStyxClient(BaseStyxClient):

    _kafka_producer: Producer

    def __init__(self,
                 styx_coordinator_adr: str,
                 styx_coordinator_port: int,
                 kafka_url: str):
        super().__init__(styx_coordinator_adr, styx_coordinator_port)
        self._kafka_url = kafka_url
        self._futures: dict[bytes, StyxFuture] = {}
        self.running_result_consumer = False
        self.result_consumer_process: threading.Thread = ...
        self.running_metadata_consumer = False
        self.metadata_consumer_process: threading.Thread = ...
        self.kafka_consumer_config = {
            "bootstrap.servers": self._kafka_url,
            "group.id": str(uuid.uuid4()),
            "fetch.min.bytes": 1,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            }
        self.kafka_producer_config = {
            "bootstrap.servers": self._kafka_url,
            "acks": "all",
            "linger.ms": 0,
            "compression.type": "none",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 1,
            "client.id": str(uuid.uuid4())
        }
        self.graph_known_event: threading.Event = threading.Event()

    def get_operator_partition(self, key, operator: BaseOperator) -> int:
        if not self.graph_known_event.is_set():
            self.graph_known_event.wait()
        return self._current_active_graph.get_operator(operator).which_partition(key)

    def start_futures_consumer_thread(self):
        self.result_consumer_process = threading.Thread(target=self.start_consuming_results)
        self.running_result_consumer = True
        self.result_consumer_process.start()

    def start_metadata_consumer_thread(self):
        self.metadata_consumer_process = threading.Thread(target=self.start_consuming_metadata)
        self.running_metadata_consumer = True
        self.metadata_consumer_process.start()

    def close(self):
        self.flush()
        del self._kafka_producer
        self.running_result_consumer = False
        self.running_metadata_consumer = False

    def start_consuming_results(self):
        "Reads the results and puts them into futures"
        if not self.graph_known_event.is_set():
            self.graph_known_event.wait()
        function_results_consumer = Consumer(**self.kafka_consumer_config)
        egress_topic_names: list[str] = self._current_active_graph.get_egress_topic_names()
        md: ClusterMetadata = function_results_consumer.list_topics()
        wait_output_topics = True
        while wait_output_topics:
            wait_output_topics = False
            for topic in egress_topic_names:
                if topic not in md.topics:
                    wait_output_topics = True
            if not wait_output_topics:
                break
            logging.warning(f"Waiting 1 sec for egress topics to be created by the Styx coordinator "
                            f"| topics: {md.topics}")
            time.sleep(1)
            md: ClusterMetadata = function_results_consumer.list_topics()
        topics_to_subscribe = [topic for topic in md.topics if topic.endswith('--OUT')]
        logging.warning(f"Subscribed to topics: {topics_to_subscribe}")
        function_results_consumer.subscribe(topics_to_subscribe)
        while self.running_result_consumer:
            # poll every 10ms (this will add at least 10 ms latency to the futures, but it makes the client lightweight)
            msg: Message = function_results_consumer.poll(0.01)
            if msg is None:
                continue
            if msg.error():
                continue
            if msg.key() in self._futures:
                self._futures[msg.key()].set(response_val=msgpack_deserialization(msg.value()),
                                             out_timestamp=msg.timestamp()[1])
        function_results_consumer.close()

    def start_consuming_metadata(self):
        metadata_consumer = Consumer(**self.kafka_consumer_config)
        md: ClusterMetadata = metadata_consumer.list_topics()
        while 'styx-metadata' not in md.topics:
            logging.warning(f"Waiting 1 sec for metadata topic to be created by the Styx coordinator "
                            f"| topics: {md.topics}")
            time.sleep(1)
            md: ClusterMetadata = metadata_consumer.list_topics()
        metadata_consumer.subscribe(['styx-metadata'])
        while self.running_metadata_consumer:
            # poll every 100ms
            msg: Message = metadata_consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                continue
            metadata = cloudpickle_deserialization(msg.value())
            if isinstance(metadata, StateflowGraph):
                self._current_active_graph = metadata
                self.graph_known_event.set()
        metadata_consumer.close()

    def open(self, consume: bool = True):
        self.start_metadata_consumer_thread()
        while True:
            try:
                self._kafka_producer = Producer(**self.kafka_producer_config)
                break
            except KafkaException as e:
                warnings.warn(f'Kafka at {self._kafka_url} not ready yet due to {e}, sleeping for 1 second')
                time.sleep(1)
        if consume:
            self.start_futures_consumer_thread()

    def flush(self):
        queue_size = self._kafka_producer.flush()
        assert queue_size == 0

    def delivery_callback(self, err, msg):
        if err is not None:
            logging.warning("Delivery failed for User record {}: {}".format(msg.key(), err))
        else:
            self._delivery_timestamps[msg.key()] = msg.timestamp()[1]
            if msg.key() in self._futures:
                self._futures[msg.key()].set_in_timestamp(msg.timestamp()[1])

    def send_event(self,
                   operator: BaseOperator,
                   key,
                   function: Type | str,
                   params: tuple = tuple(),
                   serializer: Serializer = Serializer.MSGPACK) -> StyxFuture:
        if not self.graph_known_event.is_set():
            self.graph_known_event.wait()
        request_id, serialized_value, partition = self._prepare_kafka_message(key,
                                                                              operator,
                                                                              function,
                                                                              params,
                                                                              serializer)
        self._futures[request_id] = StyxFuture(request_id=request_id)
        self._kafka_producer.produce(operator.name,
                                     key=request_id,
                                     value=serialized_value,
                                     partition=partition,
                                     on_delivery=self.delivery_callback
                                     )
        self._kafka_producer.poll(0)
        return self._futures[request_id]

    def send_batch_insert(self,
                          operator: BaseOperator,
                          partition: int,
                          function: Type | str,
                          key_value_pairs: dict[any, any],
                          serializer: Serializer = Serializer.MSGPACK) -> bytes:
        if not self.graph_known_event.is_set():
            self.graph_known_event.wait()
        request_id, serialized_value, _ = self._prepare_kafka_message(None,
                                                                      operator,
                                                                      function,
                                                                      (key_value_pairs, ),
                                                                      serializer,
                                                                      partition=partition)
        self._kafka_producer.produce(operator.name,
                                     key=request_id,
                                     value=serialized_value,
                                     partition=partition)
        self._kafka_producer.poll(0)
        return request_id

    def submit_dataflow(self, stateflow_graph: StateflowGraph, external_modules: tuple = None):
        self._verify_dataflow_input(stateflow_graph, external_modules)
        self._current_active_graph = stateflow_graph
        self.graph_known_event.set()
        msg = NetworkingManager.encode_message(msg=(stateflow_graph, ),
                                               msg_type=MessageType.SendExecutionGraph,
                                               serializer=Serializer.CLOUDPICKLE)
        s = socket.socket()
        s.connect((self._styx_coordinator_adr, self._styx_coordinator_port))
        s.send(msg)
        s.close()

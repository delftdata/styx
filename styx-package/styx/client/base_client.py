from abc import abstractmethod, ABC
import uuid

from types import ModuleType
from typing import Type

from cloudpickle import cloudpickle
from styx.common.base_networking import BaseNetworking
from styx.common.tcp_networking import NetworkingManager

from .styx_future import StyxFuture, StyxAsyncFuture
from ..common.base_operator import BaseOperator
from ..common.stateful_function import make_key_hashable
from ..common.stateflow_graph import StateflowGraph
from ..common.message_types import MessageType
from ..common.serialization import cloudpickle_serialization, cloudpickle_deserialization, Serializer, \
    msgpack_serialization


class NotAStateflowGraph(Exception):
    pass


class GraphNotSerializable(Exception):
    pass


class BaseStyxClient(ABC):

    def __init__(self,
                 styx_coordinator_adr: str,
                 styx_coordinator_port: int):
        self._styx_coordinator_adr: str = styx_coordinator_adr
        self._styx_coordinator_port: int = styx_coordinator_port
        self._networking_manager: NetworkingManager = NetworkingManager(None)
        self._delivery_timestamps: dict[bytes, int] = {}

    @property
    def delivery_timestamps(self):
        return self._delivery_timestamps

    @staticmethod
    def _get_modules(stateflow_graph: StateflowGraph):
        modules = {ModuleType(stateflow_graph.__module__)}
        for operator in stateflow_graph.nodes.values():
            modules.add(ModuleType(operator.__module__))
            for function in operator.functions.values():
                modules.add(ModuleType(function.__module__))
        return modules

    @staticmethod
    def _check_serializability(stateflow_graph: StateflowGraph):
        try:
            ser = cloudpickle_serialization(stateflow_graph)
            cloudpickle_deserialization(ser)
        except Exception:
            raise GraphNotSerializable("The submitted graph is not serializable, "
                                       "all external modules should be declared")

    def _verify_dataflow_input(self, stateflow_graph: StateflowGraph, external_modules: tuple) -> None:
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        modules = self._get_modules(stateflow_graph)
        system_module_name = __name__.split('.')[0]
        for module in modules:
            # exclude system modules
            if not module.__name__.startswith(system_module_name) and \
                    not module.__name__.startswith("stateflow"):
                cloudpickle.register_pickle_by_value(module)
        if external_modules is not None:
            for external_module in external_modules:
                cloudpickle.register_pickle_by_value(external_module)
        self._check_serializability(stateflow_graph)

    def _prepare_kafka_message(self,
                               key,
                               operator: BaseOperator,
                               function: Type | str,
                               params: tuple,
                               serializer: Serializer,
                               partition: int | None = None) -> tuple[bytes, bytes, int]:
        if partition is None:
            partition: int = make_key_hashable(key) % operator.n_partitions
        fun_name: str = function if isinstance(function, str) else function.__name__
        event = (operator.name,
                 key,
                 fun_name,
                 params,
                 partition)
        # needs to be uuid4 due to concurrent clients from the same machine
        request_id = msgpack_serialization(uuid.uuid4().int >> 64)
        serialized_value: bytes = BaseNetworking.encode_message(msg=event,
                                                                msg_type=MessageType.ClientMsg,
                                                                serializer=serializer)
        return request_id, serialized_value, partition

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def open(self, consume: bool = True):
        raise NotImplementedError

    @abstractmethod
    def flush(self):
        raise NotImplementedError

    @abstractmethod
    def send_event(self,
                   operator: BaseOperator,
                   key,
                   function: Type | str,
                   params: tuple = tuple(),
                   serializer: Serializer = Serializer.MSGPACK) -> StyxFuture | StyxAsyncFuture:
        raise NotImplementedError

    @abstractmethod
    def send_batch_insert(self,
                          operator: BaseOperator,
                          partition: int,
                          function: Type | str,
                          key_value_pairs: dict[any, any],
                          serializer: Serializer = Serializer.MSGPACK) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def submit_dataflow(self, stateflow_graph: StateflowGraph, external_modules: tuple = None):
        raise NotImplementedError

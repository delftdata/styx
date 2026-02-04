from abc import ABC, abstractmethod
import io
from types import ModuleType
from typing import TYPE_CHECKING
import uuid

from cloudpickle import cloudpickle

from styx.common.base_networking import BaseNetworking
from styx.common.exceptions import GraphNotSerializableError, NotAStateflowGraphError
from styx.common.message_types import MessageType
from styx.common.serialization import (
    Serializer,
    cloudpickle_deserialization,
    cloudpickle_serialization,
    msgpack_serialization,
    zstd_msgpack_serialization,
)
from styx.common.stateflow_graph import StateflowGraph

if TYPE_CHECKING:
    from minio import Minio

    from styx.client.styx_future import StyxAsyncFuture, StyxFuture
    from styx.common.base_operator import BaseOperator
    from styx.common.types import K, KVPairs


class BaseStyxClient(ABC):
    def __init__(
        self,
        styx_coordinator_adr: str,
        styx_coordinator_port: int,
        minio: Minio | None = None,
    ) -> None:
        self._styx_coordinator_adr: str = styx_coordinator_adr
        self._styx_coordinator_port: int = styx_coordinator_port
        self._delivery_timestamps: dict[bytes, int] = {}
        self._current_active_graph: StateflowGraph | None = None
        self.minio = minio
        if self.minio is not None and not self.minio.bucket_exists("styx-snapshots"):
            self.minio.make_bucket("styx-snapshots")

    @property
    def delivery_timestamps(self) -> dict[bytes, int]:
        return self._delivery_timestamps

    @staticmethod
    def _get_modules(stateflow_graph: StateflowGraph) -> set[ModuleType]:
        modules = {ModuleType(stateflow_graph.__module__)}
        for operator in stateflow_graph.nodes.values():
            modules.add(ModuleType(operator.__module__))
            for function in operator.functions.values():
                modules.add(ModuleType(function.__module__))
        return modules

    @staticmethod
    def _check_serializability(stateflow_graph: StateflowGraph) -> None:
        try:
            ser = cloudpickle_serialization(stateflow_graph)
            cloudpickle_deserialization(ser)
        except Exception as e:
            msg = "The submitted graph is not serializable, all external modules should be declared"
            raise GraphNotSerializableError(msg) from e

    def _verify_dataflow_input(
        self,
        stateflow_graph: StateflowGraph,
        external_modules: tuple,
    ) -> None:
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraphError
        modules = self._get_modules(stateflow_graph)
        system_module_name = __name__.split(".")[0]
        for module in modules:
            # exclude system modules
            if not module.__name__.startswith(
                system_module_name,
            ) and not module.__name__.startswith("stateflow"):
                cloudpickle.register_pickle_by_value(module)
        if external_modules is not None:
            for external_module in external_modules:
                cloudpickle.register_pickle_by_value(external_module)
        self._check_serializability(stateflow_graph)

    @abstractmethod
    def get_operator_partition(self, key: K, operator: BaseOperator) -> int:
        raise NotImplementedError

    def _prepare_kafka_message(
        self,
        key: K,
        operator: BaseOperator,
        function: type | str,
        params: tuple,
        serializer: Serializer,
        partition: int | None = None,
    ) -> tuple[bytes, bytes, int]:
        if partition is None:
            partition: int = self._current_active_graph.nodes[operator.name].which_partition(key)
        fun_name: str = function if isinstance(function, str) else function.__name__
        event = (operator.name, key, fun_name, params, partition)
        # needs to be uuid4 due to concurrent clients from the same machine
        request_id = msgpack_serialization(uuid.uuid4().int >> 64)
        serialized_value: bytes = BaseNetworking.encode_message(
            msg=event,
            msg_type=MessageType.ClientMsg,
            serializer=serializer,
        )
        return request_id, serialized_value, partition

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def open(self, consume: bool = True) -> None:
        raise NotImplementedError

    @abstractmethod
    def flush(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def send_event(
        self,
        operator: BaseOperator,
        key: K,
        function: type | str,
        params: tuple = (),
        serializer: Serializer = Serializer.MSGPACK,
    ) -> StyxFuture | StyxAsyncFuture:
        raise NotImplementedError

    def init_data(
        self,
        operator: BaseOperator,
        partition: int,
        key_value_pairs: KVPairs,
    ) -> None:
        snapshot_name = f"data/{operator.name}/{partition}/0.bin"
        sn_data: bytes = zstd_msgpack_serialization(key_value_pairs)
        self.minio.put_object(
            "styx-snapshots",
            snapshot_name,
            io.BytesIO(sn_data),
            len(sn_data),
        )

    def init_metadata(self, graph: StateflowGraph) -> None:
        topic_partition_offsets = {
            (operator_name, partition): -1
            for operator_name, operator in graph.nodes.items()
            for partition in range(operator.n_partitions)
        }
        output_offsets = {
            (operator_name, partition): -1
            for operator_name, operator in graph.nodes.items()
            for partition in range(operator.n_partitions)
        }
        epoch_counter = 0
        t_counter = 0
        sn_data: bytes = zstd_msgpack_serialization(
            (topic_partition_offsets, output_offsets, epoch_counter, t_counter),
        )
        self.minio.put_object(
            "styx-snapshots",
            "sequencer/0.bin",
            io.BytesIO(sn_data),
            len(sn_data),
        )

    @abstractmethod
    def set_graph(self, graph: StateflowGraph) -> None:
        raise NotImplementedError

    @abstractmethod
    def submit_dataflow(
        self,
        stateflow_graph: StateflowGraph,
        external_modules: tuple | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def notify_init_data_complete(self) -> None:
        raise NotImplementedError

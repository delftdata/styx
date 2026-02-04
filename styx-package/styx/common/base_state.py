from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.types import K, KVPairs, OperatorPartition, V


class BaseOperatorState(ABC):
    def __init__(self, operator_partitions: set[OperatorPartition]) -> None:
        self.operator_partitions: set[OperatorPartition] = operator_partitions

    @abstractmethod
    def put(
        self,
        key: K,
        value: V,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def put_immediate(
        self,
        key: K,
        value: V,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_all(
        self,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> dict[OperatorPartition, KVPairs]:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        raise NotImplementedError

    @abstractmethod
    def get_immediate(
        self,
        key: K,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: K, operator_name: str, partition: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def in_remote_keys(self, key: K, operator_name: str, partition: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_worker_id_old_partition(
        self,
        operator_name: str,
        partition: int,
        key: K,
    ) -> tuple[int, int]:
        raise NotImplementedError

    @abstractmethod
    def migrate_within_the_same_worker(
        self,
        operator_name: str,
        new_partition: int,
        key: K,
        old_partition: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: K, operator_name: str, partition: int) -> bool:
        raise NotImplementedError

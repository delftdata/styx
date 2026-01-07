from abc import abstractmethod, ABC
from typing import Any

from .types import OperatorPartition

class BaseOperatorState(ABC):

    def __init__(self, operator_partitions: set[OperatorPartition]):
        self.operator_partitions: set[OperatorPartition] = operator_partitions

    @abstractmethod
    def put(self, key: Any, value: Any, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def put_immediate(self, key: Any, value: Any, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def get_all(self, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def get(self, key: Any, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def get_immediate(self, key: Any, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: Any, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def in_remote_keys(self, key: Any, operator_name: str, partition: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_worker_id_old_partition(self, operator_name: str, partition: int, key: Any) -> tuple[int, int]:
        raise NotImplementedError

    @abstractmethod
    def migrate_within_the_same_worker(self, operator_name: str, new_partition: int, key: Any, old_partition: int):
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: Any, operator_name: str, partition: int):
        raise NotImplementedError

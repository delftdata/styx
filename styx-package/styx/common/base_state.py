from abc import abstractmethod, ABC

from .types import OperatorPartition

class BaseOperatorState(ABC):

    def __init__(self, operator_partitions: set[OperatorPartition]):
        self.operator_partitions: set[OperatorPartition] = operator_partitions

    @abstractmethod
    def put(self, key, value, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def put_immediate(self, key, value, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def get_all(self, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def get(self, key, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def get_immediate(self, key, t_id: int, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def delete(self, key, operator_name: str, partition: int):
        raise NotImplementedError

    @abstractmethod
    def exists(self, key, operator_name: str, partition: int):
        raise NotImplementedError

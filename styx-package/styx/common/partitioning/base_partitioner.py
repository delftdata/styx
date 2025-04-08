from abc import ABC, abstractmethod


class BasePartitioner(ABC):

    @abstractmethod
    def get_partition(self, key) -> int:
        raise NotImplementedError

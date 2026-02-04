from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.types import K


class BasePartitioner(ABC):
    @abstractmethod
    def get_partition(self, key: K) -> int:
        raise NotImplementedError

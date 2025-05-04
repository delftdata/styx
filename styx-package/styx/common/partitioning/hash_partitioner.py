from functools import lru_cache

import cityhash

from .base_partitioner import BasePartitioner
from ..exceptions import NonSupportedKeyType


class HashPartitioner(BasePartitioner):

    def __init__(self, partitions: int):
        self._partitions = partitions

    def update_partitions(self, partitions: int):
        self._partitions = partitions

    @property
    def partitions(self):
        return self._partitions

    @lru_cache(maxsize=100_000)
    def get_partition(self, key) -> int | None:
        if key is None:
            return None
        return self.make_key_hashable(key) % self._partitions

    def get_partition_composite(self, key: str, field: int, delim: str) -> int:
        parts = key.split(delim)
        return self.get_partition(parts[field])

    @staticmethod
    def make_key_hashable(key) -> int:
        if isinstance(key, int):
            return key
        elif key.isdigit():
            return int(key)
        else:
            try:
                return cityhash.CityHash64(key)
            except Exception:
                raise NonSupportedKeyType()

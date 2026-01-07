from functools import lru_cache

import cityhash

from .base_partitioner import BasePartitioner
from ..exceptions import NonSupportedKeyType


class HashPartitioner(BasePartitioner):

    def __init__(self,
                 partitions: int,
                 composite_key_hash_parameters: tuple[int, str] | None):
        self._partitions = partitions
        self._composite_key_hash_parameters = composite_key_hash_parameters

    def update_partitions(self, partitions: int):
        self._partitions = partitions

    @property
    def partitions(self):
        return self._partitions

    @lru_cache(maxsize=100_000)
    def get_partition(self, key) -> int | None:
        if key is None:
            return None
        if self._composite_key_hash_parameters is not None:
            field, delim = self._composite_key_hash_parameters
            key = key.split(delim)[field]
        return self.make_key_hashable(key) % self._partitions

    def get_partition_no_cache(self, key) -> int | None:
        if key is None:
            return None
        if self._composite_key_hash_parameters is not None:
            field, delim = self._composite_key_hash_parameters
            key = key.split(delim)[field]
        return self.make_key_hashable(key) % self._partitions

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

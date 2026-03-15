from functools import lru_cache
from typing import TYPE_CHECKING

from styx.common.partitioning._hash_partitioner import (
    get_partition_no_cache as _cy_get_partition_no_cache,
    make_key_hashable as _cy_make_key_hashable,
)
from styx.common.partitioning.base_partitioner import BasePartitioner

if TYPE_CHECKING:
    from styx.common.types import K


class HashPartitioner(BasePartitioner):
    def __init__(
        self,
        partitions: int,
        composite_key_hash_parameters: tuple[int, str] | None,
    ) -> None:
        self._partitions = partitions
        self._composite_key_hash_parameters = composite_key_hash_parameters

    def update_partitions(self, partitions: int) -> None:
        self._partitions = partitions

    @property
    def partitions(self) -> int:
        return self._partitions

    def get_partition(self, key: K) -> int | None:
        if key is None:
            return None
        return self._get_partition_cached(
            self._partitions,
            self._composite_key_hash_parameters,
            key,
        )

    @staticmethod
    @lru_cache(maxsize=100_000)
    def _get_partition_cached(
        partitions: int,
        composite_key_hash_parameters: tuple[int, str] | None,
        key: K,
    ) -> int:
        if composite_key_hash_parameters is not None:
            key: str  # This only holds for str keys
            field, delim = composite_key_hash_parameters
            key = key.split(delim)[field]
        return _cy_make_key_hashable(key) % partitions

    def get_partition_no_cache(self, key: K) -> int | None:
        return _cy_get_partition_no_cache(
            key,
            self._partitions,
            self._composite_key_hash_parameters,
        )

    @staticmethod
    def make_key_hashable(key: K) -> int:
        return _cy_make_key_hashable(key)

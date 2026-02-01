from collections.abc import Hashable
from typing import TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

# OperatorPartition: |operator_name, partition|
type OperatorPartition = tuple[str, int]

# KVPairs: hashmap of hashable key and arbitrary value
type KVPairs[K, V] = dict[K, V]

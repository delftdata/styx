from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.types import K, V


class LocalOperatorState:
    """Minimal dict-backed state for local runner execution.

    State is keyed by (operator_name, partition) -> {key: value}.
    """

    def __init__(self) -> None:
        self._data: dict[tuple[str, int], dict[K, V]] = {}

    def init_partition(self, operator_name: str, partition: int) -> None:
        self._data.setdefault((operator_name, partition), {})

    def get(self, key: K, operator_name: str, partition: int) -> V:
        return self._data[(operator_name, partition)].get(key)

    def put(self, key: K, value: V, operator_name: str, partition: int) -> None:
        self._data[(operator_name, partition)][key] = value

    def get_all(self, operator_name: str, partition: int) -> dict[K, V]:
        return dict(self._data.get((operator_name, partition), {}))

    def batch_insert(self, kv_pairs: dict[K, V], operator_name: str, partition: int) -> None:
        self._data.setdefault((operator_name, partition), {}).update(kv_pairs)

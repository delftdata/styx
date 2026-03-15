from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.types import K, V


class LocalOperatorState:
    """Minimal dict-backed state for local runner execution.

    State is keyed by (operator_name, partition) -> {key: value}.
    Per-key asyncio locks protect concurrent access.
    """

    def __init__(self) -> None:
        self._data: dict[tuple[str, int], dict[K, V]] = {}
        self._key_locks: dict[tuple[str, K], asyncio.Lock] = {}

    def key_lock(self, operator_name: str, key: K) -> asyncio.Lock:
        """Return (or create) the asyncio.Lock for a given operator + key."""
        lock_key = (operator_name, key)
        lock = self._key_locks.get(lock_key)
        if lock is None:
            lock = asyncio.Lock()
            self._key_locks[lock_key] = lock
        return lock

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

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.types import K, V
    from styx.local_runner.local_state import LocalOperatorState


class LocalStatefulFunction:
    """Duck-typed replacement for StatefulFunction for local execution.

    Provides the same user-facing interface (key, data, get, put,
    batch_insert, call_remote_async) without requiring distributed
    runtime dependencies.
    """

    def __init__(
        self,
        key: K,
        operator_name: str,
        partition: int,
        state: LocalOperatorState,
    ) -> None:
        self._key = key
        self._operator_name = operator_name
        self._partition = partition
        self._state = state
        self._remote_calls: list[tuple[str, str, K, tuple]] = []

    @property
    def key(self) -> K:
        return self._key

    @property
    def data(self) -> dict[K, V]:
        return self._state.get_all(self._operator_name, self._partition)

    def get(self) -> V:
        return self._state.get(self._key, self._operator_name, self._partition)

    def put(self, value: V) -> None:
        self._state.put(self._key, value, self._operator_name, self._partition)

    def batch_insert(self, kv_pairs: dict[K, V]) -> None:
        if kv_pairs:
            self._state.batch_insert(kv_pairs, self._operator_name, self._partition)

    def call_remote_async(
        self,
        operator_name: str,
        function_name: type | str,
        key: K,
        params: tuple = (),
    ) -> None:
        if isinstance(function_name, type):
            function_name = function_name.__name__
        self._remote_calls.append((operator_name, function_name, key, params))

    def drain_remote_calls(self) -> list[tuple[str, str, K, tuple]]:
        calls = self._remote_calls
        self._remote_calls = []
        return calls

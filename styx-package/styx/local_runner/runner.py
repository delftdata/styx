from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from styx.local_runner.local_context import LocalStatefulFunction
from styx.local_runner.local_state import LocalOperatorState

if TYPE_CHECKING:
    from styx.common.stateflow_graph import StateflowGraph
    from styx.common.types import K


class LocalStyxRunner:
    """In-process sequential executor for Styx stateful functions.

    Runs user-defined functions against a plain dict-backed state
    with no infrastructure dependencies. Useful for debugging and
    validating function logic before deploying to a full Styx cluster.

    Note: This runner does NOT provide transactional semantics,
    conflict detection, or epoch-based execution. See the README
    for known limitations.
    """

    def __init__(self, graph: StateflowGraph) -> None:
        self._graph = graph
        self._state = LocalOperatorState()
        # Initialize all partitions for all operators
        for operator_name, operator in graph:
            for partition in range(operator.n_partitions):
                self._state.init_partition(operator_name, partition)

    def init_data(self, operator_name: str, key_value_pairs: dict) -> None:
        """Pre-populate state for an operator, auto-partitioning keys.

        Args:
            operator_name: Name of the operator to populate.
            key_value_pairs: Mapping of keys to values.
        """
        operator = self._graph.nodes[operator_name]
        for key, value in key_value_pairs.items():
            partition = operator.which_partition(key)
            self._state.put(key, value, operator_name, partition)

    async def send_event(
        self,
        operator_name: str,
        key: K,
        function: str,
        params: tuple = (),
    ) -> object:
        """Execute a single function call and return its result.

        Chained call_remote_async calls are executed depth-first
        after the function returns.

        Args:
            operator_name: Target operator name.
            key: The key for this invocation.
            function: Name of the registered function to call.
            params: Additional parameters passed to the function.

        Returns:
            The return value of the user function.
        """
        return await self._execute_function(operator_name, key, function, params)

    def send_event_sync(
        self,
        operator_name: str,
        key: K,
        function: str,
        params: tuple = (),
    ) -> object:
        """Synchronous wrapper around send_event.

        Uses asyncio.run(), so it cannot be called from within
        an already-running event loop (e.g. Jupyter, pytest-asyncio).
        """
        return asyncio.run(self.send_event(operator_name, key, function, params))

    async def send_batch(
        self,
        events: list[tuple[str, K, str, tuple]],
    ) -> list[object]:
        """Execute multiple events sequentially.

        Args:
            events: List of (operator_name, key, function, params) tuples.

        Returns:
            List of results, one per event.
        """
        results = []
        for operator_name, key, function, params in events:
            result = await self._execute_function(operator_name, key, function, params)
            results.append(result)
        return results

    def get_state(self, operator_name: str) -> dict:
        """Return merged state across all partitions for an operator.

        Useful for debugging and assertions.
        """
        operator = self._graph.nodes[operator_name]
        merged = {}
        for partition in range(operator.n_partitions):
            merged.update(self._state.get_all(operator_name, partition))
        return merged

    async def _execute_function(
        self,
        operator_name: str,
        key: K,
        function_name: str,
        params: tuple,
    ) -> object:
        """Core execution: create context, run function, DFS-execute chains.

        Acquires a per-key lock so concurrent send_event calls on the
        same key are serialized.  The lock is released *before* chained
        remote calls execute, preventing deadlocks on cycles (e.g.
        concurrent transfer A->B and B->A).
        """
        operator = self._graph.nodes[operator_name]
        partition = operator.which_partition(key)
        func = operator.functions[function_name]

        lock = self._state.key_lock(operator_name, key)

        # Hold the lock while executing the function (get/put are protected).
        # Release before chaining to avoid deadlocks.
        async with lock:
            ctx = LocalStatefulFunction(
                key=key,
                operator_name=operator_name,
                partition=partition,
                state=self._state,
            )
            result = await func(ctx, *params)
            pending_chains = ctx.drain_remote_calls()

        # DFS-execute chained calls outside the lock
        for chain_op, chain_func, chain_key, chain_params in pending_chains:
            await self._execute_function(chain_op, chain_key, chain_func, chain_params)

        return result

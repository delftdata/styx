import asyncio
import fractions
from typing import TYPE_CHECKING

from styx.common.function import Function
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import Serializer

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from styx.common.base_protocol import BaseTransactionalProtocol
    from styx.common.base_state import BaseOperatorState as State
    from styx.common.stateflow_graph import StateflowGraph
    from styx.common.tcp_networking import NetworkingManager
    from styx.common.types import K, V


class StatefulFunction(Function):
    """Encapsulates a stateful function in a distributed Styx operator.

    This class wraps user-defined functions with access to state, networking,
    partitioning, and chain coordination. It supports fallback execution and
    asynchronous remote function chaining.
    """

    def __init__(
        self,
        key: K,
        function_name: str,
        partition: int,
        operator_name: str,
        operator_state: State,
        networking: NetworkingManager,
        dns: dict[str, dict[int, tuple[str, int, int]]],
        t_id: int,
        request_id: bytes,
        fallback_mode: bool,
        use_fallback_cache: bool,
        deployed_graph: StateflowGraph,
        operator_lock: asyncio.Lock,
        protocol: BaseTransactionalProtocol,
    ) -> None:
        """Initializes a stateful function with execution context.

        Args:
            key: The key of the function.
            function_name (str): Name of the user-defined function.
            partition (int): Partition ID where this function will execute.
            operator_name (str): Name of the operator this function belongs to.
            operator_state (State): The state backend to use.
            networking (NetworkingManager): Handles communication and coordination.
            dns (dict): Mapping of operator partitions to worker locations.
            t_id (int): Transaction ID.
            request_id (bytes): Unique identifier for this function invocation.
            fallback_mode (bool): Whether to enable fallback (recovery) logic.
            use_fallback_cache (bool): Whether to use cached fallback results.
            partitioner (BasePartitioner): The partitioning strategy.
            protocol (BaseTransactionalProtocol): Protocol for function invocation.
        """
        super().__init__(name=function_name)
        self.__operator_name = operator_name
        self.__state: State = operator_state
        self.__networking: NetworkingManager = networking
        self.__dns: dict[str, dict[int, tuple[str, int, int]]] = dns
        self.__t_id: int = t_id
        self.__request_id: bytes = request_id
        self.__async_remote_calls: list[tuple[str, str, int, object, tuple, bool]] = []
        self.__fallback_enabled: bool = fallback_mode
        self.__use_fallback_cache: bool = use_fallback_cache
        self.__key = key
        self.__protocol = protocol
        self.__partition = partition
        self.__deployed_graph = deployed_graph
        self.__operator_lock: asyncio.Lock = operator_lock

    async def __call__(self, *args, **kwargs) -> tuple[object, int, int]:  # noqa: ANN002, ANN003
        """
        Executes the wrapped function and triggers asynchronous remote calls if needed.

        Returns:
            (result, n_remote_calls, partial_node_count) on success
            (exception, -1, -1) on failure
        """
        try:
            # 1) Decide whether we need to fetch/migrate the key, while holding the lock briefly.
            need_remote_fetch: bool = False
            remote_info: tuple[int, int] | None = None  # (worker_id, old_partition) shape depends on your state

            async with self.__operator_lock:
                if self.__state.in_remote_keys(
                    self.__key,
                    self.__operator_name,
                    self.__partition,
                ):
                    remote_info = self.__state.get_worker_id_old_partition(
                        self.__operator_name,
                        self.__partition,
                        self.__key,
                    )

                    # Your state seems to return something like (worker_id, old_partition)
                    # since you later access remote_info[1] as the old partition.
                    old_partition = remote_info[1]

                    is_local: bool = (
                        self.__operator_name,
                        old_partition,
                    ) in self.__state.operator_partitions
                    if is_local:
                        # Transfer the key from another partition within the same worker
                        self.__state.migrate_within_the_same_worker(
                            self.__operator_name,
                            self.__partition,
                            self.__key,
                            old_partition,
                        )
                    else:
                        # We'll do networking outside the lock.
                        need_remote_fetch = True

            # 2) If needed, fetch the key from a remote worker without holding the operator lock.
            if need_remote_fetch:
                if remote_info is None:
                    logging.warning("Remote fetch failed: remote_info is None")
                await self.__networking.request_key(
                    self.__operator_name,
                    self.__partition,
                    self.__key,
                    remote_info,
                )
                await self.__networking.wait_for_remote_key_event(
                    self.__operator_name,
                    self.__partition,
                    self.__key,
                )

            # 3) Run user logic while holding the operator lock.
            async with self.__operator_lock:
                res = await self.run(*args)

            # 4) Fallback cache short-circuit.
            if self.__fallback_enabled and self.__use_fallback_cache:
                return res, len(self.__async_remote_calls), -1

            # 5) Chain / async remote calls.
            partial_node_count = 0
            n_remote_calls = 0

            if "ack_share" in kwargs and "ack_host" in kwargs:
                # Middle of the chain
                partial_node_count = kwargs.get("partial_node_count", 0)
                n_remote_calls = await self.__send_async_calls(
                    ack_host=kwargs["ack_host"],
                    ack_port=kwargs["ack_port"],
                    ack_share=kwargs["ack_share"],
                    chain_participants=kwargs["chain_participants"],
                    partial_node_count=partial_node_count,
                )
            elif self.__async_remote_calls:
                # Start of the chain
                n_remote_calls = await self.__send_async_calls(
                    ack_host="",
                    ack_port=-1,
                    ack_share=1,
                    chain_participants=[],
                    partial_node_count=partial_node_count,
                    is_root=True,
                )

        except Exception as e:
            return e, -1, -1
        else:
            return res, n_remote_calls, partial_node_count

    @property
    def data(self) -> dict[K, V]:
        """dict: All state associated with the current operator, and partition."""
        return self.__state.get_all(self.__t_id, self.__operator_name, self.__partition)

    @property
    def key(self) -> K:
        """The key for this function instance."""
        return self.__key

    async def run(self, *args) -> None:  # noqa: ANN002
        """Executes the actual function logic. Meant to be overridden by subclasses.

        Args:
            *args: Positional arguments to the function.

        Raises:
            NotImplementedError: If not overridden in subclass.
        """
        raise NotImplementedError

    def get(self) -> K:
        """Retrieves the value from state for the current key.

        Returns:
            object: The current value associated with the key.
        """
        if self.__fallback_enabled:
            value = self.__state.get_immediate(
                self.key,
                self.__t_id,
                self.__operator_name,
                self.__partition,
            )
        else:
            value = self.__state.get(
                self.key,
                self.__t_id,
                self.__operator_name,
                self.__partition,
            )
        return value

    def put(self, value: V) -> None:
        """Stores a value in the state for the current key.

        Args:
            value: The value to store.
        """
        if self.__fallback_enabled:
            self.__state.put_immediate(
                self.key,
                value,
                self.__t_id,
                self.__operator_name,
                self.__partition,
            )
        else:
            self.__state.put(
                self.key,
                value,
                self.__t_id,
                self.__operator_name,
                self.__partition,
            )

    def batch_insert(self, kv_pairs: dict) -> None:
        if kv_pairs:
            self.__state.batch_insert(kv_pairs, self.__operator_name, self.__partition)

    async def __send_async_calls(
        self,
        ack_host: str,
        ack_port: int,
        ack_share: int,
        chain_participants: list[int],
        partial_node_count: int,
        is_root: bool = False,
    ) -> int:
        """Sends all pending asynchronous remote function calls.

        Args:
            ack_host: Host for acknowledgement callbacks.
            ack_port: Port for acknowledgement callbacks.
            ack_share: Fractional contribution for chain tracking.
            chain_participants (list[int]): Workers involved in this chain.
            partial_node_count (int): Partial chain node count.
            is_root (bool, optional): Whether this function is the chain root.

        Returns:
            int: Number of remote function calls sent.
        """
        n_remote_calls: int = len(self.__async_remote_calls)
        if n_remote_calls == 0:
            return n_remote_calls
        # if fallback is enabled there is no need to call the functions because they are already cached
        new_share_fraction: str = str(fractions.Fraction(ack_share) / n_remote_calls)
        if is_root:
            # This is the root
            ack_host = self.__networking.host_name
            ack_port = self.__networking.host_port
            self.__networking.prepare_function_chain(self.__t_id)
        else:
            if not self.__networking.in_the_same_network(ack_host, ack_port):
                chain_participants.append(self.__networking.worker_id)
            partial_node_count += 1
        remote_calls: list[Awaitable] = []
        for entry in self.__async_remote_calls:
            operator_name, function_name, partition, key, params, is_local = entry
            ack_payload: tuple[str, int, int, str, list[int], int] = (
                ack_host,
                ack_port,
                self.__t_id,
                new_share_fraction,
                chain_participants,
                partial_node_count,
            )
            if is_local:
                payload = RunFuncPayload(
                    request_id=self.__request_id,
                    key=key,
                    operator_name=operator_name,
                    partition=partition,
                    function_name=function_name,
                    params=params,
                    ack_payload=ack_payload,
                )
                if self.__fallback_enabled:
                    # and no cache
                    remote_calls.append(
                        self.__protocol.run_fallback_function(
                            t_id=self.__t_id,
                            payload=payload,
                        ),
                    )
                else:
                    remote_calls.append(
                        self.__protocol.run_function(t_id=self.__t_id, payload=payload),
                    )
                    # add to cache
                    self.__networking.add_remote_function_call(self.__t_id, payload)
            else:
                remote_calls.append(
                    self.__call_remote_function_no_response(
                        operator_name=operator_name,
                        function_name=function_name,
                        key=key,
                        partition=partition,
                        params=params,
                        ack_payload=ack_payload,
                    ),
                )
            partial_node_count = 0
        await asyncio.gather(*remote_calls)
        return n_remote_calls

    def __get_partition(self, operator_name: str, key: K) -> int:
        return self.__deployed_graph.nodes[operator_name].which_partition(key)

    def call_remote_async(
        self,
        operator_name: str,
        function_name: type | str,
        key: K,
        params: tuple = (),
    ) -> None:
        """Queues a remote asynchronous function call.

        Args:
            operator_name (str): Name of the target operator.
            function_name (Type | str): Function type or name to invoke.
            key: Key for the remote call.
            params (tuple, optional): Parameters for the remote function call.
        """
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition: int = self.__get_partition(operator_name, key)
        is_local: bool = self.__networking.in_the_same_network(
            self.__dns[operator_name][partition][0],
            self.__dns[operator_name][partition][2],
        )
        self.__async_remote_calls.append(
            (operator_name, function_name, partition, key, params, is_local),
        )

    async def __call_remote_function_no_response(
        self,
        operator_name: str,
        function_name: type | str,
        key: K,
        partition: int,
        params: tuple = (),
        ack_payload: tuple[str, int, int, str, list[int], int] | None = None,
    ) -> None:
        """Sends a remote function call without expecting a response.

        Args:
            operator_name (str): Target operator name.
            function_name (Type | str): Function to call.
            key: Key for routing.
            partition (int): Partition ID of the target.
            params (tuple, optional): Parameters for the function.
            ack_payload (optional): Chain acknowledgement metadata.
        """
        if isinstance(function_name, type):
            function_name = function_name.__name__
        payload, operator_host, operator_port = self.__prepare_message_transmission(
            operator_name,
            key,
            function_name,
            partition,
            params,
            ack_payload,
        )
        await self.__networking.send_message(
            operator_host,
            operator_port,
            msg=payload,
            msg_type=MessageType.RunFunRemote,
            serializer=Serializer.MSGPACK,
        )

    def __prepare_message_transmission(
        self,
        operator_name: str,
        key: K,
        function_name: str,
        partition: int,
        params: tuple,
        ack_payload: tuple[str, int, int, str, list[int], int] | None = None,
    ) -> tuple[tuple[int, bytes, str, str, K, int, bool, tuple], str, int] | None:
        """Prepares the payload and routing details for a remote function call.

        Args:
            operator_name (str): Name of the target operator.
            key: Key for partitioning.
            function_name (str): Name of the function to invoke.
            partition (int): Partition number.
            params (tuple): Parameters to send.
            ack_payload (optional): Optional acknowledgement payload.

        Returns:
            tuple: (payload, operator_host, operator_port)
        """
        try:
            payload = (
                self.__t_id,  # __T_ID__
                self.__request_id,  # __RQ_ID__
                operator_name,  # __OP_NAME__
                function_name,  # __FUN_NAME__
                key,  # __KEY__
                partition,  # __PARTITION__
                self.__fallback_enabled,
                params,
            )  # __PARAMS__
            if ack_payload is not None:
                payload += (ack_payload,)
            operator_host = self.__dns[operator_name][partition][0]
            operator_port = self.__dns[operator_name][partition][2]
        except KeyError:
            logging.error(f"Couldn't find operator: {operator_name} in {self.__dns}")
        else:
            return payload, operator_host, operator_port

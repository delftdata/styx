from typing import Any

from .message_types import MessageType
from .tcp_networking import NetworkingManager
from .serialization import Serializer
from .base_operator import BaseOperator
from .base_protocol import BaseTransactionalProtocol
from .stateful_function import StatefulFunction
from .exceptions import OperatorDoesNotContainFunction
from .logging import logging
from .partitioning.hash_partitioner import HashPartitioner


class Operator(BaseOperator):
    """A stateful operator that executes user-defined stateful functions in Styx.

    This class handles function registration, partitioning logic,
    communication between workers, and execution of distributed function chains.
    """

    def __init__(self,
                 name: str,
                 n_partitions: int = 1):
        """Initializes the operator with a name and number of partitions.

        Args:
            name (str): The name of the operator.
            n_partitions (int, optional): The number of partitions. Defaults to 1.
        """
        super().__init__(name, n_partitions)
        self.__state = ...
        self.__networking: NetworkingManager = ...
        # where the other functions exist
        self.__dns: dict[str, dict[int, tuple[str, int, int]]] = {}
        self.__functions: dict[str, type] = {}
        self.__partitioner: HashPartitioner = HashPartitioner(n_partitions)
        self.__is_shadow: bool = False

    def which_partition(self, key: Any) -> int:
        """Determines the partition for a given key.

        Args:
            key: The key to partition.

        Returns:
            int: The partition number.
        """
        return self.__partitioner.get_partition(key)

    def make_shadow(self):
        """Marks this operator instance as a shadow partition."""
        self.__is_shadow = True

    @property
    def is_shadow(self):
        """bool: Indicates whether this operator is a shadow partition."""
        return self.__is_shadow

    @property
    def dns(self):
        """dict: A mapping from function names and partition numbers to worker addresses."""
        return self.__dns

    @property
    def functions(self):
        """dict: A mapping from function names to function classes/types."""
        return self.__functions

    async def run_function(self,
                           key,
                           t_id: int,
                           request_id: bytes,
                           function_name: str,
                           partition: int,
                           ack_payload: tuple[str, int, int, str, list[int], int] | None,
                           fallback_mode: bool,
                           use_fallback_cache: bool,
                           params: tuple,
                           protocol: BaseTransactionalProtocol) -> bool:
        """Executes a registered function with given parameters in a distributed execution chain.

        Args:
            key: The partitioning key.
            t_id (int): The transaction ID.
            request_id (bytes): Unique identifier for this request.
            function_name (str): Name of the function to invoke.
            partition (int): Partition on which to execute the function.
            ack_payload (tuple | None): Metadata for acknowledgement in distributed chains.
            fallback_mode (bool): Whether to execute in fallback (recovery) mode.
            use_fallback_cache (bool): Whether to use cached fallback results.
            params (tuple): Parameters to pass to the function.
            protocol (BaseTransactionalProtocol): Protocol for managing distributed transactional execution..

        Returns:
            bool: True if execution succeeded, False if an exception was encountered.
        """
        f = self.__materialize_function(function_name, partition, key, t_id, request_id,
                                        fallback_mode, use_fallback_cache, protocol)
        params = (f, ) + tuple(params)
        logging.info(f'ack_payload: {ack_payload} RQ_ID: {request_id} TID: {t_id} '
                     f'function: {self.name}:{function_name} fallback mode: {fallback_mode}')
        success: bool = True
        if ack_payload is not None:
            # part of a chain (not root)
            ack_host, ack_port, ack_id, fraction_str, chain_participants, partial_node_count = ack_payload
            resp, n_remote_calls, partial_node_count = await f(*params,
                                                               ack_host=ack_host,
                                                               ack_port=ack_port,
                                                               ack_share=fraction_str,
                                                               chain_participants=chain_participants,
                                                               partial_node_count=partial_node_count)
            if isinstance(resp, Exception):
                await self._send_chain_abort(str(resp), ack_host, ack_port, ack_id)
                success = False
            elif fallback_mode and use_fallback_cache:
                await self.__send_cache_ack(ack_host, ack_port, ack_id, resp)
            elif n_remote_calls == 0:
                # we need to count the last node as part of the chain
                partial_node_count += 1
                await self.__send_ack(ack_host, ack_port, ack_id, fraction_str,
                                      chain_participants, partial_node_count, resp)
        else:
            # root of a chain, or single call
            resp, _, _ = await f(*params)
            if isinstance(resp, Exception):
                self.__networking.abort_chain(t_id, str(resp))
                success = False
            else:
                self.__networking.add_response(t_id, resp)
        del f
        return success

    async def _send_chain_abort(self, resp, ack_host, ack_port, ack_id) -> None:
        """Sends an abort signal to the worker that holds the root of a distributed chain.

        Args:
            resp: Error response or exception message.
            ack_host: Hostname or IP of the next worker.
            ack_port: Port number of the next worker.
            ack_id: Acknowledgement ID for the chain.
        """
        if self.__networking.in_the_same_network(ack_host, ack_port):
            self.__networking.abort_chain(ack_id, resp)
        else:
            await self.__networking.send_message(ack_host, ack_port,
                                                 msg=(ack_id, resp),
                                                 msg_type=MessageType.ChainAbort,
                                                 serializer=Serializer.MSGPACK)

    async def __send_cache_ack(self, ack_host, ack_port, ack_id, resp) -> None:
        """Sends an acknowledgement to the worker that holds the root of a distributed chain during fallback mode with cache enabled.

        Args:
            ack_host: Hostname or IP of the next worker.
            ack_port: Port number of the next worker.
            ack_id: Acknowledgement ID for the cache.
            resp: Response value to acknowledge.
        """
        if self.__networking.in_the_same_network(ack_host, ack_port):
            # case when the ack host is the same worker
            self.__networking.add_ack_cnt(ack_id, resp)
        else:
            await self.__networking.send_message(ack_host, ack_port,
                                                 msg=(ack_id, resp),
                                                 msg_type=MessageType.AckCache,
                                                 serializer=Serializer.MSGPACK)

    async def __send_ack(self,
                         ack_host,
                         ack_port,
                         ack_id,
                         fraction_str,
                         chain_participants,
                         partial_node_count,
                         resp) -> None:
        """Sends an acknowledgement to the worker that holds the root of a distributed chain during normal operation.

        Args:
            ack_host: Hostname or IP of the next worker.
            ack_port: Port number of the next worker.
            ack_id: Acknowledgement ID for the chain.
            fraction_str: Fraction of chain progress.
            chain_participants: List of worker IDs that participated in the chain.
            partial_node_count: Count of nodes contributing to the result.
            resp: Response to propagate.
        """
        if self.__networking.in_the_same_network(ack_host, ack_port):
            # case when the ack host is the same worker
            self.__networking.add_ack_fraction_str(ack_id, fraction_str, chain_participants, partial_node_count, resp)
        else:
            if self.__networking.worker_id not in chain_participants:
                chain_participants.append(self.__networking.worker_id)
            await self.__networking.send_message(ack_host, ack_port,
                                                 msg=(ack_id, fraction_str, chain_participants, partial_node_count, resp),
                                                 msg_type=MessageType.Ack,
                                                 serializer=Serializer.MSGPACK)

    def __materialize_function(self, function_name, partition, key, t_id, request_id,
                               fallback_mode, use_fallback_cache, protocol):
        """Constructs and binds a `StatefulFunction` instance.

        Args:
            function_name: Name of the registered function to bind.
            partition: Target partition number.
            key: The partitioning key.
            t_id: Transaction ID.
            request_id: Unique request identifier.
            fallback_mode: Whether to use fallback logic.
            use_fallback_cache: Whether to use the fallback cache.
            protocol: Coordination protocol to use.

        Returns:
            StatefulFunction: A bound function instance ready for execution.

        Raises:
            OperatorDoesNotContainFunction: If the function is not registered.
        """
        f = StatefulFunction(key,
                             function_name,
                             partition,
                             self.name,
                             self.__state,
                             self.__networking,
                             self.__dns,
                             t_id,
                             request_id,
                             fallback_mode,
                             use_fallback_cache,
                             self.__partitioner,
                             protocol)
        try:
            f.run = self.__functions[function_name]
        except KeyError:
            raise OperatorDoesNotContainFunction(f'Operator: {self.name} does not contain function: {function_name}')
        return f

    def register(self, func: type):
        """Registers a function with this operator.

        Args:
            func (type): The function class or callable to register.
        """
        self.__functions[func.__name__] = func

    def attach_state_networking(self, state, networking, dns):
        """Attaches shared state and networking dependencies.

        Args:
            state: The state backend.
            networking: The networking manager instance.
            dns: Mapping of partition assignments across the cluster.
        """
        self.__state = state
        self.__networking = networking
        self.__dns = dns

    def set_n_partitions(self, n_partitions: int):
        """Sets the number of partitions and updates the partitioner.

        Args:
            n_partitions (int): New number of partitions.
        """
        self.n_partitions = n_partitions
        self.__partitioner.update_partitions(n_partitions)

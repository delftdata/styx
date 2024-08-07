from .message_types import MessageType
from .tcp_networking import NetworkingManager
from .serialization import Serializer
from .base_operator import BaseOperator
from .base_protocol import BaseTransactionalProtocol
from .stateful_function import StatefulFunction
from .exceptions import OperatorDoesNotContainFunction
from .logging import logging


class Operator(BaseOperator):

    def __init__(self,
                 name: str,
                 n_partitions: int = 1):
        super().__init__(name, n_partitions)
        self.__state = ...
        self.__networking: NetworkingManager = ...
        # where the other functions exist
        self.__dns: dict[str, dict[str, tuple[str, int, int]]] = {}
        self.__functions: dict[str, type] = {}

    @property
    def functions(self):
        return self.__functions

    async def run_function(self,
                           key,
                           t_id: int,
                           request_id: bytes,
                           timestamp: int,
                           function_name: str,
                           ack_payload: tuple[str, int, int, str, list[int], int] | None,
                           fallback_mode: bool,
                           use_fallback_cache: bool,
                           params: tuple,
                           protocol: BaseTransactionalProtocol) -> tuple[any, bool]:
        f = self.__materialize_function(function_name, key, t_id, request_id, timestamp,
                                        fallback_mode, use_fallback_cache, protocol)
        params = (f, ) + tuple(params)
        logging.info(f'ack_payload: {ack_payload} RQ_ID: {request_id} TID: {t_id} '
                     f'function: {self.name}:{function_name} fallback mode: {fallback_mode}')
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
                await self._send_chain_abort(resp, ack_host, ack_port, ack_id, request_id)
            elif fallback_mode and use_fallback_cache:
                await self.__send_cache_ack(ack_host, ack_port, ack_id)
            elif n_remote_calls == 0:
                # we need to count the last node as part of the chain
                partial_node_count += 1
                await self.__send_ack(ack_host, ack_port, ack_id, fraction_str, chain_participants, partial_node_count)
        else:
            # root of a chain, or single call
            resp, _, _ = await f(*params)
        del f
        return resp

    async def _send_chain_abort(self, resp, ack_host, ack_port, ack_id, request_id) -> None:
        if self.__networking.in_the_same_network(ack_host, ack_port):
            self.__networking.abort_chain(ack_id, str(resp), request_id)
        else:
            await self.__networking.send_message(ack_host, ack_port,
                                                 msg=(ack_id, str(resp), request_id),
                                                 msg_type=MessageType.ChainAbort,
                                                 serializer=Serializer.MSGPACK)

    async def __send_cache_ack(self, ack_host, ack_port, ack_id) -> None:
        if self.__networking.in_the_same_network(ack_host, ack_port):
            # case when the ack host is the same worker
            await self.__networking.add_ack_cnt(ack_id)
        else:
            await self.__networking.send_message(ack_host, ack_port,
                                                 msg=(ack_id, ),
                                                 msg_type=MessageType.AckCache,
                                                 serializer=Serializer.MSGPACK)

    async def __send_ack(self,
                         ack_host,
                         ack_port,
                         ack_id,
                         fraction_str,
                         chain_participants,
                         partial_node_count) -> None:
        if self.__networking.in_the_same_network(ack_host, ack_port):
            # case when the ack host is the same worker
            await self.__networking.add_ack_fraction_str(ack_id, fraction_str, chain_participants, partial_node_count)
        else:
            if self.__networking.worker_id not in chain_participants:
                chain_participants.append(self.__networking.worker_id)
            await self.__networking.send_message(ack_host, ack_port,
                                                 msg=(ack_id, fraction_str, chain_participants, partial_node_count),
                                                 msg_type=MessageType.Ack,
                                                 serializer=Serializer.MSGPACK)

    def __materialize_function(self, function_name, key, t_id, request_id, timestamp,
                               fallback_mode, use_fallback_cache, protocol):
        f = StatefulFunction(key,
                             function_name,
                             self.name,
                             self.__state,
                             self.__networking,
                             timestamp,
                             self.__dns,
                             t_id,
                             request_id,
                             fallback_mode,
                             use_fallback_cache,
                             protocol)
        try:
            f.run = self.__functions[function_name]
        except KeyError:
            raise OperatorDoesNotContainFunction(f'Operator: {self.name} does not contain function: {function_name}')
        return f

    def register(self, func: type):
        self.__functions[func.__name__] = func

    def attach_state_networking(self, state, networking, dns):
        self.__state = state
        self.__networking = networking
        self.__dns = dns

    def set_n_partitions(self, n_partitions: int):
        self.n_partitions = n_partitions

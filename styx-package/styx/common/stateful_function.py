import asyncio
import fractions
import traceback

from typing import Awaitable, Type

from .logging import logging
from .tcp_networking import NetworkingManager

from .serialization import Serializer
from .function import Function
from .base_state import BaseOperatorState as State
from .base_protocol import BaseTransactionalProtocol
from .message_types import MessageType
from .run_func_payload import RunFuncPayload


class StatefulFunction(Function):

    def __init__(self,
                 key,
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
                 deployed_graph,
                 operator_lock: asyncio.Lock,
                 protocol: BaseTransactionalProtocol):
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

    async def __call__(self, *args, **kwargs):
        try:
            if self.__state.in_remote_keys(self.__key, self.__operator_name, self.__partition):
                # Migration phase, need to get the key from a remote worker first
                async with self.__operator_lock:
                    # Need to lock only for the message send to avoid duplicates
                    await self.__networking.request_key(self.__operator_name,
                                                        self.__partition,
                                                        self.__key,
                                                        self.__state.get_worker_id_old_partition(self.__operator_name,
                                                                                                 self.__partition,
                                                                                                 self.__key))
                await self.__networking.wait_for_remote_key_event(self.__operator_name,
                                                                  self.__partition,
                                                                  self.__key)
            async with self.__operator_lock:
                res = await self.run(*args)
            # logging.info(f'Run args: {args} kwargs: {kwargs} async remote calls: {self.__async_remote_calls}')
            if self.__fallback_enabled and self.__use_fallback_cache:
                # if the fallback is enabled, and we are using the cached functions we can just return here
                return res, len(self.__async_remote_calls), -1
            partial_node_count = 0
            n_remote_calls = 0
            if 'ack_share' in kwargs and 'ack_host' in kwargs:
                # middle of the chain
                partial_node_count = kwargs['partial_node_count']
                n_remote_calls = await self.__send_async_calls(ack_host=kwargs['ack_host'],
                                                               ack_port=kwargs['ack_port'],
                                                               ack_share=kwargs['ack_share'],
                                                               chain_participants=kwargs['chain_participants'],
                                                               partial_node_count=partial_node_count)
            elif len(self.__async_remote_calls) > 0:
                # start of the chain
                n_remote_calls = await self.__send_async_calls(ack_host="",
                                                               ack_port=-1,
                                                               ack_share=1,
                                                               chain_participants=[],
                                                               partial_node_count=partial_node_count,
                                                               is_root=True)
            return res, n_remote_calls, partial_node_count
        except Exception as e:
            logging.warning(traceback.format_exc())
            # logging.debug(f"{self.__request_id} | {self.__t_id} | {self.__key} | "
            #               f"Call @{self.__operator_name}:{self.name}:FB={self.__fallback_enabled} failed with error: {e}")
            return e, -1, -1

    @property
    def data(self):
        return self.__state.get_all(self.__t_id, self.__operator_name, self.__partition)

    @property
    def key(self):
        return self.__key

    async def run(self, *args):
        raise NotImplementedError

    def get(self):
        if self.__fallback_enabled:
            value = self.__state.get_immediate(self.key, self.__t_id, self.__operator_name, self.__partition)
        else:
            value = self.__state.get(self.key, self.__t_id, self.__operator_name, self.__partition)
        # logging.warning(f'GET: {self.key}:{value} '
        #                 f'with t_id: {self.__t_id} '
        #                 f'operator: {self.__operator_name} '
        #                 f'fallback: {self.__fallback_enabled} '
        #                 f'request_id: {self.__request_id} ')
        return value

    def put(self, value):
        # logging.warning(f'PUT: {self.key}:{value} '
        #                 f'with t_id: {self.__t_id} '
        #                 f'operator: {self.__operator_name} '
        #                 f'fallback: {self.__fallback_enabled} '
        #                 f'request_id: {self.__request_id} ')
        if self.__fallback_enabled:
            self.__state.put_immediate(self.key, value, self.__t_id, self.__operator_name, self.__partition)
        else:
            self.__state.put(self.key, value, self.__t_id, self.__operator_name, self.__partition)

    def batch_insert(self, kv_pairs: dict):
        if kv_pairs:
            self.__state.batch_insert(kv_pairs, self.__operator_name, self.__partition)

    async def __send_async_calls(self,
                                 ack_host,
                                 ack_port,
                                 ack_share,
                                 chain_participants: list[int],
                                 partial_node_count: int,
                                 is_root: bool = False):
        n_remote_calls: int = len(self.__async_remote_calls)
        if n_remote_calls == 0:
            return n_remote_calls
        # if fallback is enabled there is no need to call the functions because they are already cached
        new_share_fraction: str = str(fractions.Fraction(f'1/{n_remote_calls}') * fractions.Fraction(ack_share))
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
            ack_payload: tuple[str, int, int, str, list[int], int] = (ack_host,
                                                                      ack_port,
                                                                      self.__t_id,
                                                                      new_share_fraction,
                                                                      chain_participants,
                                                                      partial_node_count)
            # logging.warning(f'Sending F-call with ack payload: {ack_payload}')
            if is_local:
                payload = RunFuncPayload(request_id=self.__request_id,
                                         key=key,
                                         operator_name=operator_name,
                                         partition=partition,
                                         function_name=function_name,
                                         params=params,
                                         ack_payload=ack_payload)
                if self.__fallback_enabled:
                    # and no cache
                    remote_calls.append(self.__protocol.run_fallback_function(t_id=self.__t_id,
                                                                              payload=payload,
                                                                              internal=True))
                else:
                    remote_calls.append(self.__protocol.run_function(t_id=self.__t_id,
                                                                     payload=payload,
                                                                     internal=True))
                    # add to cache
                    self.__networking.add_remote_function_call(self.__t_id, payload)
            else:
                remote_calls.append(self.__call_remote_function_no_response(operator_name=operator_name,
                                                                            function_name=function_name,
                                                                            key=key,
                                                                            partition=partition,
                                                                            params=params,
                                                                            ack_payload=ack_payload))
            partial_node_count = 0
        await asyncio.gather(*remote_calls)
        return n_remote_calls

    def __get_partition(self, operator_name: str, key) -> int:
        return self.__deployed_graph.nodes[operator_name].which_partition(key)

    def call_remote_async(self,
                          operator_name: str,
                          function_name: Type | str,
                          key,
                          params: tuple = tuple()):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition: int = self.__get_partition(operator_name, key)
        is_local: bool = self.__networking.in_the_same_network(self.__dns[operator_name][partition][0],
                                                               self.__dns[operator_name][partition][2])
        self.__async_remote_calls.append((operator_name, function_name, partition, key, params, is_local))

    async def __call_remote_function_no_response(self,
                                                 operator_name: str,
                                                 function_name: Type | str,
                                                 key,
                                                 partition: int,
                                                 params: tuple = tuple(),
                                                 ack_payload=None):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        payload, operator_host, operator_port = self.__prepare_message_transmission(operator_name,
                                                                                    key,
                                                                                    function_name,
                                                                                    partition,
                                                                                    params,
                                                                                    ack_payload)
        await self.__networking.send_message(operator_host,
                                             operator_port,
                                             msg=payload,
                                             msg_type=MessageType.RunFunRemote,
                                             serializer=Serializer.MSGPACK)

    def __prepare_message_transmission(self, operator_name: str, key,
                                       function_name: str, partition: int, params: tuple, ack_payload=None):
        try:

            payload = (self.__t_id,  # __T_ID__
                       self.__request_id,  # __RQ_ID__
                       operator_name,  # __OP_NAME__
                       function_name,  # __FUN_NAME__
                       key,  # __KEY__
                       partition,  # __PARTITION__
                       self.__fallback_enabled,
                       params)  # __PARAMS__
            if ack_payload is not None:
                payload += (ack_payload, )
            operator_host = self.__dns[operator_name][partition][0]
            operator_port = self.__dns[operator_name][partition][2]
        except KeyError:
            logging.error(f"Couldn't find operator: {operator_name} in {self.__dns}")
        else:
            return payload, operator_host, operator_port

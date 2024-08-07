import asyncio
import fractions
import traceback
import uuid

from typing import Awaitable, Type

import mmh3

from .logging import logging
from .tcp_networking import NetworkingManager

from .serialization import Serializer
from .function import Function
from .base_state import BaseOperatorState as State
from .base_protocol import BaseTransactionalProtocol
from .exceptions import NonSupportedKeyType
from .message_types import MessageType
from .run_func_payload import RunFuncPayload


def make_key_hashable(key) -> int:
    if isinstance(key, int):
        return key
    elif isinstance(key, str):
        try:
            # uuid type given by the user
            return uuid.UUID(key).int
        except ValueError:
            return mmh3.hash(key, seed=0, signed=False)
    # if not int, str or uuid throw exception
    raise NonSupportedKeyType()


class StatefulFunction(Function):

    def __init__(self,
                 key,
                 function_name: str,
                 operator_name: str,
                 operator_state: State,
                 networking: NetworkingManager,
                 timestamp: int,
                 dns: dict[str, dict[str, tuple[str, int, int]]],
                 t_id: int,
                 request_id: bytes,
                 fallback_mode: bool,
                 use_fallback_cache: bool,
                 protocol: BaseTransactionalProtocol):
        super().__init__(name=function_name)
        self.__operator_name = operator_name
        self.__state: State = operator_state
        self.__networking: NetworkingManager = networking
        self.__timestamp: int = timestamp
        self.__dns: dict[str, dict[str, tuple[str, int, int]]] = dns
        self.__t_id: int = t_id
        self.__request_id: bytes = request_id
        self.__async_remote_calls: list[tuple[str, str, int, object, tuple, bool]] = []
        self.__fallback_enabled: bool = fallback_mode
        self.__use_fallback_cache: bool = use_fallback_cache
        self.__key = key
        self.__protocol = protocol

    async def __call__(self, *args, **kwargs):
        try:
            res = await self.run(*args)
            logging.info(f'Run args: {args} kwargs: {kwargs} async remote calls: {self.__async_remote_calls}')
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
            logging.debug(traceback.format_exc())
            return e, -1, -1

    @property
    def data(self):
        return self.__state.get_all(self.__t_id, self.__operator_name)

    @property
    def key(self):
        return self.__key

    async def run(self, *args):
        raise NotImplementedError

    def get(self):
        if self.__fallback_enabled:
            value = self.__state.get_immediate(self.key, self.__t_id, self.__operator_name)
        else:
            value = self.__state.get(self.key, self.__t_id, self.__operator_name)
        # logging.info(f'GET: {self.key}:{value} with t_id: {self.__t_id} operator: {self.__operator_name}')
        return value

    def put(self, value):
        # logging.info(f'PUT: {self.key}:{value} with t_id: {self.__t_id} operator: {self.__operator_name}')
        if self.__fallback_enabled:
            self.__state.put_immediate(self.key, value, self.__t_id, self.__operator_name)
        else:
            self.__state.put(self.key, value, self.__t_id, self.__operator_name)

    def batch_insert(self, kv_pairs: dict):
        if kv_pairs:
            self.__state.batch_insert(kv_pairs, self.__operator_name)

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
            await self.__networking.prepare_function_chain(self.__t_id)
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
                                         timestamp=self.__timestamp,
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
                    await self.__networking.add_remote_function_call(self.__t_id, payload)
            else:
                remote_calls.append(self.__call_remote_function_no_response(operator_name=operator_name,
                                                                            function_name=function_name,
                                                                            key=key,
                                                                            partition=partition,
                                                                            params=params,
                                                                            ack_payload=ack_payload))
            partial_node_count = 0
        async with asyncio.TaskGroup() as tg:
            for remote_call in remote_calls:
                tg.create_task(remote_call)
        return n_remote_calls

    def call_remote_async(self,
                          operator_name: str,
                          function_name: Type | str,
                          key,
                          params: tuple = tuple()):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition: int = make_key_hashable(key) % len(self.__dns[operator_name].keys())
        is_local: bool = self.__networking.in_the_same_network(self.__dns[operator_name][str(partition)][0],
                                                               self.__dns[operator_name][str(partition)][2])
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
        logging.info(f"Call RunFunRemote: {payload}")
        await self.__networking.send_message(operator_host,
                                             operator_port,
                                             msg=payload,
                                             msg_type=MessageType.RunFunRemote,
                                             serializer=Serializer.MSGPACK)

    # @deprecated(reason="Request response is no longer supported")
    # async def __call_remote_function_request_response(self,
    #                                                   operator_name: str,
    #                                                   function_name:  Type | str,
    #                                                   partition: int,
    #                                                   key,
    #                                                   params: tuple = tuple()):
    #     if isinstance(function_name, type):
    #         function_name = function_name.__name__
    #     payload, operator_host, operator_port = self.__prepare_message_transmission(operator_name,
    #                                                                                 key,
    #                                                                                 function_name,
    #                                                                                 partition,
    #                                                                                 params)
    #     logging.info(f'(SF)  Start {operator_host}:{operator_port} of {operator_name}:{partition}')
    #     resp = await self.__networking.send_message_request_response(operator_host,
    #                                                                  operator_port,
    #                                                                  msg=payload,
    #                                                                  msg_type=MessageType.RunFunRqRsRemote,
    #                                                                  serializer=Serializer.MSGPACK)
    #     return resp

    def __prepare_message_transmission(self, operator_name: str, key,
                                       function_name: str, partition: int, params: tuple, ack_payload=None):
        try:

            payload = (self.__t_id,  # __T_ID__
                       self.__request_id,  # __RQ_ID__
                       operator_name,  # __OP_NAME__
                       function_name,  # __FUN_NAME__
                       key,  # __KEY__
                       partition,  # __PARTITION__
                       self.__timestamp,  # __TIMESTAMP__
                       self.__fallback_enabled,
                       params)  # __PARAMS__
            if ack_payload is not None:
                payload += (ack_payload, )
            operator_host = self.__dns[operator_name][str(partition)][0]
            operator_port = self.__dns[operator_name][str(partition)][2]
        except KeyError:
            logging.error(f"Couldn't find operator: {operator_name} in {self.__dns}")
        else:
            return payload, operator_host, operator_port

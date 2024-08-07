import asyncio
import fractions
import socket
import struct
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum, auto
from pickle import UnpicklingError

from .exceptions import SerializerNotSupported
from .logging import logging
from .run_func_payload import RunFuncPayload
from .serialization import Serializer, cloudpickle_serialization, msgpack_serialization, \
    pickle_serialization, cloudpickle_deserialization, msgpack_deserialization, pickle_deserialization


class MessagingMode(Enum):
    WORKER_COR = auto()
    PROTOCOL_PROTOCOL = auto()


class BaseNetworking(ABC):

    def __init__(self,
                 host_port,
                 mode: MessagingMode = MessagingMode.WORKER_COR):
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.host_port: int = host_port
        # event_id: ack_event
        self.waited_ack_events: dict[int, asyncio.Event] = {}
        # tid: fraction
        self.ack_fraction: dict[int, fractions.Fraction] = {}
        # tid: (count, total)
        self.ack_cnts: dict[int, tuple[int, int]] = {}
        # t_id: list of workers
        self.chain_participants: dict[int, list[int]] = defaultdict(list)
        self.aborted_events: dict[int, tuple[str, bytes]] = {}
        # t_id: functions that it runs
        self.remote_function_calls: dict[int, list[RunFuncPayload]] = defaultdict(list)
        # set of t_ids that aborted because of an exception
        self.logic_aborts_everywhere: set[int] = set()
        self.messaging_mode = mode
        self.worker_id = -1
        self.lock: asyncio.Lock = asyncio.Lock()

    def __repr__(self):
        return f'{type(self).__name__} ({self.host_name}:{self.host_port}, mode: {self.messaging_mode.name})'

    @abstractmethod
    async def close_all_connections(self):
        raise NotImplementedError

    @abstractmethod
    async def send_message(self,
                           host,
                           port,
                           msg: tuple | bytes,
                           msg_type: int,
                           serializer: Serializer = Serializer.CLOUDPICKLE):
        raise NotImplementedError

    @abstractmethod
    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: tuple | bytes,
                                            msg_type: int,
                                            serializer: Serializer = Serializer.CLOUDPICKLE):
        raise NotImplementedError

    def in_the_same_network(self, host: str, port: int) -> bool:
        return self.host_name == host and self.host_port == port

    def set_worker_id(self, worker_id: int):
        self.worker_id = worker_id

    def cleanup_after_epoch(self):
        self.waited_ack_events = {}
        self.ack_fraction = {}
        self.ack_cnts = {}
        self.aborted_events = {}
        self.remote_function_calls = defaultdict(list)
        self.logic_aborts_everywhere = set()
        self.chain_participants = defaultdict(list)

    async def add_remote_function_call(self, t_id: int, payload: RunFuncPayload):
        async with self.lock:
            self.remote_function_calls[t_id].append(payload)

    def add_chain_participants(self, t_id: int, chain_participants: list[int]):
        for participant in chain_participants:
            if participant != self.worker_id and participant not in self.chain_participants[t_id]:
                self.chain_participants[t_id].append(participant)

    async def add_ack_fraction_str(self,
                                   ack_id: int,
                                   fraction_str: str,
                                   chain_participants: list[int],
                                   partial_node_count: int):
        if ack_id in self.aborted_events:
            # if the transaction was aborted we can instantly return
            return
        try:
            async with self.lock:
                self.add_chain_participants(ack_id, chain_participants)
                self.ack_cnts[ack_id] = (self.ack_cnts[ack_id][0],
                                         self.ack_cnts[ack_id][1] + partial_node_count)
                self.ack_fraction[ack_id] += fractions.Fraction(fraction_str)
                if self.ack_fraction[ack_id] == 1:
                    # All ACK parts have been gathered
                    # logging.warning(f"Transaction total nodes: {self.ack_cnts[ack_id][1]}")
                    self.waited_ack_events[ack_id].set()
                elif self.ack_fraction[ack_id] > 1:
                    # This should never happen, it means that multiple instances of the same function have run
                    logging.error(f'ack: {ack_id} larger than 1 -> {self.ack_fraction[ack_id]}')
        except KeyError:
            logging.error(f'TID: {ack_id} not in ack list!')

    async def add_ack_cnt(self,
                          ack_id: int,
                          cnt: int = 1):
        if ack_id in self.aborted_events:
            # if the transaction was aborted we can instantly return
            return
        try:
            async with self.lock:
                self.ack_cnts[ack_id] = (self.ack_cnts[ack_id][0] + cnt,
                                         self.ack_cnts[ack_id][1])
                if self.ack_cnts[ack_id][0] == self.ack_cnts[ack_id][1]:
                    # All ACK parts have been gathered
                    self.waited_ack_events[ack_id].set()
                elif self.ack_cnts[ack_id][0] > self.ack_cnts[ack_id][1]:
                    # This should never happen
                    logging.error(f'ack: {ack_id} larger than total: '
                                  f'{self.ack_cnts[ack_id][0]}>{self.ack_cnts[ack_id][1]}')
        except KeyError:
            logging.error(f'TID: {ack_id} not in ack list!')

    async def prepare_function_chain(self, t_id: int):
        async with self.lock:
            logging.info(f'New function chain for T_ID: {t_id}')
            self.waited_ack_events[t_id] = asyncio.Event()
            self.ack_fraction[t_id] = fractions.Fraction(0)
            self.ack_cnts[t_id] = (0, 0)

    async def reset_ack_for_fallback(self, ack_id: int):
        async with self.lock:
            if ack_id in self.waited_ack_events:
                self.waited_ack_events[ack_id].clear()
                self.ack_fraction[ack_id] = fractions.Fraction(0)
            else:
                logging.error(f"{ack_id} should exist!")

    async def reset_ack_for_fallback_cache(self, ack_id: int):
        async with self.lock:
            if ack_id in self.waited_ack_events:
                self.waited_ack_events[ack_id].clear()
            else:
                logging.error(f"{ack_id} should exist!")

    def clear_aborted_events_for_fallback(self):
        self.aborted_events = {}
        self.logic_aborts_everywhere = set()

    def abort_chain(self, aborted_t_id: int, exception_str: str, request_id: bytes):
        self.aborted_events[aborted_t_id] = (exception_str, request_id)
        self.transaction_failed(aborted_t_id)
        self.waited_ack_events[aborted_t_id].set()

    def transaction_failed(self, aborted_t_id):
        self.logic_aborts_everywhere.add(aborted_t_id)

    def merge_remote_logic_aborts(self, remote_logic_aborts: set[int]):
        self.logic_aborts_everywhere = self.logic_aborts_everywhere.union(remote_logic_aborts)

    @staticmethod
    def encode_message(msg: object | bytes, msg_type: int, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 0) + cloudpickle_serialization(msg)
            return msg
        elif serializer == Serializer.MSGPACK:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 1) + msgpack_serialization(msg)
            return msg
        elif serializer == Serializer.PICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 2) + pickle_serialization(msg)
            return msg
        elif serializer == Serializer.NONE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 3) + msg
            return msg
        else:
            logging.error(f'Serializer: {serializer} is not supported')
            raise SerializerNotSupported()

    @staticmethod
    def get_msg_type(msg: bytes):
        return msg[0]

    @staticmethod
    def decode_message(data):
        try:
            serializer = data[1]
            if serializer == 0:
                msg = cloudpickle_deserialization(data[2:])
                return msg
            elif serializer == 1:
                msg = msgpack_deserialization(data[2:])
                return msg
            elif serializer == 2:
                msg = pickle_deserialization(data[2:])
                return msg
            elif serializer == 3:
                msg = data[2:]
                return msg
            else:
                logging.error(f'Serializer: {serializer} is not supported')
                raise SerializerNotSupported()
        except UnpicklingError:
            logging.error(f'Unpickling msg: {data}')
            raise UnpicklingError

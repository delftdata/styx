import asyncio
import fractions
import socket
import struct
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import IntEnum
from pickle import UnpicklingError

from .exceptions import SerializerNotSupported
from .logging import logging
from .run_func_payload import RunFuncPayload
from .serialization import Serializer, cloudpickle_serialization, msgpack_serialization, \
    pickle_serialization, cloudpickle_deserialization, msgpack_deserialization, pickle_deserialization, \
    zstd_msgpack_deserialization, zstd_msgpack_serialization


class MessagingMode(IntEnum):
    WORKER_COR = 0
    PROTOCOL_PROTOCOL = 1
    HEARTBEAT = 2


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
        self.aborted_events: dict[int, str] = {}
        self.client_responses: dict[int, str] = {}
        # t_id: functions that it runs
        self.remote_function_calls: dict[int, list[RunFuncPayload]] = defaultdict(list)
        # set of t_ids that aborted because of an exception
        self.logic_aborts_everywhere: set[int] = set()
        self.messaging_mode = mode
        self.worker_id = -1

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
        self.waited_ack_events.clear()
        self.ack_fraction.clear()
        self.ack_cnts.clear()
        self.aborted_events.clear()
        self.remote_function_calls.clear()
        self.logic_aborts_everywhere.clear()
        self.chain_participants.clear()
        self.client_responses.clear()

    def add_remote_function_call(self, t_id: int, payload: RunFuncPayload):
        self.remote_function_calls[t_id].append(payload)

    def add_chain_participants(self, t_id: int, chain_participants: list[int]):
        for participant in chain_participants:
            if participant != self.worker_id and participant not in self.chain_participants[t_id]:
                self.chain_participants[t_id].append(participant)

    def add_ack_fraction_str(self,
                             ack_id: int,
                             fraction_str: str,
                             chain_participants: list[int],
                             partial_node_count: int):
        if ack_id in self.aborted_events:
            # if the transaction was aborted we can instantly return
            return
        try:
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

    def add_ack_cnt(self,
                    ack_id: int,
                    cnt: int = 1,
                    ):
        if ack_id in self.aborted_events:
            # if the transaction was aborted we can instantly return
            return
        try:
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

    def prepare_function_chain(self, t_id: int):
        logging.info(f'New function chain for T_ID: {t_id}')
        self.waited_ack_events[t_id] = asyncio.Event()
        self.ack_fraction[t_id] = fractions.Fraction(0)
        self.ack_cnts[t_id] = (0, 0)

    def reset_ack_for_fallback(self, ack_id: int):
        if ack_id in self.waited_ack_events:
            self.waited_ack_events[ack_id].clear()
            self.ack_fraction[ack_id] = fractions.Fraction(0)
        else:
            logging.error(f"ACK_ID: {ack_id} should exist in the reset_ack_for_fallback!")

    def reset_ack_for_fallback_cache(self, ack_id: int):
        if ack_id in self.waited_ack_events:
            self.waited_ack_events[ack_id].clear()
        else:
            logging.error(f"ACK_ID: {ack_id} should exist in the reset_ack_for_fallback_cache!")

    def clear_aborted_events_for_fallback(self):
        self.aborted_events.clear()
        self.logic_aborts_everywhere.clear()
        self.client_responses.clear()

    def abort_chain(self, aborted_t_id: int, exception_str: str):
        if aborted_t_id not in self.aborted_events:
            self.aborted_events[aborted_t_id] = exception_str
            self.logic_aborts_everywhere.add(aborted_t_id)
        if aborted_t_id in self.waited_ack_events:
            self.waited_ack_events[aborted_t_id].set()

    def add_response(self, t_id: int, response: str):
        if response is None:
            logging.error(f'Response for T_ID: {t_id} should not be None!')
        self.client_responses[t_id] = response

    def merge_remote_logic_aborts(self, remote_logic_aborts: set[int]):
        self.logic_aborts_everywhere = self.logic_aborts_everywhere.union(remote_logic_aborts)

    @staticmethod
    def encode_message(msg: object | bytes, msg_type: int, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 0) + cloudpickle_serialization(msg)
            return msg
        elif serializer == Serializer.MSGPACK:
            ser_msg: bytes = msgpack_serialization(msg)
            ser_id = 1
            if len(ser_msg) > 1_048_576:
                # If it's more than 1MB compress by default
                ser_msg = zstd_msgpack_serialization(ser_msg, already_ser=True)
                ser_id = 4
            msg = struct.pack('>B', msg_type) + struct.pack('>B', ser_id) + ser_msg
            return msg
        elif serializer == Serializer.PICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 2) + pickle_serialization(msg)
            return msg
        elif serializer == Serializer.NONE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 3) + msg
            return msg
        elif serializer == Serializer.COMPRESSED_MSGPACK:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 4) + zstd_msgpack_serialization(msg)
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
            elif serializer == 4:
                msg = zstd_msgpack_deserialization(data[2:])
                return msg
            else:
                logging.error(f'Serializer: {serializer} is not supported')
                raise SerializerNotSupported()
        except UnpicklingError:
            logging.error(f'Unpickling msg: {data}')
            raise UnpicklingError

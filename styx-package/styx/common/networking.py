import asyncio
import dataclasses
import fractions
import struct
import socket
from enum import Enum, auto

import zmq
from aiozmq import create_zmq_stream, ZmqStream

from .run_func_payload import RunFuncPayload
from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization, pickle_serialization, pickle_deserialization
from .exceptions import SerializerNotSupported


@dataclasses.dataclass
class SocketConnection(object):
    zmq_socket: ZmqStream
    socket_lock: asyncio.Lock


class MessagingMode(Enum):
    WORKER_COR = auto()
    PROTOCOL_PROTOCOL = auto()


class SocketPool(object):

    def __init__(self, host: str, port: int, size: int = 4, mode: MessagingMode = MessagingMode.WORKER_COR):
        self.host = host
        self.port = port
        self.size = size
        self.conns: list[SocketConnection] = []
        self.index: int = 0
        self.messaging_mode: MessagingMode = mode
        if self.messaging_mode == MessagingMode.WORKER_COR:
            self.zmq_socket_type = zmq.DEALER
        else:
            self.zmq_socket_type = zmq.PUSH

    def __iter__(self):
        return self

    def __next__(self) -> SocketConnection:
        conn = self.conns[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def create_socket_connections(self):
        for _ in range(self.size):
            soc = await create_zmq_stream(self.zmq_socket_type, connect=f"tcp://{self.host}:{self.port}",
                                          high_read=0, high_write=0)
            soc.transport.setsockopt(zmq.LINGER, 0)
            # soc.transport.setsockopt(zmq.IMMEDIATE, 1)
            self.conns.append(SocketConnection(soc, asyncio.Lock()))

    def close(self):
        for conn in self.conns:
            conn.zmq_socket.close()
            if conn.socket_lock.locked():
                conn.socket_lock.release()
        self.conns = []


class NetworkingManager(object):

    def __init__(self, host_port, size: int = 4, mode: MessagingMode = MessagingMode.WORKER_COR):
        # HERE BETTER TO ADD A CONNECTION POOL
        self.pools: dict[tuple[str, int], SocketPool] = {}
        self.get_socket_lock = asyncio.Lock()
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.host_port: int = host_port
        # event_id: ack_event
        self.waited_ack_events: dict[int, asyncio.Event] = {}
        self.ack_fraction: dict[int, fractions.Fraction] = {}
        # t_id: list of workers
        self.chain_participants: dict[int, list[int]] = {}
        self.aborted_events: dict[int, str] = {}
        # t_id: functions that it runs
        self.remote_function_calls: dict[int, list[RunFuncPayload]] = {}
        # set of t_ids that aborted because of an exception
        self.logic_aborts_everywhere: set[int] = set()
        self.socket_pool_size = size
        self.messaging_mode = mode
        self.worker_id = -1

    def in_the_same_network(self, host: str, port: int) -> bool:
        return self.host_port == host and self.host_port == port

    def set_worker_id(self, worker_id: int):
        self.worker_id = worker_id

    def cleanup_after_epoch(self):
        self.waited_ack_events = {}
        self.ack_fraction = {}
        self.aborted_events = {}
        self.remote_function_calls = {}
        self.logic_aborts_everywhere = set()
        self.chain_participants = {}

    def add_remote_function_call(self, t_id: int, payload: RunFuncPayload):
        if t_id in self.remote_function_calls:
            self.remote_function_calls[t_id].append(payload)
        else:
            self.remote_function_calls[t_id] = [payload]

    def add_chain_participants(self, t_id: int, chain_participants: list[int]):
        for participant in chain_participants:
            if participant not in self.chain_participants[t_id] and participant != self.worker_id:
                self.chain_participants[t_id].append(participant)

    def add_ack_fraction_str(self, ack_id: int, fraction_str: str, chain_participants: list[int]):
        try:
            if ack_id in self.aborted_events:
                return
            self.add_chain_participants(ack_id, chain_participants)
            self.ack_fraction[ack_id] += fractions.Fraction(fraction_str)
            if self.ack_fraction[ack_id] == 1:
                # All ACK parts have been gathered
                self.waited_ack_events[ack_id].set()
            elif self.ack_fraction[ack_id] > 1:
                # This should never happen, it means that multiple instances of the same function have run
                logging.error(f'WTF ack: {ack_id} larger than 1 -> {self.ack_fraction[ack_id]}')
        except KeyError:
            logging.error(f'TID: {ack_id} not in ack list!')

    async def close_all_connections(self):
        async with self.get_socket_lock:
            for pool in self.pools.values():
                pool.close()
            self.pools = {}

    async def create_socket_connection(self, host: str, port):
        self.pools[(host, port)] = SocketPool(host, port, size=self.socket_pool_size, mode=self.messaging_mode)
        await self.pools[(host, port)].create_socket_connections()

    async def close_socket_connection(self, host: str, port):
        async with self.get_socket_lock:
            if (host, port) in self.pools:
                self.pools[(host, port)].close()
                del self.pools[(host, port)]
            else:
                logging.warning('The socket that you are trying to close does not exist')

    async def send_message(self,
                           host,
                           port,
                           msg: tuple | bytes,
                           msg_type: int,
                           serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        msg = self.encode_message(msg, msg_type, serializer)
        socket_conn.zmq_socket.write((msg, ))

    def prepare_function_chain(self, t_id: int):
        self.waited_ack_events[t_id] = asyncio.Event()
        self.ack_fraction[t_id] = fractions.Fraction(0)
        self.chain_participants[t_id] = []

    def reset_ack_for_fallback(self, ack_id: int):
        if ack_id in self.waited_ack_events and self.waited_ack_events[ack_id].is_set():
            self.waited_ack_events[ack_id].clear()
            self.ack_fraction[ack_id] = fractions.Fraction(0)

    def clear_aborted_events_for_fallback(self):
        self.aborted_events = {}

    def abort_chain(self, aborted_t_id: int, exception_str: str):
        self.aborted_events[aborted_t_id] = exception_str
        self.transaction_failed(aborted_t_id)
        self.waited_ack_events[aborted_t_id].set()

    def transaction_failed(self, aborted_t_id):
        self.logic_aborts_everywhere.add(aborted_t_id)

    def merge_remote_logic_aborts(self, remote_logic_aborts: set[int]):
        self.logic_aborts_everywhere = self.logic_aborts_everywhere.union(remote_logic_aborts)

    async def __receive_message(self, sock):
        # To be used only by the request response because the lock is needed
        answer = await sock.read()
        return self.decode_message(answer[0])

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: tuple | bytes,
                                            msg_type: int,
                                            serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        async with socket_conn.socket_lock:
            await self.__send_message_given_sock(socket_conn.zmq_socket, msg, msg_type, serializer)
            resp = await self.__receive_message(socket_conn.zmq_socket)
            logging.info("NETWORKING MODULE RECEIVED RESPONSE")
            return resp

    async def __send_message_given_sock(self, sock, msg: object, msg_type: int, serializer: Serializer):
        msg = self.encode_message(msg, msg_type, serializer)
        sock.write((msg, ))

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

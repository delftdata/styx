from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict
from enum import IntEnum
import os
from pickle import UnpicklingError
import socket
import struct

from setuptools._distutils.util import strtobool

from styx.common.exceptions import SerializerNotSupportedError
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import (
    Serializer,
    cloudpickle_deserialization,
    cloudpickle_serialization,
    msgpack_deserialization,
    msgpack_serialization,
    pickle_deserialization,
    pickle_serialization,
    zstd_msgpack_deserialization,
    zstd_msgpack_serialization,
)

USE_COMPRESSION: bool = bool(strtobool(os.getenv("ENABLE_COMPRESSION", "true")))
COMPRESS_AFTER: int = int(os.getenv("COMPRESS_AFTER", "4096"))
BATCH_FLUSH_INTERVAL_MS: float = float(os.getenv("BATCH_FLUSH_INTERVAL_MS", "1"))


class MessagingMode(IntEnum):
    WORKER_COR = 0
    PROTOCOL_PROTOCOL = 1
    HEARTBEAT = 2


class BaseNetworking(ABC):
    def __init__(
        self,
        host_port: int,
        mode: MessagingMode = MessagingMode.WORKER_COR,
    ) -> None:
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.host_port: int = host_port

        # event_id: |ack_event|
        self.waited_ack_events: dict[int, asyncio.Event] = {}
        # tid: |float fraction accumulated by leaves of the chain|
        # We use float (not fractions.Fraction) for speed; chain ACK shares
        # are 1/N at each level, and sums of those across leaves converge
        # close to 1.0. Compare with an ε tolerance instead of equality.
        self.ack_fraction: dict[int, float] = {}
        # t_id: |list of workers|
        self.chain_participants: dict[int, list[int]] = defaultdict(list)
        self.aborted_events: dict[int, str] = {}
        self.client_responses: dict[int, str] = {}
        # set of t_ids that aborted because of an exception
        self.logic_aborts_everywhere: set[int] = set()

        self.messaging_mode: MessagingMode = mode
        self.worker_id: int = -1

    def __repr__(self) -> str:
        return f"{type(self).__name__} ({self.host_name}:{self.host_port}, mode: {self.messaging_mode.name})"

    @abstractmethod
    async def close_all_connections(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send_message(
        self,
        host: str,
        port: int,
        msg: tuple | bytes,
        msg_type: int,
        serializer: Serializer = Serializer.CLOUDPICKLE,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send_message_request_response(
        self,
        host: str,
        port: int,
        msg: tuple | bytes,
        msg_type: int,
        serializer: Serializer = Serializer.CLOUDPICKLE,
    ) -> object:
        raise NotImplementedError

    def in_the_same_network(self, host: str, port: int) -> bool:
        return self.host_name == host and self.host_port == port

    def set_worker_id(self, worker_id: int) -> None:
        self.worker_id = worker_id

    def cleanup_after_epoch(self) -> None:
        self.waited_ack_events.clear()
        self.ack_fraction.clear()
        self.aborted_events.clear()
        self.logic_aborts_everywhere.clear()
        self.chain_participants.clear()
        self.client_responses.clear()

    def add_chain_participants(self, t_id: int, chain_participants: list[int]) -> None:
        for participant in chain_participants:
            if participant != self.worker_id and participant not in self.chain_participants[t_id]:
                self.chain_participants[t_id].append(participant)

    # Float ACK accounting: each chain leaf contributes share = 1/(N1*N2*...)
    # where Ni is the fan-out at level i. Sum of all leaf shares should equal
    # 1.0 modulo floating-point error. ε of 1e-9 tolerates accumulation error
    # for chains with up to ~10^9 leaves — vastly more than any real workload.
    _ACK_FRACTION_COMPLETE_EPSILON: float = 1e-9

    def add_ack_fraction_str(
        self,
        ack_id: int,
        fraction_str: str,
        chain_participants: list[int],
    ) -> None:
        if ack_id in self.aborted_events:
            # if the transaction was aborted we can instantly return
            return
        try:
            self.add_chain_participants(ack_id, chain_participants)
            self.ack_fraction[ack_id] += float(fraction_str)
            if self.ack_fraction[ack_id] >= 1.0 - self._ACK_FRACTION_COMPLETE_EPSILON:
                self.waited_ack_events[ack_id].set()
        except KeyError:
            logging.error(f"TID: {ack_id} not in ack list!")

    def prepare_function_chain(self, t_id: int) -> None:
        logging.info(f"New function chain for T_ID: {t_id}")
        self.waited_ack_events[t_id] = asyncio.Event()
        self.ack_fraction[t_id] = 0.0

    def reset_ack_for_fallback(self, ack_id: int) -> None:
        if ack_id in self.waited_ack_events:
            self.waited_ack_events[ack_id].clear()
            self.ack_fraction[ack_id] = 0.0

    def clear_aborted_events_for_fallback(self) -> None:
        self.aborted_events.clear()
        self.logic_aborts_everywhere.clear()
        self.client_responses.clear()

    def abort_chain(self, aborted_t_id: int, exception_str: str) -> None:
        if aborted_t_id not in self.aborted_events:
            self.aborted_events[aborted_t_id] = exception_str
            self.logic_aborts_everywhere.add(aborted_t_id)
        if aborted_t_id in self.waited_ack_events:
            self.waited_ack_events[aborted_t_id].set()

    def add_response(self, t_id: int, response: str | Exception) -> None:
        if response is None:
            logging.error(f"Response for T_ID: {t_id} should not be None!")
        self.client_responses[t_id] = response

    def merge_remote_logic_aborts(self, remote_logic_aborts: set[int]) -> None:
        self.logic_aborts_everywhere = self.logic_aborts_everywhere.union(
            remote_logic_aborts,
        )

    @staticmethod
    def encode_message(
        msg: object | bytes,
        msg_type: int,
        serializer: Serializer,
    ) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            return struct.pack(">B", msg_type) + struct.pack(">B", 0) + cloudpickle_serialization(msg)
        if serializer == Serializer.MSGPACK:
            ser_msg: bytes = msgpack_serialization(msg)
            ser_id = 1
            if USE_COMPRESSION and len(ser_msg) > COMPRESS_AFTER:
                # If it's more than 4KB compress
                ser_msg = zstd_msgpack_serialization(ser_msg, already_ser=True)
                ser_id = 4
            return struct.pack(">B", msg_type) + struct.pack(">B", ser_id) + ser_msg
        if serializer == Serializer.PICKLE:
            return struct.pack(">B", msg_type) + struct.pack(">B", 2) + pickle_serialization(msg)
        if serializer == Serializer.NONE:
            return struct.pack(">B", msg_type) + struct.pack(">B", 3) + msg
        if serializer == Serializer.COMPRESSED_MSGPACK:
            return struct.pack(">B", msg_type) + struct.pack(">B", 4) + zstd_msgpack_serialization(msg)
        logging.error(f"Serializer: {serializer} is not supported")
        raise SerializerNotSupportedError

    @staticmethod
    def get_msg_type(msg: bytes) -> MessageType:
        return MessageType(msg[0])

    @staticmethod
    def decode_message(data: bytes) -> object:
        try:
            serializer = data[1]
            if serializer == Serializer.CLOUDPICKLE:
                return cloudpickle_deserialization(data[2:])
            if serializer == Serializer.MSGPACK:
                return msgpack_deserialization(data[2:])
            if serializer == Serializer.PICKLE:
                return pickle_deserialization(data[2:])
            if serializer == Serializer.NONE:
                return data[2:]
            if serializer == Serializer.COMPRESSED_MSGPACK:
                return zstd_msgpack_deserialization(data[2:])
            logging.error(f"Serializer: {serializer} is not supported")
            raise SerializerNotSupportedError
        except UnpicklingError:
            logging.error(f"Unpickling msg: {data}")
            raise

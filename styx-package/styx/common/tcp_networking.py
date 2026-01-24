import asyncio
import os
import socket
import struct
import sys
from struct import unpack
from timeit import default_timer as timer
from typing import Any

from setuptools._distutils.util import strtobool
from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition

from .exceptions import SerializerNotSupported
from .base_networking import BaseNetworking, MessagingMode
from .logging import logging
from .serialization import (
    Serializer,
    cloudpickle_serialization,
    msgpack_serialization,
    pickle_serialization,
    zstd_msgpack_serialization,
)
from .util.aio_task_scheduler import AIOTaskScheduler

USE_COMPRESSION: bool = bool(strtobool(os.getenv("ENABLE_COMPRESSION", "true")))


class StyxSocketClient:
    def __init__(self):
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.target_host: str | None = None
        self.target_port: int | None = None
        self.lock = asyncio.Lock()
        self.n_retries = 3

    async def create_connection(self, host, port) -> bool:
        self.target_host = host
        self.target_port = port
        success = True
        i = 0
        while i < self.n_retries:
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.target_host, self.target_port, limit=2**32
                )
            except socket.error as e:
                logging.warning(f"{host}:{port} is not up yet, sleeping for 500 msec -> {e}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Uncaught exception: {e}")
            else:
                logging.info(f"Connection made to {host}:{port}")
                break
            i += 1
        if i == self.n_retries:
            logging.error(f"Cannot connect to worker {host}:{port} after {self.n_retries} attempts.")
            success = False
        return success

    async def send_message(self, message: bytes):
        i = 0
        async with self.lock:
            while i < self.n_retries:
                try:
                    if self.writer is None:
                        raise ConnectionError("Writer is not initialized (connection not established).")
                    self.writer.write(message)
                    await self.writer.drain()
                except (RuntimeError, ConnectionResetError, socket.error, BrokenPipeError):
                    logging.warning(
                        f"Broken connection in rq-rs, close the old ones and retry. "
                        f"Attempt {i} at {self.target_host}:{self.target_port}"
                    )
                    await self.close()
                    await asyncio.sleep(0.5)
                    await self.create_connection(self.target_host, self.target_port)
                except Exception as e:
                    logging.error(f"Uncaught exception: {e}")
                    i = self.n_retries
                    break
                else:
                    break
                i += 1
        if i == self.n_retries:
            logging.error(f"Cannot send_message_rq_rs to worker {self.target_host}:{self.target_port}")

    async def send_message_rq_rs(self, message: bytes) -> bytes | None:
        i = 0
        resp: bytes | None = None
        async with self.lock:
            while i < self.n_retries:
                try:
                    if self.writer is None or self.reader is None:
                        raise ConnectionError("Reader/Writer not initialized (connection not established).")
                    self.writer.write(message)
                    await self.writer.drain()
                    (size,) = unpack(">Q", await self.reader.readexactly(8))
                    resp = await self.reader.readexactly(size)
                except (RuntimeError, ConnectionResetError, socket.error, BrokenPipeError):
                    logging.warning(
                        f"Broken connection in rq-rs, close the old ones and retry. "
                        f"Attempt {i} at {self.target_host}:{self.target_port}"
                    )
                    await self.close()
                    await asyncio.sleep(0.5)
                    await self.create_connection(self.target_host, self.target_port)
                except Exception as e:
                    logging.error(f"Uncaught exception: {e}")
                    i = self.n_retries
                    break
                else:
                    break
                i += 1
        if i == self.n_retries:
            logging.error(f"Cannot send_message_rq_rs to worker {self.target_host}:{self.target_port}")
        return resp

    async def close(self):
        try:
            if self.writer is not None:
                self.writer.close()
                await self.writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            logging.warning(
                f"Worker failure detected {self.target_host}:{self.target_port} "
                f"[Connection reset by peer] Recovery will be automatically initiated."
            )
        except Exception as e:
            logging.error(f"Uncaught exception: {e}")
        finally:
            self.reader = None
            self.writer = None


class SocketPool:
    def __init__(self, host: str, port: int, size: int = 4, mode: MessagingMode = MessagingMode.WORKER_COR):
        self.host = host
        self.port = port
        self.size = size
        self.conns: list[StyxSocketClient] = []
        self.index: int = 0
        self.messaging_mode: MessagingMode = mode

    def __iter__(self):
        return self

    def __next__(self) -> StyxSocketClient:
        conn = self.conns[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def create_socket_connections(self):
        for _ in range(self.size):
            client = StyxSocketClient()
            await client.create_connection(self.host, self.port)
            self.conns.append(client)

    async def close(self):
        for conn in self.conns:
            await conn.close()
        self.conns = []


class NetworkingManager(BaseNetworking):
    def __init__(self, host_port, size: int = 4, mode: MessagingMode = MessagingMode.WORKER_COR):
        super().__init__(host_port, mode)
        self.aio_task_scheduler = AIOTaskScheduler()
        self.pools: dict[tuple[str, int], SocketPool] = {}
        self.get_socket_lock = asyncio.Lock()
        self.socket_pool_size = size
        self.send_message_time_ms = 0.0
        self.send_message_calls = 0
        self.send_message_size = 0

        self.peers: dict[int, tuple[str, int, int]] = {}
        self.wait_remote_key_event: dict[OperatorPartition, dict[Any, asyncio.Event]] = {}

    async def close_all_connections(self):
        for pool in self.pools.values():
            await pool.close()
        self.pools = {}

    async def close_worker_connections(self, host, port):
        pool = self.pools.pop((host, port), None)
        if pool is not None:
            await pool.close()

    def start_networking_tasks(self):
        self.aio_task_scheduler.create_task(self.start_metrics())

    async def create_socket_connection(self, host: str, port: int):
        self.pools[(host, port)] = SocketPool(host, port, size=self.socket_pool_size, mode=self.messaging_mode)
        await self.pools[(host, port)].create_socket_connections()

    async def send_message(
        self,
        host,
        port,
        msg: tuple | bytes,
        msg_type: int,
        serializer: Serializer = Serializer.CLOUDPICKLE,
    ):
        start_time = timer()
        msg = self.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        await socket_conn.send_message(msg)
        end_time = timer()
        self.send_message_time_ms += (end_time - start_time) * 1000.0
        self.send_message_calls += 1
        self.send_message_size += sys.getsizeof(msg)

    def set_peers(self, peers: dict[int, tuple[str, int, int]]):
        self.peers = peers

    async def request_key(
        self,
        operator_name: str,
        partition: int,
        key: Any,
        worker_id_old_part: tuple[int, int] | None,
    ):
        operator_partition = (operator_name, partition)
        if (
            worker_id_old_part is None
            or (
                operator_partition in self.wait_remote_key_event
                and key in self.wait_remote_key_event[operator_partition]
            )
        ):
            # If a request for that key is already made don't send it again
            return

        worker_id, old_partition = worker_id_old_part
        host, port, _ = self.peers[worker_id]

        if operator_partition in self.wait_remote_key_event:
            self.wait_remote_key_event[operator_partition][key] = asyncio.Event()
        else:
            self.wait_remote_key_event[operator_partition] = {key: asyncio.Event()}

        await self.send_message(
            host,
            port,
            (operator_partition, key, old_partition, self.host_name, self.host_port - 1000),
            MessageType.RequestRemoteKey,
            serializer=Serializer.MSGPACK,
        )

    async def wait_for_remote_key_event(self, operator_name: str, partition: int, key: Any):
        operator_partition = (operator_name, partition)
        ev = self.wait_remote_key_event[operator_partition][key]
        await ev.wait()
        # (6) cleanup to prevent unbounded growth
        del self.wait_remote_key_event[operator_partition][key]
        if not self.wait_remote_key_event[operator_partition]:
            del self.wait_remote_key_event[operator_partition]

    def key_received(self, operator_partition: OperatorPartition, key: Any):
        operator_partition = tuple(operator_partition)
        self.wait_remote_key_event[operator_partition][key].set()

    async def send_message_request_response(
        self,
        host,
        port,
        msg: tuple | bytes,
        msg_type: int,
        serializer: Serializer = Serializer.CLOUDPICKLE,
    ):
        msg = self.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])

        raw = await socket_conn.send_message_rq_rs(msg)
        if raw is None:
            raise ConnectionError(f"No response from {host}:{port} for msg_type={msg_type}")

        resp = self.decode_message(raw)
        return resp

    async def start_metrics(self, interval=30):
        while True:
            await asyncio.sleep(interval)
            await self.log_metrics()

    async def log_metrics(self):
        avg_msg_time_ms = (
            self.send_message_time_ms / self.send_message_calls
            if self.send_message_calls > 0
            else 0
        )
        avg_msg_size = (
            self.send_message_size // self.send_message_calls
            if self.send_message_calls > 0
            else 0
        )
        logging.warning(
            f"{self}: "
            f"Send_message calls: {self.send_message_calls}, "
            f"Send_message time: {self.send_message_time_ms} ms, "
            f"Avg message time: {avg_msg_time_ms} ms, "
            f"Avg message size: {avg_msg_size} B."
        )

    @staticmethod
    def encode_message(msg: object | bytes, msg_type: int, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack(">B", msg_type) + struct.pack(">B", 0) + cloudpickle_serialization(msg)
        elif serializer == Serializer.MSGPACK:
            ser_msg: bytes = msgpack_serialization(msg)
            ser_id = 1
            if USE_COMPRESSION and len(ser_msg) > 4096:
                # If it's more than 4KB compress
                ser_msg = zstd_msgpack_serialization(ser_msg, already_ser=True)
                ser_id = 4
            msg = struct.pack(">B", msg_type) + struct.pack(">B", ser_id) + ser_msg
        elif serializer == Serializer.PICKLE:
            msg = struct.pack(">B", msg_type) + struct.pack(">B", 2) + pickle_serialization(msg)
        elif serializer == Serializer.NONE:
            msg = struct.pack(">B", msg_type) + struct.pack(">B", 3) + msg
        elif serializer == Serializer.COMPRESSED_MSGPACK:
            msg = struct.pack(">B", msg_type) + struct.pack(">B", 4) + zstd_msgpack_serialization(msg)
        else:
            logging.error(f"Serializer: {serializer} is not supported")
            raise SerializerNotSupported()

        msg = struct.pack(">Q", len(msg)) + msg
        return msg

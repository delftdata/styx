import asyncio
import os
import struct
from struct import unpack
from typing import TYPE_CHECKING

from setuptools._distutils.util import strtobool

from styx.common.base_networking import BATCH_FLUSH_INTERVAL_MS, BaseNetworking, MessagingMode
from styx.common.exceptions import SerializerNotSupportedError
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import (
    Serializer,
    cloudpickle_serialization,
    msgpack_serialization,
    pickle_serialization,
    zstd_msgpack_serialization,
)
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

if TYPE_CHECKING:
    from styx.common.types import K, OperatorPartition

USE_COMPRESSION: bool = bool(strtobool(os.getenv("ENABLE_COMPRESSION", "true")))
COMPRESS_AFTER: int = int(os.getenv("COMPRESS_AFTER", "4096"))


class StyxSocketClient:
    def __init__(self) -> None:
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.target_host: str | None = None
        self.target_port: int | None = None
        self.lock: asyncio.Lock = asyncio.Lock()
        self.n_retries: int = 3
        self._send_buffer: list[bytes] = []

    def buffer_message(self, message: bytes) -> None:
        """Append message to the send buffer without flushing to the socket."""
        self._send_buffer.append(message)

    async def flush(self) -> None:
        """Write all buffered messages in one write()+drain() call."""
        if not self._send_buffer:
            return
        async with self.lock:
            if not self._send_buffer:
                return
            if self.writer is None:
                logging.error("flush called but writer is not initialized — discarding buffer.")
                self._send_buffer.clear()
                return
            data = b"".join(self._send_buffer)
            self._send_buffer.clear()
            i = 0
            while i < self.n_retries:
                try:
                    self.writer.write(data)
                    await self.writer.drain()
                    break
                except (OSError, RuntimeError, ConnectionResetError, BrokenPipeError):
                    logging.warning(
                        f"Broken connection during flush, reconnecting. "
                        f"Attempt {i} at {self.target_host}:{self.target_port}",
                    )
                    await self.close()
                    await asyncio.sleep(0.5)
                    await self.create_connection(self.target_host, self.target_port)
                except Exception as e:
                    logging.error(f"Uncaught exception during flush: {e}")
                    i = self.n_retries
                    break
                i += 1
            if i == self.n_retries:
                logging.error(
                    f"Cannot flush to {self.target_host}:{self.target_port} after {self.n_retries} attempts.",
                )

    async def create_connection(self, host: str, port: int) -> bool:
        self.target_host = host
        self.target_port = port
        success = True
        i = 0
        while i < self.n_retries:
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.target_host,
                    self.target_port,
                    limit=2**32,
                )
            except OSError as e:
                logging.warning(
                    f"{host}:{port} is not up yet, sleeping for 500 msec -> {e}",
                )
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Uncaught exception: {e}")
            else:
                logging.info(f"Connection made to {host}:{port}")
                break
            i += 1
        if i == self.n_retries:
            logging.error(
                f"Cannot connect to worker {host}:{port} after {self.n_retries} attempts.",
            )
            success = False
        return success

    async def send_message(self, message: bytes) -> None:
        if self.writer is None:
            msg = "Writer is not initialized (connection not established)."
            raise ConnectionError(msg)
        i = 0
        async with self.lock:
            while i < self.n_retries:
                try:
                    self.writer.write(message)
                    await self.writer.drain()
                except OSError, RuntimeError, ConnectionResetError, BrokenPipeError:
                    logging.warning(
                        f"Broken connection in rq-rs, close the old ones and retry. "
                        f"Attempt {i} at {self.target_host}:{self.target_port}",
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
            logging.error(
                f"Cannot send_message_rq_rs to worker {self.target_host}:{self.target_port}",
            )

    async def send_message_rq_rs(self, message: bytes) -> bytes | None:
        if self.writer is None or self.reader is None:
            msg = "Reader/Writer not initialized (connection not established)."
            raise ConnectionError(msg)
        i = 0
        resp: bytes | None = None
        async with self.lock:
            while i < self.n_retries:
                try:
                    self.writer.write(message)
                    await self.writer.drain()
                    (size,) = unpack(">Q", await self.reader.readexactly(8))
                    resp = await self.reader.readexactly(size)
                except OSError, RuntimeError, ConnectionResetError, BrokenPipeError:
                    logging.warning(
                        f"Broken connection in rq-rs, close the old ones and retry. "
                        f"Attempt {i} at {self.target_host}:{self.target_port}",
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
            logging.error(
                f"Cannot send_message_rq_rs to worker {self.target_host}:{self.target_port}",
            )
        return resp

    async def close(self) -> None:
        self._send_buffer.clear()
        try:
            if self.writer is not None:
                self.writer.close()
                await self.writer.wait_closed()
        except ConnectionResetError, BrokenPipeError:
            logging.warning(
                f"Worker failure detected {self.target_host}:{self.target_port} "
                f"[Connection reset by peer] Recovery will be automatically initiated.",
            )
        except Exception as e:
            logging.error(f"Uncaught exception: {e}")
        finally:
            self.reader = None
            self.writer = None


class SocketPool:
    def __init__(
        self,
        host: str,
        port: int,
        size: int = 4,
        mode: MessagingMode = MessagingMode.WORKER_COR,
    ) -> None:
        self.host = host
        self.port = port
        self.size = size
        self.conns: list[StyxSocketClient] = []
        self.index: int = 0
        self.messaging_mode: MessagingMode = mode

    def __iter__(self) -> SocketPool:
        return self

    def __next__(self) -> StyxSocketClient:
        conn = self.conns[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def create_socket_connections(self) -> None:
        for _ in range(self.size):
            client = StyxSocketClient()
            await client.create_connection(self.host, self.port)
            self.conns.append(client)

    async def flush_all(self) -> None:
        for conn in self.conns:
            await conn.flush()

    async def close(self) -> None:
        for conn in self.conns:
            await conn.close()
        self.conns = []


class NetworkingManager(BaseNetworking):
    def __init__(
        self,
        host_port: int | None,
        size: int = 4,
        mode: MessagingMode = MessagingMode.WORKER_COR,
    ) -> None:
        super().__init__(host_port, mode)
        self.aio_task_scheduler = AIOTaskScheduler(max_concurrency=1_000)
        self.pools: dict[tuple[str, int], SocketPool] = {}
        self.get_socket_lock: asyncio.Lock = asyncio.Lock()
        self.socket_pool_size: int = size

        self.peers: dict[int, tuple[str, int, int]] = {}
        self.wait_remote_key_event: dict[OperatorPartition, dict[K, asyncio.Event]] = {}
        self._flush_task: asyncio.Task | None = None

    async def _flush_loop(self) -> None:
        interval = BATCH_FLUSH_INTERVAL_MS / 1000.0
        try:
            while True:
                await asyncio.sleep(interval)
                for pool in self.pools.values():
                    await pool.flush_all()
        except asyncio.CancelledError:
            # Final flush before exit so in-flight messages are not dropped.
            for pool in self.pools.values():
                await pool.flush_all()

    async def close_all_connections(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        for pool in self.pools.values():
            await pool.close()
        self.pools = {}

    async def close_worker_connections(self, host: str, port: int) -> None:
        pool = self.pools.pop((host, port), None)
        if pool is not None:
            await pool.close()

    async def create_socket_connection(self, host: str, port: int) -> None:
        self.pools[(host, port)] = SocketPool(
            host,
            port,
            size=self.socket_pool_size,
            mode=self.messaging_mode,
        )
        await self.pools[(host, port)].create_socket_connections()
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def send_message(
        self,
        host: str,
        port: int,
        msg: tuple | bytes,
        msg_type: int,
        serializer: Serializer = Serializer.CLOUDPICKLE,
    ) -> None:
        msg = self.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        socket_conn.buffer_message(msg)

    def set_peers(self, peers: dict[int, tuple[str, int, int]]) -> None:
        self.peers = peers

    async def request_key(
        self,
        operator_name: str,
        partition: int,
        key: K,
        worker_id_old_part: tuple[int, int] | None,
    ) -> None:
        operator_partition = (operator_name, partition)
        if worker_id_old_part is None or (
            operator_partition in self.wait_remote_key_event and key in self.wait_remote_key_event[operator_partition]
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
            (
                operator_partition,
                key,
                old_partition,
                self.host_name,
                self.host_port - 1000,
            ),
            MessageType.RequestRemoteKey,
            serializer=Serializer.MSGPACK,
        )

    async def wait_for_remote_key_event(
        self,
        operator_name: str,
        partition: int,
        key: K,
    ) -> None:
        operator_partition = (operator_name, partition)
        ev = self.wait_remote_key_event[operator_partition][key]
        await ev.wait()
        # (6) cleanup to prevent unbounded growth
        del self.wait_remote_key_event[operator_partition][key]
        if not self.wait_remote_key_event[operator_partition]:
            del self.wait_remote_key_event[operator_partition]

    def key_received(self, operator_partition: OperatorPartition, key: K) -> None:
        operator_partition = tuple(operator_partition)
        self.wait_remote_key_event[operator_partition][key].set()

    async def send_message_request_response(
        self,
        host: str,
        port: int,
        msg: tuple | bytes,
        msg_type: int,
        serializer: Serializer = Serializer.CLOUDPICKLE,
    ) -> object:
        msg = self.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])

        raw = await socket_conn.send_message_rq_rs(msg)
        if raw is None:
            msg = f"No response from {host}:{port} for msg_type={msg_type}"
            raise ConnectionError(msg)

        return self.decode_message(raw)

    @staticmethod
    def encode_message(
        msg: object | bytes,
        msg_type: int,
        serializer: Serializer,
    ) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack(">B", msg_type) + struct.pack(">B", 0) + cloudpickle_serialization(msg)
        elif serializer == Serializer.MSGPACK:
            ser_msg: bytes = msgpack_serialization(msg)
            ser_id = 1
            if USE_COMPRESSION and len(ser_msg) > COMPRESS_AFTER:
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
            raise SerializerNotSupportedError
        return struct.pack(">Q", len(msg)) + msg

import asyncio
import socket
import struct
import sys
from struct import unpack
from timeit import default_timer as timer

from .exceptions import SerializerNotSupported
from .base_networking import BaseNetworking, MessagingMode
from .logging import logging
from .serialization import Serializer, cloudpickle_serialization, msgpack_serialization, pickle_serialization, \
    zstd_msgpack_serialization
from .util.aio_task_scheduler import AIOTaskScheduler


class StyxSocketClient(object):

    def __init__(self):
        self.reader = None
        self.writer = None
        self.target_host = None
        self.target_port = None
        self.lock = asyncio.Lock()
        self.n_retries = 3

    async def create_connection(self, host, port) -> bool:
        self.target_host = host
        self.target_port = port
        success = True
        i = 0
        while i < self.n_retries:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.target_host, self.target_port,
                                                                         limit=2 ** 32)
            except socket.error as e:
                logging.warning(f"{host}:{port} is not up yet, sleeping for 100 msec -> {e}")
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
        await self.lock.acquire()
        while i < self.n_retries:
            try:
                self.writer.write(message)
                await self.writer.drain()
            except (RuntimeError, ConnectionResetError, socket.error, BrokenPipeError):
                logging.warning(f"Broken connection in rq-rs, close the old ones and retry. "
                                f"Attempt {i} at {self.target_host}:{self.target_port}")
                # Broken connection, close the old one and retry
                await self.close()
                await asyncio.sleep(0.5)
                await self.create_connection(self.target_host, self.target_port)
            except Exception as e:
                logging.error(f"Uncaught exception: {e}")
                # Cannot recover from unknown exception
                i = self.n_retries
                break
            else:
                # send_message successful
                break
            i += 1
        self.lock.release()
        if i == self.n_retries:
            logging.error(f"Cannot send_message_rq_rs to worker {self.target_host}:{self.target_port}")

    async def send_message_rq_rs(self, message: bytes):
        i = 0
        resp = None
        await self.lock.acquire()
        while i < self.n_retries:
            try:
                self.writer.write(message)
                await self.writer.drain()
                (size, ) = unpack('>Q', await self.reader.readexactly(8))
                resp = await self.reader.readexactly(size)
            except (RuntimeError, ConnectionResetError, socket.error, BrokenPipeError):
                logging.warning(f"Broken connection in rq-rs, close the old ones and retry. "
                                f"Attempt {i} at {self.target_host}:{self.target_port}")
                # Broken connection, close the old one and retry
                await self.close()
                await asyncio.sleep(0.5)
                await self.create_connection(self.target_host, self.target_port)
            except Exception as e:
                logging.error(f"Uncaught exception: {e}")
                # Cannot recover from unknown exception
                i = self.n_retries
                break
            else:
                break
            i += 1
        self.lock.release()
        if i == self.n_retries:
            logging.error(f"Cannot send_message_rq_rs to worker {self.target_host}:{self.target_port}")
        return resp

    async def close(self):
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            logging.warning(f"Worker failure detected {self.target_host}:{self.target_port} "
                            f"[Connection reset by peer] Recovery will be automatically initiated.")
        except Exception as e:
            logging.error(f"Uncaught exception: {e}")


class SocketPool(object):

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

    def __init__(self,
                 host_port,
                 size: int = 4,
                 mode: MessagingMode = MessagingMode.WORKER_COR):
        super().__init__(host_port, mode)
        self.aio_task_scheduler = AIOTaskScheduler()
        self.pools: dict[tuple[str, int], SocketPool] = {}
        self.get_socket_lock = asyncio.Lock()
        self.socket_pool_size = size
        self.send_message_time = 0.0
        self.send_message_calls = 0
        self.send_message_size = 0
        self.write_buffer_len = 0

    async def close_all_connections(self):
        for pool in self.pools.values():
            await pool.close()
        self.pools = {}

    async def close_worker_connections(self, host, port):
        await self.pools[(host, port)].close()
        del self.pools[(host, port)]

    def start_networking_tasks(self):
        self.aio_task_scheduler.create_task(self.start_metrics())

    async def create_socket_connection(self, host: str, port):
        self.pools[(host, port)] = SocketPool(host, port, size=self.socket_pool_size, mode=self.messaging_mode)
        await self.pools[(host, port)].create_socket_connections()

    async def send_message(self,
                           host,
                           port,
                           msg: tuple | bytes,
                           msg_type: int,
                           serializer: Serializer = Serializer.CLOUDPICKLE):
        start_time = timer()
        msg = self.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        await socket_conn.send_message(msg)
        end_time = timer()
        self.send_message_time += end_time - start_time
        self.send_message_calls += 1
        self.send_message_size += sys.getsizeof(msg)

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: tuple | bytes,
                                            msg_type: int,
                                            serializer: Serializer = Serializer.CLOUDPICKLE):
        msg = self.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        resp = self.decode_message(await socket_conn.send_message_rq_rs(msg))
        return resp

    async def start_metrics(self, interval=30):
        while True:
            await asyncio.sleep(interval)
            await self.log_metrics()

    async def log_metrics(self):
        avg_msg_time = (
            self.send_message_time / 1000 / self.send_message_calls
            if self.send_message_calls > 0 else 0
        )
        avg_msg_size = (
            self.send_message_size // self.send_message_calls
            if self.send_message_calls > 0 else 0
        )
        logging.warning(
            f'{self}: '
            f'Send_message calls: {self.send_message_calls}, '
            f'Send_message time: {self.send_message_time / 1000} ms, '
            f'Avg message time: {avg_msg_time} ms, '
            f'Avg message size: {avg_msg_size} B.'
        )

    @staticmethod
    def encode_message(msg: object | bytes, msg_type: int, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 0) + cloudpickle_serialization(msg)
        elif serializer == Serializer.MSGPACK:
            ser_msg: bytes = msgpack_serialization(msg)
            ser_id = 1
            if len(ser_msg) > 1_048_576:
                # If it's more than 1MB compress by default
                ser_msg = zstd_msgpack_serialization(ser_msg, already_ser=True)
                ser_id = 4
            msg = struct.pack('>B', msg_type) + struct.pack('>B', ser_id) + ser_msg
        elif serializer == Serializer.PICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 2) + pickle_serialization(msg)
        elif serializer == Serializer.NONE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 3) + msg
        elif serializer == Serializer.COMPRESSED_MSGPACK:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 4) + zstd_msgpack_serialization(msg)
        else:
            logging.error(f'Serializer: {serializer} is not supported')
            raise SerializerNotSupported()
        msg = struct.pack('>Q', len(msg)) + msg
        return msg

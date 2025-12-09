import asyncio
import os
import concurrent.futures
import socket
import struct

import uvloop

from setuptools._distutils.util import strtobool

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import zstd_msgpack_serialization, msgpack_serialization
from styx.common.tcp_networking import NetworkingManager
from styx.common.types import OperatorPartition, KVPairs
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio

ENABLE_COMPRESSION: bool = bool(strtobool(os.getenv('ENABLE_COMPRESSION', "true")))

class AsyncSnapshottingProcess(object):

    def __init__(self, snapshotting_port: int, worker_id: int):
        self.snapshotting_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.snapshotting_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                 struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.snapshotting_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.snapshotting_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.snapshotting_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.snapshotting_socket.bind(('0.0.0.0', snapshotting_port))
        self.snapshotting_socket.setblocking(False)

        self.aio_task_scheduler = AIOTaskScheduler()
        self.pool: concurrent.futures.ProcessPoolExecutor = concurrent.futures.ProcessPoolExecutor(4)
        self.delta_maps: dict[OperatorPartition, KVPairs] = {}
        self.worker_id = worker_id
        self.async_snapshots = AsyncSnapshotsMinio(self.worker_id)

    def process_delta(self, delta: dict[OperatorPartition, KVPairs]):
        for operator_partition, kv_pairs in delta.items():
            if kv_pairs:
                if operator_partition not in self.delta_maps:
                    self.delta_maps[operator_partition] = kv_pairs
                else:
                    self.delta_maps[operator_partition].update(kv_pairs)

    def clear_delta_maps(self):
        for data in self.delta_maps.values():
            data.clear()

    def init_delta_maps(self, assigned_partitions: list):
        self.delta_maps = {(op_part[0], op_part[1]): {} for op_part in assigned_partitions}

    def take_snapshot(self, metadata: tuple):
        loop = asyncio.get_running_loop()
        (topic_partition_offsets,
         topic_partition_output_offsets,
         epoch_counter,
         t_counter) = metadata
        logging.warning(f"ASYNC_SN | Starting Snapshot at epoch: {epoch_counter}")
        self.async_snapshots.start_snapshotting(topic_partition_offsets,
                                                topic_partition_output_offsets,
                                                epoch_counter,
                                                t_counter)
        for operator_partition, delta_map in self.delta_maps.items():
            operator_name, partition = operator_partition
            loop.run_in_executor(self.pool,
                                 self.async_snapshots.store_snapshot,
                                 self.async_snapshots.snapshot_id,
                                 f"data/{operator_name}/{partition}/{self.async_snapshots.snapshot_id}.bin",
                                 zstd_msgpack_serialization(delta_map) if ENABLE_COMPRESSION else msgpack_serialization(delta_map)
                                 ).add_done_callback(self.async_snapshots.snapshot_completed_callback)
        self.clear_delta_maps()

    def snapshotting_controller(self, data: bytes):
        message_type: int = NetworkingManager.get_msg_type(data)
        match message_type:
            case MessageType.SnapProcDelta:
                (delta,) = NetworkingManager.decode_message(data)
                self.process_delta(delta)
            case MessageType.SnapTakeSnapshot:
                metadata = NetworkingManager.decode_message(data)
                self.take_snapshot(metadata)
            case MessageType.SnapNAssigned:
                (assigned_partitions, snapshot_id) = NetworkingManager.decode_message(data)
                self.init_delta_maps(assigned_partitions)
                self.async_snapshots.update_n_assigned_partitions(len(assigned_partitions))
                if snapshot_id != -1:
                    self.async_snapshots.set_snapshot_id(snapshot_id)
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    async def start_snapshot_tcp_service(self):
        async def request_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            queue = asyncio.Queue()

            async def reader_task():
                try:
                    while True:
                        try:
                            data = await asyncio.wait_for(reader.readexactly(8), timeout=1)
                            (size,) = struct.unpack('>Q', data)
                            message = await asyncio.wait_for(reader.readexactly(size), timeout=1)
                            await queue.put(message)
                        except asyncio.TimeoutError:
                            continue
                        except (asyncio.IncompleteReadError, ConnectionResetError):
                            logging.info("Client disconnected.")
                            return
                        except (asyncio.CancelledError, GeneratorExit):
                            return
                        except Exception:
                            logging.exception("Unhandled exception in reader_task")
                finally:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

            async def executor_task():
                try:
                    while True:
                        try:
                            message = await asyncio.wait_for(queue.get(), timeout=1)
                        except asyncio.TimeoutError:
                            continue  # loop again to check for cancellation
                        except (asyncio.CancelledError, GeneratorExit):
                            return
                        except Exception:
                            logging.exception("Unexpected exception during queue.get()")
                            continue

                        try:
                            self.snapshotting_controller(message)
                        except Exception:
                            logging.exception("Error in snapshotting_controller")
                        finally:
                            queue.task_done()
                except GeneratorExit:
                    return
                except Exception:
                    logging.exception("Unhandled exception in executor_task")

            # Launch tasks
            reader_t = asyncio.create_task(reader_task(), name="reader_task")
            executor_t = asyncio.create_task(executor_task(), name="executor_task")

            try:
                await asyncio.wait(
                    [reader_t, executor_t],
                    return_when=asyncio.FIRST_EXCEPTION
                )
            finally:
                for t in (reader_t, executor_t):
                    if not t.done():
                        t.cancel()

                await asyncio.gather(reader_t, executor_t, return_exceptions=True)
                await asyncio.sleep(0)  # Let loop finalize pending callbacks

        server = await asyncio.start_server(request_handler, sock=self.snapshotting_socket, limit=2 ** 32)
        async with server:
            await server.serve_forever()

    def start_snapshot_process(self):
        uvloop.run(self.start_snapshot_tcp_service())

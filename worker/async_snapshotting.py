import asyncio
import concurrent.futures
import socket
import struct
from timeit import default_timer as timer

import uvloop

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager
from styx.common.types import OperatorPartition, KVPairs
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio


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
        self.pool:concurrent.futures.ProcessPoolExecutor | None = None
        # snapshot_id: delta_map
        self.delta_maps: dict[OperatorPartition, KVPairs] = {}
        self.network_lock: asyncio.Lock = asyncio.Lock()
        self.worker_id = worker_id
        self.async_snapshots = AsyncSnapshotsMinio(self.worker_id)

    def process_delta(self, delta: dict[OperatorPartition, KVPairs]):
        # Add a delta to the snapshot
        self.delta_maps.update(delta)

    def clear_state(self, snapshot_id: int):
        # In case of failure (TODO probably need to get the snapshot id)
        pass

    def take_snapshot(self, metadata: tuple):
        # Take a snapshot
        loop = asyncio.get_running_loop()
        (topic_partition_offsets,
         topic_partition_output_offsets,
         epoch_counter,
         t_counter) = metadata
        self.async_snapshots.start_snapshotting(topic_partition_offsets,
                                                topic_partition_output_offsets,
                                                epoch_counter,
                                                t_counter)
        for operator_partition, delta_map in self.delta_maps.items():
            start = timer()
            operator_name, partition = operator_partition
            loop.run_in_executor(self.pool,
                                 self.async_snapshots.store_snapshot,
                                 self.async_snapshots.snapshot_id,
                                 f"data/{operator_name}/{partition}/{self.async_snapshots.snapshot_id}.bin",
                                 delta_map
                                 ).add_done_callback(self.async_snapshots.snapshot_completed_callback)
            end = timer()
            logging.warning(f"Snapshot of {operator_name}:{partition} took: {round((end - start) * 1000, 4)}")
        self.delta_maps.clear()

    async def snapshotting_controller(self, data: bytes):
        message_type: int = NetworkingManager.get_msg_type(data)
        match message_type:
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case MessageType.SnapProcDelta:
                (delta, ) = NetworkingManager.decode_message(data)
                async with self.network_lock:
                    self.process_delta(delta)
            case MessageType.SnapClearState:
                message = NetworkingManager.decode_message(data)
                async with self.network_lock:
                    self.clear_state(message)
            case MessageType.SnapTakeSnapshot:
                metadata = NetworkingManager.decode_message(data)
                async with self.network_lock:
                    self.take_snapshot(metadata)
            case MessageType.SnapNAssigned:
                (n_partitions, ) = NetworkingManager.decode_message(data)
                self.async_snapshots.update_n_assigned_partitions(n_partitions)
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    async def start_snapshot_tcp_service(self):
        async def request_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(
                        self.snapshotting_controller(message)
                    )
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()
        with concurrent.futures.ProcessPoolExecutor(1) as self.pool:
            server = await asyncio.start_server(request_handler, sock=self.snapshotting_socket, limit=2 ** 32)
            async with server:
                await server.serve_forever()

    def start_snapshot_process(self):
        uvloop.run(self.start_snapshot_tcp_service())

import asyncio
import concurrent.futures
import multiprocessing
import socket
import struct
from typing import TYPE_CHECKING

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import zstd_msgpack_serialization
from styx.common.tcp_networking import NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler
import uvloop

from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio

if TYPE_CHECKING:
    from styx.common.types import KVPairs, OperatorPartition


class AsyncSnapshottingProcess:
    def __init__(self, snapshotting_port: int, worker_id: int) -> None:
        self.snapshotting_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.snapshotting_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        self.snapshotting_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.snapshotting_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_SNDBUF,
            1024 * 1024,
        )
        self.snapshotting_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_RCVBUF,
            1024 * 1024,
        )
        self.snapshotting_socket.bind(("0.0.0.0", snapshotting_port))  # noqa: S104
        self.snapshotting_socket.setblocking(False)

        self.aio_task_scheduler = AIOTaskScheduler()
        self.pool: concurrent.futures.ProcessPoolExecutor = concurrent.futures.ProcessPoolExecutor(
            4,
            multiprocessing.get_context("spawn"),
        )
        self.delta_maps: dict[OperatorPartition, KVPairs] = {}
        self.worker_id = worker_id
        self.async_snapshots = AsyncSnapshotsMinio(self.worker_id)

    def process_delta(self, delta: dict[OperatorPartition, KVPairs]) -> None:
        for operator_partition, kv_pairs in delta.items():
            if kv_pairs:
                if operator_partition not in self.delta_maps:
                    self.delta_maps[operator_partition] = kv_pairs
                else:
                    self.delta_maps[operator_partition].update(kv_pairs)

    def clear_delta_maps(self) -> None:
        for data in self.delta_maps.values():
            data.clear()

    def init_delta_maps(self, assigned_partitions: list) -> None:
        self.delta_maps = {(op_part[0], op_part[1]): {} for op_part in assigned_partitions}

    def take_snapshot(self, metadata: tuple) -> None:
        loop = asyncio.get_running_loop()
        (
            topic_partition_offsets,
            topic_partition_output_offsets,
            epoch_counter,
            t_counter,
        ) = metadata
        logging.warning(f"ASYNC_SN | Starting Snapshot at epoch: {epoch_counter}")
        self.async_snapshots.start_snapshotting(
            topic_partition_offsets,
            topic_partition_output_offsets,
            epoch_counter,
            t_counter,
        )
        for operator_partition, delta_map in self.delta_maps.items():
            operator_name, partition = operator_partition
            data_to_store: bytes = zstd_msgpack_serialization(delta_map)
            self.async_snapshots.register_size(len(data_to_store))
            loop.run_in_executor(
                self.pool,
                self.async_snapshots.store_snapshot,
                f"data/{operator_name}/{partition}/{self.async_snapshots.snapshot_id}.bin",
                data_to_store,
            ).add_done_callback(self.async_snapshots.snapshot_completed_callback)
        self.clear_delta_maps()

    def snapshotting_controller(self, data: bytes) -> None:
        message_type: int = NetworkingManager.get_msg_type(data)
        match message_type:
            case MessageType.SnapProcDelta:
                (delta,) = NetworkingManager.decode_message(data)
                self.process_delta(delta)
            case MessageType.SnapTakeSnapshot:
                metadata = NetworkingManager.decode_message(data)
                self.take_snapshot(metadata)
            case MessageType.SnapNAssigned:
                (assigned_partitions, snapshot_id) = NetworkingManager.decode_message(
                    data,
                )
                self.init_delta_maps(assigned_partitions)
                self.async_snapshots.update_n_assigned_partitions(
                    len(assigned_partitions),
                )
                if snapshot_id != -1:
                    self.async_snapshots.set_snapshot_id(snapshot_id)
            case _:
                logging.error(
                    f"Worker Service: Non supported command message type: {message_type}",
                )

    async def start_snapshot_tcp_service(self) -> None:
        server = await asyncio.start_server(
            self._snapshot_request_handler,
            sock=self.snapshotting_socket,
            limit=2**32,
        )
        async with server:
            await server.serve_forever()

    async def _snapshot_request_handler(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        reader_t = asyncio.create_task(
            self._snapshot_reader_task(reader, writer, queue),
            name="snapshot_reader_task",
        )
        executor_t = asyncio.create_task(
            self._snapshot_executor_task(queue),
            name="snapshot_executor_task",
        )

        try:
            await asyncio.wait(
                [reader_t, executor_t],
                return_when=asyncio.FIRST_EXCEPTION,
            )
        finally:
            await self._cancel_and_join(reader_t, executor_t)
            # Let loop finalize pending callbacks (mirrors your original intent)
            await asyncio.sleep(0)

    async def _snapshot_reader_task(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, queue: asyncio.Queue[bytes | None]
    ) -> None:
        try:
            while True:
                msg = await self._read_framed_message(reader)
                if msg is None:
                    logging.info("Client disconnected.")
                    return
                await queue.put(msg)

        except asyncio.CancelledError, GeneratorExit:
            return
        except Exception:
            logging.exception("Unhandled exception in snapshot reader task")
        finally:
            # Tell executor to stop
            try:
                await queue.put(None)
            except Exception:
                logging.warning("executor did not stop gracefully")
            # Close writer
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                logging.warning("start_snapshot_tcp_service did not stop gracefully")

    async def _snapshot_executor_task(self, queue: asyncio.Queue[bytes | None]) -> None:
        try:
            while True:
                message = await self._queue_get_with_timeout(queue, timeout_s=1)
                if message is None:
                    return  # sentinel or timeout (None is sentinel; timeout handled below)
                try:
                    self.snapshotting_controller(message)
                except Exception:
                    logging.exception("Error in snapshotting_controller")
                finally:
                    queue.task_done()

        except asyncio.CancelledError, GeneratorExit:
            return
        except Exception:
            logging.exception("Unhandled exception in snapshot executor task")

    async def _read_framed_message(self, reader: asyncio.StreamReader) -> bytes | None:
        """
        Reads a single framed message:
          - 8-byte big-endian unsigned length
          - payload of that length
        Returns None on disconnect/reset.
        Retries on timeout (caller loops).
        """
        while True:
            try:
                header = await asyncio.wait_for(reader.readexactly(8), timeout=1)
                (size,) = struct.unpack(">Q", header)
                return await asyncio.wait_for(reader.readexactly(size), timeout=1)
            except TimeoutError:
                continue
            except asyncio.IncompleteReadError, ConnectionResetError:
                return None

    async def _queue_get_with_timeout(
        self,
        queue: asyncio.Queue[bytes | None],
        timeout_s: float,
    ) -> bytes | None:
        """
        Returns:
          - the item from the queue
          - None if timeout occurs (caller decides what to do)
        """
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout_s)
        except TimeoutError:
            return None

    async def _cancel_and_join(self, *tasks: asyncio.Task) -> None:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def start_snapshot_process(self) -> None:
        uvloop.run(self.start_snapshot_tcp_service())

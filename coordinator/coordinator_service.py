#!/usr/bin/env python3

import asyncio
from asyncio import StreamReader, StreamWriter
from collections.abc import Awaitable, Callable
import concurrent.futures
import contextlib
from enum import Enum, auto
import os
import socket
import struct
import time
from timeit import default_timer as timer
from typing import TYPE_CHECKING

from aria_sync_metadata import AriaSyncMetadata
from coordinator_metadata import Coordinator
from minio import Minio
import minio.error
from prometheus_client import Gauge, start_http_server
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.tcp_networking import MessagingMode, NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler
import uvloop

from coordinator.migration_metadata import MigrationMetadata

if TYPE_CHECKING:
    from styx.common.stateflow_graph import StateflowGraph

    from coordinator.worker_pool import Worker

SERVER_PORT = 8888
PROTOCOL_PORT = 8889

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY: str = os.environ["MINIO_ROOT_PASSWORD"]

PROTOCOL = Protocols.Aria

SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")
SNAPSHOT_FREQUENCY_SEC = int(os.getenv("SNAPSHOT_FREQUENCY_SEC", "30"))
HEARTBEAT_CHECK_INTERVAL: int = int(
    os.getenv("HEARTBEAT_CHECK_INTERVAL", "1000"),
)  # 1000ms

CoordHandler = Callable[[StreamWriter, bytes, concurrent.futures.ProcessPoolExecutor], Awaitable[None]]


class RecoveryState(Enum):
    IDLE = auto()
    RECOVERING = auto()


class CoordinatorService:
    def __init__(self) -> None:
        self.networking = NetworkingManager(SERVER_PORT)
        self.protocol_networking = NetworkingManager(
            PROTOCOL_PORT,
            size=4,
            mode=MessagingMode.PROTOCOL_PROTOCOL,
        )
        self.minio_client: Minio = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        self.coordinator = Coordinator(self.networking, self.minio_client)
        self.aio_task_scheduler = AIOTaskScheduler()
        self.aio_task_scheduler_coord = AIOTaskScheduler()

        self.puller_task: asyncio.Task | None = None

        self.coor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coor_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        self.coor_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.coor_socket.bind(("0.0.0.0", SERVER_PORT))  # noqa: S104
        self.coor_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        self.protocol_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.protocol_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_SNDBUF,
            1024 * 1024,
        )
        self.protocol_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_RCVBUF,
            1024 * 1024,
        )
        self.protocol_socket.bind(("0.0.0.0", SERVER_PORT + 1))  # noqa: S104
        self.protocol_socket.setblocking(False)

        self.aria_metadata: AriaSyncMetadata | None = None
        self.migration_metadata: MigrationMetadata | None = None
        self.workers_that_re_registered: list[Worker] = []
        self.recovery_lock: asyncio.Lock = asyncio.Lock()
        self.recovery_state: RecoveryState = RecoveryState.IDLE

        self.metrics_server = start_http_server(8000)
        self.cpu_usage_gauge = Gauge(
            "worker_cpu_usage_percent",
            "CPU usage percentage",
            ["instance"],
        )
        self.memory_usage_gauge = Gauge(
            "worker_memory_usage_mb",
            "Memory usage in MB",
            ["instance"],
        )
        self.network_rx_gauge = Gauge(
            "worker_network_rx_kb",
            "Network received KB",
            ["instance"],
        )
        self.network_tx_gauge = Gauge(
            "worker_network_tx_kb",
            "Network transmitted KB",
            ["instance"],
        )
        self.epoch_latency_gauge = Gauge(
            "worker_epoch_latency_ms",
            "Epoch Latency (ms)",
            ["instance"],
        )
        self.epoch_throughput_gauge = Gauge(
            "worker_epoch_throughput_tps",
            "Epoch Throughput (transactions per second)",
            ["instance"],
        )
        self.epoch_abort_gauge = Gauge(
            "worker_abort_percent",
            "Epoch Concurrency Abort percentage",
            ["instance"],
        )
        self.latency_breakdown_gauge = Gauge(
            "latency_breakdown",
            "Time Spent in different phases within the transactional protocol",
            ["instance", "component"],
        )
        self.snapshotting_gauge = Gauge(
            "worker_total_snapshotting_time_ms",
            "Snapshotting time (ms)",
            ["instance"],
        )
        self.heartbeat_gauge = Gauge(
            "time_since_last_heartbeat",
            "Time Since Last Heartbeat",
            ["instance"],
        )

        self.migration_in_progress: bool = False

        self.networking_locks: dict[MessageType, asyncio.Lock] = {
            MessageType.SendExecutionGraph: asyncio.Lock(),
            MessageType.MigrationRepartitioningDone: asyncio.Lock(),
            MessageType.MigrationDone: asyncio.Lock(),
            MessageType.MigrationInitDone: asyncio.Lock(),
            MessageType.RegisterWorker: asyncio.Lock(),
            MessageType.SnapID: asyncio.Lock(),
            MessageType.Heartbeat: asyncio.Lock(),
            MessageType.AriaProcessingDone: asyncio.Lock(),
            MessageType.AriaCommit: asyncio.Lock(),
            MessageType.AriaFallbackStart: asyncio.Lock(),
            MessageType.AriaFallbackDone: asyncio.Lock(),
            MessageType.SyncCleanup: asyncio.Lock(),
            MessageType.DeterministicReordering: asyncio.Lock(),
            MessageType.ReadyAfterRecovery: asyncio.Lock(),
        }

        self.snapshotting_task: asyncio.Task | None = None

        self._protocol_controller_handlers_map: dict[MessageType, Callable[[bytes], Awaitable[None]]] = {
            MessageType.AriaProcessingDone: self._handle_aria_processing_done,
            MessageType.AriaCommit: self._handle_aria_commit,
            MessageType.AriaFallbackStart: self._handle_aria_fallback_sync,
            MessageType.AriaFallbackDone: self._handle_aria_fallback_sync,
            MessageType.SyncCleanup: self._handle_sync_cleanup,
            MessageType.DeterministicReordering: self._handle_deterministic_reordering,
            MessageType.MigrationDone: self._handle_migration_done,
        }

        self._coordinator_handlers_map: dict[MessageType, CoordHandler] = {
            MessageType.SendExecutionGraph: self._handle_send_execution_graph,
            MessageType.MigrationRepartitioningDone: self._handle_migration_repartitioning_done,
            MessageType.MigrationInitDone: self._handle_migration_init_done,
            MessageType.RegisterWorker: self._handle_register_worker,
            MessageType.SnapID: self._handle_snap_id,
            MessageType.Heartbeat: self._handle_heartbeat,
            MessageType.ReadyAfterRecovery: self._handle_ready_after_recovery,
            MessageType.InitDataComplete: self._handle_init_data_complete,
        }

    async def coordinator_controller(
        self,
        transport: StreamWriter,
        data: bytes,
        pool: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt: MessageType = self.networking.get_msg_type(data)
        handler = self._coordinator_handlers_map.get(mt)
        if handler is None:
            logging.error(f"COORDINATOR SERVER: Non supported message type: {mt}")
            return
        await handler(transport, data, pool)

    # ------------------------
    # Handlers
    # ------------------------
    async def _handle_send_execution_graph(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.SendExecutionGraph
        async with self.networking_locks[mt]:
            (graph,) = self.networking.decode_message(data)

            if self.coordinator.graph_submitted and not self.migration_in_progress:
                await self._start_migration(graph)
            elif not self.coordinator.graph_submitted:
                await self._submit_initial_graph(graph)
            else:
                logging.warning("Another migration request is currently in progress!")
                return

            logging.info("Submitted Stateflow Graph to Workers")

    async def _start_migration(self, graph: StateflowGraph) -> None:
        # Gracefully stop the transactional protocol in the next epoch
        self.migration_in_progress = True
        await self.stop_snapshotting()

        if self.aria_metadata is not None:
            self.aria_metadata.stop_in_next_epoch()

        logging.warning(f"MIGRATION | START {graph}")
        await self.coordinator.update_stateflow_graph(graph)

        n_workers = len(self.coordinator.worker_pool.get_participating_workers())
        self.migration_metadata = MigrationMetadata(n_workers)

    async def _submit_initial_graph(self, graph: StateflowGraph) -> None:
        await self.coordinator.submit_stateflow_graph(graph)
        n_workers = len(self.coordinator.worker_pool.get_participating_workers())
        self.aria_metadata = AriaSyncMetadata(n_workers)

    async def _handle_migration_repartitioning_done(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.MigrationRepartitioningDone
        logging.warning("Migration repartitioning done received!")

        async with self.networking_locks[mt]:
            epoch_counter, t_counter, input_offsets, output_offsets = self.networking.decode_message(data)

            sync_complete: bool = await self.migration_metadata.repartitioning_done(
                epoch_counter,
                t_counter,
                input_offsets,
                output_offsets,
            )

            logging.warning(f"Migration repartitioning is complete: {sync_complete}")

            if not sync_complete:
                return

            await self.finalize_migration_repartition()
            await self.migration_metadata.cleanup(mt)

    async def _handle_migration_init_done(
        self,
        _: StreamWriter,
        __: bytes,
        ___: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.MigrationInitDone
        async with self.networking_locks[mt]:
            sync_complete: bool = await self.migration_metadata.set_empty_sync_done(mt)
            logging.warning(f"MIGRATION | MigrationInitDone | {self.migration_metadata.sync_sum}")

            if not sync_complete:
                return

            logging.warning("MIGRATION | MigrationInitDone | sync_complete")
            n_workers = len(self.coordinator.worker_pool.get_participating_workers())

            self.aria_metadata = AriaSyncMetadata(n_workers)
            await self.protocol_networking.close_all_connections()
            await self.finalize_migration()
            await self.migration_metadata.cleanup(mt)

    async def _handle_register_worker(
        self,
        transport: StreamWriter,
        data: bytes,
        _: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.RegisterWorker
        async with self.networking_locks[mt]:
            worker_ip, worker_port, protocol_port = self.networking.decode_message(data)

            worker_id, init_recovery = self.coordinator.register_worker(
                worker_ip,
                worker_port,
                protocol_port,
            )

            transport.write(
                self.networking.encode_message(
                    msg=worker_id,
                    msg_type=MessageType.RegisterWorker,
                    serializer=Serializer.MSGPACK,
                ),
            )

            if init_recovery:
                await self._track_reregistered_worker(worker_id)

            logging.warning(
                f"Worker registered {worker_ip}:{worker_port} with id {worker_id}",
            )

    async def _track_reregistered_worker(self, worker_id: int) -> None:
        async with self.recovery_lock:
            self.workers_that_re_registered.append(
                self.coordinator.get_worker_with_id(worker_id),
            )

    async def _handle_snap_id(
        self,
        _: StreamWriter,
        data: bytes,
        pool: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.SnapID
        async with self.networking_locks[mt]:
            (
                worker_id,
                snapshot_id,
                start,
                end,
                partial_input_offsets,
                partial_output_offsets,
                epoch_counter,
                t_counter,
            ) = self.networking.decode_message(data)

            snapshot_time = end - start
            self.snapshotting_gauge.labels(instance=worker_id).set(snapshot_time)

            logging.warning(
                f"Worker: {worker_id} | "
                f"@Epoch: {epoch_counter} | "
                f"Completed snapshot: {snapshot_id} | "
                f"started at: {start} | "
                f"ended at: {end} | "
                f"took: {snapshot_time}ms",
            )

            self.coordinator.register_snapshot(
                worker_id,
                snapshot_id,
                partial_input_offsets,
                partial_output_offsets,
                epoch_counter,
                t_counter,
                pool,
            )

    async def _handle_heartbeat(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.Heartbeat
        async with self.networking_locks[mt]:
            worker_id, cpu_perc, mem_util, rx_net, tx_net = self.networking.decode_message(data)

            self.cpu_usage_gauge.labels(instance=worker_id).set(cpu_perc)  # %
            self.memory_usage_gauge.labels(instance=worker_id).set(mem_util)  # MB
            self.network_rx_gauge.labels(instance=worker_id).set(rx_net)  # KB
            self.network_tx_gauge.labels(instance=worker_id).set(tx_net)  # KB

            heartbeat_rcv_time = timer()
            logging.info(
                f"Heartbeat received from: {worker_id} at time: {heartbeat_rcv_time}",
            )

            self.coordinator.register_worker_heartbeat(worker_id, heartbeat_rcv_time)

    async def _handle_ready_after_recovery(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.ReadyAfterRecovery
        async with self.networking_locks[mt]:
            (worker_id,) = self.networking.decode_message(data)
            self.coordinator.worker_is_ready_after_recovery(worker_id)
            logging.info(f"ready after recovery received from: {worker_id}")

    async def _handle_init_data_complete(
        self,
        _: StreamWriter,
        __: bytes,
        ___: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        self.coordinator.init_data_complete()
        await asyncio.sleep(0)

    async def protocol_controller(self, data: bytes) -> None:
        mt: MessageType = self.protocol_networking.get_msg_type(data)
        handler = self._protocol_controller_handlers_map.get(mt)
        if handler is None:
            logging.error(
                f"COORDINATOR PROTOCOL SERVER: Non supported message type: {mt}",
            )
            return
        await handler(data)

    # ------------------------
    # Handlers
    # ------------------------
    async def _handle_aria_processing_done(self, data: bytes) -> None:
        mt = MessageType.AriaProcessingDone
        async with self.networking_locks[mt]:
            if not self.aria_metadata.sent_proceed_msg:
                self.aria_metadata.sent_proceed_msg = True
                await self.worker_wants_to_proceed()

            worker_id, remote_logic_aborts = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_aria_processing_done(
                worker_id,
                remote_logic_aborts,
            )
            if not sync_complete:
                return

            await self.finalize_worker_sync(
                mt,
                (self.aria_metadata.logic_aborts_everywhere,),
                Serializer.PICKLE,
            )
            self.aria_metadata.cleanup()

    async def _handle_aria_commit(self, data: bytes) -> None:
        mt = MessageType.AriaCommit
        async with self.networking_locks[mt]:
            worker_id, aborted, remote_t_counter, processed_seq_size = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_aria_commit_done(
                worker_id,
                aborted,
                remote_t_counter,
                processed_seq_size,
            )
            if not sync_complete:
                return

            await self.finalize_worker_sync(
                mt,
                (
                    self.aria_metadata.concurrency_aborts_everywhere,
                    self.aria_metadata.processed_seq_size,
                    self.aria_metadata.max_t_counter,
                    self.aria_metadata.take_snapshot,
                ),
                Serializer.PICKLE,
            )
            self.aria_metadata.cleanup(take_snapshot=True)

    async def _handle_aria_fallback_sync(self, data: bytes) -> None:
        # Handles both AriaFallbackStart and AriaFallbackDone
        mt: MessageType = self.protocol_networking.get_msg_type(data)
        async with self.networking_locks[mt]:
            (worker_id,) = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_empty_sync_done(worker_id)
            if not sync_complete:
                return

            await self.finalize_worker_sync(
                mt,
                b"",
                Serializer.NONE,
            )
            self.aria_metadata.cleanup()

    async def _handle_sync_cleanup(self, data: bytes) -> None:
        mt = MessageType.SyncCleanup
        async with self.networking_locks[mt]:
            (
                worker_id,
                epoch_throughput,
                epoch_latency,
                local_abort_rate,
                wal_time,
                func_time,
                chain_ack_time,
                sync_time,
                conflict_res_time,
                commit_time,
                fallback_time,
                snap_time,
            ) = self.protocol_networking.decode_message(data)

            self._record_epoch_metrics(
                worker_id=worker_id,
                epoch_throughput=epoch_throughput,
                epoch_latency=epoch_latency,
                local_abort_rate=local_abort_rate,
                wal_time=wal_time,
                func_time=func_time,
                chain_ack_time=chain_ack_time,
                sync_time=sync_time,
                conflict_res_time=conflict_res_time,
                commit_time=commit_time,
                fallback_time=fallback_time,
                snap_time=snap_time,
            )

            sync_complete: bool = self.aria_metadata.set_empty_sync_done(worker_id)
            if not sync_complete:
                return

            await self.finalize_worker_sync(
                mt,
                (self.aria_metadata.stop_next_epoch,),
                Serializer.MSGPACK,
            )
            self.aria_metadata.cleanup(epoch_end=True)

    def _record_epoch_metrics(
        self,
        *,
        worker_id: str,
        epoch_throughput: float,
        epoch_latency: float,
        local_abort_rate: float,
        wal_time: float,
        func_time: float,
        chain_ack_time: float,
        sync_time: float,
        conflict_res_time: float,
        commit_time: float,
        fallback_time: float,
        snap_time: float,
    ) -> None:
        self.epoch_throughput_gauge.labels(instance=worker_id).set(epoch_throughput)
        self.epoch_latency_gauge.labels(instance=worker_id).set(epoch_latency)
        self.epoch_abort_gauge.labels(instance=worker_id).set(local_abort_rate)

        self.latency_breakdown_gauge.labels(instance=worker_id, component="WAL").set(wal_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="1st Run").set(func_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Chain Acks").set(chain_ack_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="SYNC").set(sync_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Conflict Resolution").set(conflict_res_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Commit time").set(commit_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Fallback").set(fallback_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Async Snapshot").set(snap_time)

    async def _handle_deterministic_reordering(self, data: bytes) -> None:
        mt = MessageType.DeterministicReordering
        async with self.networking_locks[mt]:
            (
                worker_id,
                remote_read_reservation,
                remote_write_set,
                remote_read_set,
            ) = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_deterministic_reordering_done(
                worker_id,
                remote_read_reservation,
                remote_write_set,
                remote_read_set,
            )
            if not sync_complete:
                return

            await self.finalize_worker_sync(
                mt,
                (
                    self.aria_metadata.global_read_reservations,
                    self.aria_metadata.global_write_set,
                    self.aria_metadata.global_read_set,
                ),
                Serializer.PICKLE,
            )
            self.aria_metadata.cleanup()

    async def _handle_migration_done(self, _: bytes) -> None:
        mt = MessageType.MigrationDone
        async with self.networking_locks[mt]:
            sync_complete: bool = await self.migration_metadata.set_empty_sync_done(mt)
            logging.warning(
                f"MIGRATION | MigrationDone | {self.migration_metadata.sync_sum}",
            )
            if not sync_complete:
                return

            logging.warning(
                f"MIGRATION_FINISHED at time: {time.time_ns() // 1_000_000}",
            )
            await self.migration_metadata.cleanup(mt)
            self.migration_in_progress = False

            logging.warning("Restarting the snapshotting mechanism")
            self.snapshotting_task = asyncio.create_task(self.send_snapshot_marker())

    async def start_puller(self) -> None:
        async def request_handler(reader: StreamReader, writer: StreamWriter) -> None:
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack(">Q", data)
                    self.aio_task_scheduler.create_task(
                        self.protocol_controller(await reader.readexactly(size)),
                    )
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(
            request_handler,
            sock=self.protocol_socket,
            limit=2**32,
        )
        async with server:
            await server.serve_forever()

    async def tcp_service(self) -> None:
        self.puller_task = asyncio.create_task(self.start_puller())
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        with concurrent.futures.ProcessPoolExecutor(1) as pool:

            async def request_handler(
                reader: StreamReader,
                writer: StreamWriter,
            ) -> None:
                try:
                    while True:
                        data = await reader.readexactly(8)
                        (size,) = struct.unpack(">Q", data)
                        message = await reader.readexactly(size)
                        self.aio_task_scheduler_coord.create_task(
                            self.coordinator_controller(writer, message, pool),
                        )
                except asyncio.IncompleteReadError as e:
                    logging.info(f"Client disconnected unexpectedly: {e}")
                except asyncio.CancelledError:
                    pass
                finally:
                    logging.info("Closing the connection")
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(
                request_handler,
                sock=self.coor_socket,
                limit=2**32,
            )
            async with server:
                await server.serve_forever()

    async def finalize_migration_repartition(self) -> None:
        logging.warning("Sending MigrationRepartitioningDone to all workers")
        async with asyncio.TaskGroup() as tg:
            for worker in self.coordinator.worker_pool.get_participating_workers():
                tg.create_task(
                    self.protocol_networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=(
                            self.migration_metadata.epoch_counter,
                            self.migration_metadata.t_counter,
                            self.migration_metadata.input_offsets,
                            self.migration_metadata.output_offsets,
                        ),
                        msg_type=MessageType.MigrationRepartitioningDone,
                        serializer=Serializer.MSGPACK,
                    ),
                )

    async def finalize_migration(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for worker in self.coordinator.worker_pool.get_participating_workers():
                logging.warning(f"Sending MigrationDone to : {worker}")
                tg.create_task(
                    self.protocol_networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=b"",
                        msg_type=MessageType.MigrationDone,
                        serializer=Serializer.NONE,
                    ),
                )

    async def finalize_worker_sync(
        self,
        msg_type: MessageType,
        message: tuple | bytes,
        serializer: Serializer = Serializer.MSGPACK,
    ) -> None:
        async with asyncio.TaskGroup() as tg:
            for worker in self.coordinator.worker_pool.get_participating_workers():
                tg.create_task(
                    self.protocol_networking.send_message(
                        worker.worker_ip,
                        worker.protocol_port,
                        msg=message,
                        msg_type=msg_type,
                        serializer=serializer,
                    ),
                )

    async def worker_wants_to_proceed(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for worker in self.coordinator.worker_pool.get_participating_workers():
                tg.create_task(
                    self.protocol_networking.send_message(
                        worker.worker_ip,
                        worker.protocol_port,
                        msg=b"",
                        msg_type=MessageType.RemoteWantsToProceed,
                        serializer=Serializer.NONE,
                    ),
                )

    async def _reset_after_recovery(self) -> None:
        """
        Reset all coordinator-side protocol metadata after a successful recovery.
        This is the core of the 'robust recovery state machine'.
        """
        participating_workers = self.coordinator.worker_pool.get_participating_workers()
        n_workers = len(participating_workers)

        logging.warning("Resetting protocol metadata after recovery")

        # 1) Reset Aria metadata (only if a graph is submitted)
        if self.coordinator.graph_submitted:
            self.aria_metadata = AriaSyncMetadata(n_workers)
        else:
            self.aria_metadata = None

        # 2) Reset migration metadata
        self.migration_metadata = MigrationMetadata(n_workers)
        self.migration_in_progress = False

        # 3) Reset snapshot completion metadata
        self.coordinator.completed_input_offsets.clear()
        self.coordinator.completed_out_offsets.clear()
        self.coordinator.completed_epoch_counter = 0
        self.coordinator.completed_t_counter = 0
        self.coordinator.prev_completed_snapshot_id = -1
        # All workers will effectively need to rebuild their snapshot IDs
        self.coordinator.worker_snapshot_ids = {worker.worker_id: -1 for worker in participating_workers}

        # 4) Reset worker heartbeat gauges and baseline times
        for worker in participating_workers:
            # Next heartbeat from worker defines fresh baseline
            worker.previous_heartbeat = 1_000_000.0
            self.heartbeat_gauge.labels(instance=worker.worker_id).set(0)

        # 5) Reset epoch-related metrics
        for worker in participating_workers:
            wid = worker.worker_id
            self.epoch_throughput_gauge.labels(instance=wid).set(0)
            self.epoch_latency_gauge.labels(instance=wid).set(0)
            self.epoch_abort_gauge.labels(instance=wid).set(0)
        # Reset latency breakdown (all labels)
        self.latency_breakdown_gauge._metrics.clear()  # noqa: SLF001

        logging.warning("Protocol metadata reset complete")

    async def _perform_recovery(self, workers_to_remove: set[Worker]) -> None:
        """
        Full recovery state machine:
        - close dead worker connections
        - start recovery
        - wait cluster healthy
        - reset protocol metadata
        - close protocol connections
        - notify workers that everyone is healthy
        """
        if not workers_to_remove:
            return

        logging.warning(f"Starting recovery process for workers: {workers_to_remove}")

        # 1) Clean up dead worker channels and buffered tasks
        logging.warning(f"Closing connections to dead workers: {workers_to_remove}")
        for worker in workers_to_remove:
            await self.networking.close_worker_connections(
                worker.worker_ip,
                worker.worker_port,
            )
            await self.protocol_networking.close_worker_connections(
                worker.worker_ip,
                worker.protocol_port,
            )
        await self.aio_task_scheduler.close()
        self.aio_task_scheduler = AIOTaskScheduler()

        # 2) Start recovery
        logging.warning(
            "Starting recovery process (reassign operators, send InitRecovery)",
        )
        await self.coordinator.start_recovery_process(workers_to_remove)

        # 3) Wait for the cluster to become healthy
        logging.warning("Waiting on the cluster to become healthy")
        await self.coordinator.wait_cluster_healthy()

        # 4) Reset protocol metadata (& snapshot/metrics state)
        logging.warning("Cleaning up protocol after everyone is healthy")
        await self._reset_after_recovery()

        # 5) Close all protocol connections (workers will reconnect clean)
        await self.protocol_networking.close_all_connections()

        # 6) Notify Cluster that everyone is ready
        logging.warning("Notify workers that cluster is healthy")
        await self.coordinator.notify_cluster_healthy()

        logging.warning("Recovery process completed")

    async def heartbeat_monitor_coroutine(self) -> None:
        interval_time = HEARTBEAT_CHECK_INTERVAL / 1000
        while True:
            await asyncio.sleep(interval_time)
            heartbeat_check_time = timer()
            workers_to_remove, heartbeats_per_worker = self.coordinator.check_heartbeats(heartbeat_check_time)
            for (
                worker_id,
                time_since_last_heartbeat_ms,
            ) in heartbeats_per_worker.items():
                self.heartbeat_gauge.labels(instance=worker_id).set(
                    time_since_last_heartbeat_ms,
                )

            # Add workers that re-registered (same IP/ports) to the failed set
            if (workers_to_remove or self.workers_that_re_registered) and self.recovery_state == RecoveryState.IDLE:
                async with self.recovery_lock:
                    if self.recovery_state != RecoveryState.IDLE:
                        # Another recovery started while we were waiting for the lock
                        continue
                    self.recovery_state = RecoveryState.RECOVERING
                    try:
                        # Merge "dead" workers and workers that re-registered with init_recovery=True
                        re_registered_set = set(self.workers_that_re_registered)
                        workers_to_remove.update(re_registered_set)
                        self.workers_that_re_registered = []

                        await self._perform_recovery(workers_to_remove)
                    except Exception as e:
                        logging.error(f"Error during recovery: {e}")
                    finally:
                        self.recovery_state = RecoveryState.IDLE

    async def send_snapshot_marker(self) -> None:
        while True:
            await asyncio.sleep(SNAPSHOT_FREQUENCY_SEC)
            if self.aria_metadata is not None:
                self.aria_metadata.take_snapshot_at_next_epoch()

    async def stop_snapshotting(self) -> None:
        if self.snapshotting_task:
            self.snapshotting_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.snapshotting_task
            self.snapshotting_task = None

    def init_snapshot_minio_bucket(self) -> None:
        try:
            if not self.minio_client.bucket_exists(SNAPSHOT_BUCKET_NAME):
                self.minio_client.make_bucket(SNAPSHOT_BUCKET_NAME)
        except minio.error.S3Error:
            # BUCKET ALREADY EXISTS
            pass

    async def main(self) -> None:
        self.init_snapshot_minio_bucket()
        self.aio_task_scheduler_coord.create_task(self.heartbeat_monitor_coroutine())
        self.snapshotting_task = asyncio.create_task(self.send_snapshot_marker())
        await self.tcp_service()


if __name__ == "__main__":
    coordinator_service = CoordinatorService()
    uvloop.run(coordinator_service.main())

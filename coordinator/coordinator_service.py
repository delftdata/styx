import asyncio
import os
import socket
import concurrent.futures
import struct
from asyncio import StreamReader, StreamWriter

from timeit import default_timer as timer

import uvloop
from minio import Minio
import minio.error
from prometheus_client import start_http_server, Gauge

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from coordinator.worker_pool import Worker
from coordinator_metadata import Coordinator
from aria_sync_metadata import AriaSyncMetadata

SERVER_PORT = 8888
PROTOCOL_PORT = 8889

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']

PROTOCOL = Protocols.Aria

SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")
SNAPSHOT_FREQUENCY_SEC = int(os.getenv('SNAPSHOT_FREQUENCY_SEC', 10))
SNAPSHOT_COMPACTION_INTERVAL_SEC = int(os.getenv('SNAPSHOT_COMPACTION_INTERVAL_SEC', 10))
HEARTBEAT_CHECK_INTERVAL: int = int(os.getenv('HEARTBEAT_CHECK_INTERVAL', 1000))  # 1000ms


class CoordinatorService(object):

    def __init__(self):
        self.networking = NetworkingManager(SERVER_PORT)
        self.protocol_networking = NetworkingManager(PROTOCOL_PORT, size=4, mode=MessagingMode.PROTOCOL_PROTOCOL)
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.coordinator = Coordinator(self.networking,self.minio_client)
        self.aio_task_scheduler = AIOTaskScheduler()

        self.puller_task: asyncio.Task = ...

        self.coor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                 struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.coor_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.coor_socket.bind(('0.0.0.0', SERVER_PORT))
        self.coor_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                 struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.protocol_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.protocol_socket.bind(('0.0.0.0', SERVER_PORT + 1))
        self.protocol_socket.setblocking(False)

        self.aria_metadata: AriaSyncMetadata = ...
        self.workers_that_re_registered: list[Worker] = []
        self.recovery_lock: asyncio.Lock = asyncio.Lock()

    # Refactoring candidate
    async def coordinator_controller(self, transport, data, pool: concurrent.futures.ProcessPoolExecutor):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            case MessageType.SendExecutionGraph:
                message = self.networking.decode_message(data)
                # Received execution graph from a styx client
                await self.coordinator.submit_stateflow_graph(message[0])
                logging.info("Submitted Stateflow Graph to Workers")
                self.aria_metadata = AriaSyncMetadata(len(self.coordinator.worker_pool.get_participating_workers()))
            case MessageType.RegisterWorker:  # REGISTER_WORKER
                worker_ip, worker_port, protocol_port = self.networking.decode_message(data)
                # A worker registered to the coordinator
                worker_id, init_recovery = self.coordinator.register_worker(worker_ip, worker_port, protocol_port)
                transport.write(self.networking.encode_message(msg=worker_id,
                                                            msg_type=MessageType.RegisterWorker,
                                                            serializer=Serializer.MSGPACK))
                if init_recovery:
                    async with self.recovery_lock:
                        self.workers_that_re_registered.append(self.coordinator.get_worker_with_id(worker_id))
                logging.warning(f"Worker registered {worker_ip}:{worker_port} with id {worker_id}")
            case MessageType.SnapID:
                # Get snap id from worker
                (worker_id, snapshot_id, start, end,
                 partial_input_offsets, partial_output_offsets,
                 epoch_counter, t_counter) = self.networking.decode_message(data)
                snapshot_time = end - start
                self.snapshotting_gauge.labels(instance=worker_id).set(snapshot_time)
                logging.warning(f'Worker: {worker_id} | '
                                f'Completed snapshot: {snapshot_id} | '
                                f'started at: {start} | '
                                f'ended at: {end} | '
                                f'took: {snapshot_time}ms')
                self.coordinator.register_snapshot(worker_id, snapshot_id,
                                                   partial_input_offsets, partial_output_offsets,
                                                   epoch_counter, t_counter,
                                                   pool)
            case MessageType.Heartbeat:
                # HEARTBEATS
                (worker_id, cpu_perc, mem_util, rx_net, tx_net) = self.networking.decode_message(data)
                self.cpu_usage_gauge.labels(instance=worker_id).set(cpu_perc) # %
                self.memory_usage_gauge.labels(instance=worker_id).set(mem_util) # MB
                self.network_rx_gauge.labels(instance=worker_id).set(rx_net) # KB
                self.network_tx_gauge.labels(instance=worker_id).set(tx_net) # KB
                heartbeat_rcv_time = timer()
                logging.info(f'Heartbeat received from: {worker_id} at time: {heartbeat_rcv_time}')
                self.coordinator.register_worker_heartbeat(worker_id, heartbeat_rcv_time)
            case MessageType.ReadyAfterRecovery:
                # report ready after recovery
                (worker_id,) = self.networking.decode_message(data)
                self.coordinator.worker_is_ready_after_recovery(worker_id)
                logging.info(f'ready after recovery received from: {worker_id}')
            case _:
                # Any other message type
                logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")

    async def protocol_controller(self, data):
        message_type: int = self.protocol_networking.get_msg_type(data)
        match message_type:
            case MessageType.AriaProcessingDone:
                if not self.aria_metadata.sent_proceed_msg:
                    self.aria_metadata.sent_proceed_msg = True
                    await self.worker_wants_to_proceed()
                message = self.protocol_networking.decode_message(data)
                if message == b'':
                    remote_logic_aborts = set()
                else:
                    remote_logic_aborts = message[0]
                sync_complete: bool = await self.aria_metadata.set_aria_processing_done(remote_logic_aborts)
                if sync_complete:
                    await self.finalize_worker_sync(MessageType(message_type),
                                                    (self.aria_metadata.logic_aborts_everywhere,),
                                                    Serializer.PICKLE)
                    await self.aria_metadata.cleanup()
            case MessageType.AriaCommit:
                message = self.protocol_networking.decode_message(data)
                aborted, remote_t_counter, processed_seq_size = message
                sync_complete: bool = await self.aria_metadata.set_aria_commit_done(aborted,
                                                                                    remote_t_counter,
                                                                                    processed_seq_size)
                if sync_complete:
                    await self.finalize_worker_sync(MessageType(message_type),
                                                    (self.aria_metadata.concurrency_aborts_everywhere,
                                                     self.aria_metadata.processed_seq_size,
                                                     self.aria_metadata.max_t_counter),
                                                    Serializer.PICKLE)
                    await self.aria_metadata.cleanup()
            case MessageType.SyncCleanup | MessageType.AriaFallbackStart | MessageType.AriaFallbackDone:
                if message_type == MessageType.SyncCleanup:
                    (worker_id, epoch_throughput, epoch_latency,
                     local_abort_rate, wal_time, func_time, chain_ack_time,
                     sync_time, conflict_res_time, commit_time,
                     fallback_time, snap_time) = self.protocol_networking.decode_message(data)
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

                sync_complete: bool = await self.aria_metadata.set_empty_sync_done()
                if sync_complete:
                    await self.finalize_worker_sync(MessageType(message_type),
                                                    b'',
                                                    Serializer.NONE)
                    await self.aria_metadata.cleanup()
            case MessageType.DeterministicReordering:
                message = self.protocol_networking.decode_message(data)
                remote_read_reservation, remote_write_set, remote_read_set = message
                sync_complete: bool = await self.aria_metadata.set_deterministic_reordering_done(
                    remote_read_reservation,
                    remote_write_set,
                    remote_read_set)
                if sync_complete:
                    await self.finalize_worker_sync(MessageType(message_type),
                                                    (self.aria_metadata.global_read_reservations,
                                                     self.aria_metadata.global_write_set,
                                                     self.aria_metadata.global_read_set),
                                                    Serializer.PICKLE)
                    await self.aria_metadata.cleanup()


    async def start_puller(self):
        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    self.aio_task_scheduler.create_task(self.protocol_controller(await reader.readexactly(size)))
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(request_handler, sock=self.protocol_socket, limit=2 ** 32)
        async with server:
            await server.serve_forever()

    async def tcp_service(self):
        self.puller_task = asyncio.create_task(self.start_puller())
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            async def request_handler(reader: StreamReader, writer: StreamWriter):
                try:
                    while True:
                        data = await reader.readexactly(8)
                        (size,) = struct.unpack('>Q', data)
                        message = await reader.readexactly(size)
                        self.aio_task_scheduler.create_task(self.coordinator_controller(writer, message, pool))
                except asyncio.IncompleteReadError as e:
                    logging.info(f"Client disconnected unexpectedly: {e}")
                except asyncio.CancelledError:
                    pass
                finally:
                    logging.info("Closing the connection")
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(request_handler, sock=self.coor_socket, limit=2 ** 32)
            async with server:
                await server.serve_forever()

    def start_networking_tasks(self):
        self.networking.start_networking_tasks()
        self.protocol_networking.start_networking_tasks()

    async def finalize_worker_sync(self,
                                   msg_type: MessageType,
                                   message: tuple | bytes,
                                   serializer: Serializer = Serializer.MSGPACK):
        async with asyncio.TaskGroup() as tg:
            for worker in self.coordinator.worker_pool.get_participating_workers():
                tg.create_task(self.protocol_networking.send_message(worker.worker_ip, worker.protocol_port,
                                                                     msg=message,
                                                                     msg_type=msg_type,
                                                                     serializer=serializer))

    async def worker_wants_to_proceed(self):
        async with asyncio.TaskGroup() as tg:
            for worker in self.coordinator.worker_pool.get_participating_workers():
                tg.create_task(self.protocol_networking.send_message(worker.worker_ip, worker.protocol_port,
                                                                     msg=b'',
                                                                     msg_type=MessageType.RemoteWantsToProceed,
                                                                     serializer=Serializer.NONE))

    async def heartbeat_monitor_coroutine(self):
        interval_time = HEARTBEAT_CHECK_INTERVAL / 1000
        while True:
            await asyncio.sleep(interval_time)
            heartbeat_check_time = timer()
            workers_to_remove, heartbeats_per_worker = self.coordinator.check_heartbeats(heartbeat_check_time)
            for worker_id, time_since_last_heartbeat_ms in heartbeats_per_worker.items():
                self.heartbeat_gauge.labels(instance=worker_id).set(time_since_last_heartbeat_ms)
            if workers_to_remove or self.workers_that_re_registered:
                async with self.recovery_lock:
                    # There was a failure on a participating worker
                    workers_to_remove.update(self.workers_that_re_registered)
                    # 1) Clean up dead worker channels
                    logging.warning(f"Closing connections to dead workers: {workers_to_remove}")
                    for worker in workers_to_remove:
                        await self.networking.close_worker_connections(worker.worker_ip, worker.worker_port)
                    # 2) Start recovery
                    logging.warning("Starting recovery process")
                    await self.coordinator.start_recovery_process(workers_to_remove)
                    # 3) Wait for the cluster to become healthy
                    logging.warning("Waiting on the cluster to become healthy")
                    await self.coordinator.wait_cluster_healthy()
                    # 4) Cleanup protocol metadata
                    logging.warning("Cleaning up protocol after everyone is healthy")
                    self.aria_metadata = AriaSyncMetadata(len(self.coordinator.worker_pool.get_participating_workers()))
                    await self.protocol_networking.close_all_connections()
                    # 4) Notify Cluster that everyone is ready
                    logging.warning("Notify workers")
                    await self.coordinator.notify_cluster_healthy()
                    self.workers_that_re_registered = []

    async def send_snapshot_marker(self):
        while True:
            await asyncio.sleep(SNAPSHOT_FREQUENCY_SEC)
            async with asyncio.TaskGroup() as tg:
                for worker_id, worker in self.coordinator.worker_pool.get_workers().items():
                    tg.create_task(self.networking.send_message(worker[0], worker[1],
                                                                msg=b'',
                                                                msg_type=MessageType.SnapMarker,
                                                                serializer=Serializer.NONE))
            logging.warning('Snapshot marker sent')

    def init_snapshot_minio_bucket(self):
        try:
            if not self.minio_client.bucket_exists(SNAPSHOT_BUCKET_NAME):
                self.minio_client.make_bucket(SNAPSHOT_BUCKET_NAME)
        except minio.error.S3Error:
            # BUCKET ALREADY EXISTS
            pass

    def init_prometheus(self):
        self.metrics_server = start_http_server(8000)
        self.cpu_usage_gauge = Gauge("worker_cpu_usage_percent",
                                     "CPU usage percentage",
                                     ["instance"])
        self.memory_usage_gauge = Gauge("worker_memory_usage_mb",
                                        "Memory usage in MB",
                                        ["instance"])
        self.network_rx_gauge = Gauge("worker_network_rx_kb",
                                      "Network received KB",
                                      ["instance"])
        self.network_tx_gauge = Gauge("worker_network_tx_kb",
                                      "Network transmitted KB",
                                      ["instance"])
        self.epoch_latency_gauge = Gauge("worker_epoch_latency_ms",
                                         "Epoch Latency (ms)",
                                         ["instance"])
        self.epoch_throughput_gauge = Gauge("worker_epoch_throughput_tps",
                                            "Epoch Throughput (transactions per second)",
                                            ["instance"])
        self.epoch_abort_gauge = Gauge("worker_abort_percent",
                                            "Epoch Concurrency Abort percentage",
                                            ["instance"])
        self.latency_breakdown_gauge = Gauge("latency_breakdown",
                                             "Time Spent in different phases within the transactional protocol",
                                             ["instance", "component"])
        self.snapshotting_gauge = Gauge("worker_total_snapshotting_time_ms",
                                            "Snapshotting time (ms)",
                                            ["instance"])
        self.heartbeat_gauge = Gauge("time_since_last_heartbeat",
                                            "Time Since Last Heartbeat",
                                            ["instance"])

    async def main(self):
        self.init_snapshot_minio_bucket()
        self.init_prometheus()
        self.aio_task_scheduler.create_task(self.heartbeat_monitor_coroutine())
        self.start_networking_tasks()
        if PROTOCOL == Protocols.Unsafe or PROTOCOL == Protocols.MVCC:
            self.aio_task_scheduler.create_task(self.send_snapshot_marker())
        await self.tcp_service()


if __name__ == "__main__":
    coordinator_service = CoordinatorService()
    uvloop.run(coordinator_service.main())

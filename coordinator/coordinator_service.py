import asyncio
import os
import socket
import time
import concurrent.futures
from asyncio import StreamReader, StreamWriter
from struct import unpack

from timeit import default_timer as timer

import uvloop
from minio import Minio
import minio.error

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

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

HEARTBEAT_CHECK_INTERVAL: int = int(os.getenv('HEARTBEAT_CHECK_INTERVAL', 500))  # 100ms
HEARTBEAT_LIMIT: int = int(os.getenv('HEARTBEAT_LIMIT', 5000))  # 5000ms


class CoordinatorService(object):

    def __init__(self):
        self.networking = NetworkingManager(SERVER_PORT)
        self.protocol_networking = NetworkingManager(PROTOCOL_PORT, size=4, mode=MessagingMode.PROTOCOL_PROTOCOL)
        self.coordinator = Coordinator(self.networking)
        self.aio_task_scheduler = AIOTaskScheduler()
        self.background_tasks = set()
        self.healthy_workers: set[int] = set()
        self.worker_is_healthy: dict[int, asyncio.Event] = {}

        self.puller_task: asyncio.Task = ...

        self.coor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coor_socket.bind(('0.0.0.0', SERVER_PORT))
        self.coor_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol_socket.bind(('0.0.0.0', SERVER_PORT + 1))
        self.protocol_socket.setblocking(False)

        self.aria_metadata: AriaSyncMetadata = ...

    # Refactoring candidate
    async def coordinator_controller(self, writer: StreamWriter, data, pool: concurrent.futures.ProcessPoolExecutor):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            case MessageType.SendExecutionGraph:
                message = self.networking.decode_message(data)
                # Received execution graph from a styx client
                await self.coordinator.submit_stateflow_graph(message[0])
                logging.info("Submitted Stateflow Graph to Workers")
                logging.warning(f"Registering metadata with: {len(self.coordinator.worker_pool.get_workers())} workers")
                self.aria_metadata = AriaSyncMetadata(len(self.coordinator.worker_pool.get_workers()))
            case MessageType.RegisterWorker:  # REGISTER_WORKER
                worker_ip, worker_port, protocol_port = self.networking.decode_message(data)
                # A worker registered to the coordinator
                worker_id, send_recovery = await self.coordinator.register_worker(worker_ip, worker_port, protocol_port)
                reply = self.networking.encode_message(msg=worker_id,
                                                       msg_type=MessageType.RegisterWorker,
                                                       serializer=Serializer.MSGPACK)
                writer.write(reply)
                await writer.drain()
                logging.info(f"Worker registered {worker_ip}:{worker_port} with id {reply}")
                if send_recovery:
                    await self.coordinator.send_operators_snapshot_offsets(worker_id)
                self.healthy_workers.add(worker_id)
            case MessageType.SnapID:
                # Get snap id from worker
                worker_id, snapshot_id, start, end = self.networking.decode_message(data)
                self.coordinator.register_snapshot(worker_id, snapshot_id, pool)
                logging.warning(f'Worker: {worker_id} | '
                                f'Completed snapshot: {snapshot_id} | '
                                f'started at: {start} | '
                                f'ended at: {end} | '
                                f'took: {end - start}ms')
            case MessageType.Heartbeat:
                # HEARTBEATS
                worker_id = self.networking.decode_message(data)[0]
                heartbeat_rcv_time = timer()
                logging.info(f'Heartbeat received from: {worker_id} at time: {heartbeat_rcv_time}')
                self.coordinator.register_worker_heartbeat(worker_id, heartbeat_rcv_time)
            case MessageType.ReadyAfterRecovery:
                # report ready after recovery
                worker_id = self.networking.decode_message(data)[0]
                self.worker_is_healthy[worker_id].set()
                logging.info(f'ready after recovery received from: {worker_id}')
            case _:
                # Any other message type
                logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")

    # Refactoring candidate
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
                    (size,) = unpack('>Q', data)
                    self.aio_task_scheduler.create_task(self.protocol_controller(await reader.readexactly(size)))
            except asyncio.IncompleteReadError as e:
                logging.warning(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.warning("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(request_handler, sock=self.protocol_socket, limit=2 ** 32)
        async with server:
            await server.serve_forever()

    async def tcp_service(self):
        self.puller_task = self.aio_task_scheduler.create_task(self.start_puller())
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            async def request_handler(reader: StreamReader, writer: StreamWriter):
                try:
                    while True:
                        data = await reader.readexactly(8)
                        (size,) = unpack('>Q', data)
                        message = await reader.readexactly(size)
                        self.aio_task_scheduler.create_task(self.coordinator_controller(writer, message, pool))
                except asyncio.IncompleteReadError as e:
                    logging.warning(f"Client disconnected unexpectedly: {e}")
                except asyncio.CancelledError:
                    pass
                finally:
                    logging.warning("Closing the connection")
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
            for worker_id, worker in self.coordinator.worker_pool.get_workers().items():
                tg.create_task(self.protocol_networking.send_message(worker[0], worker[2],
                                                                     msg=message,
                                                                     msg_type=msg_type,
                                                                     serializer=serializer))

    async def worker_wants_to_proceed(self):
        async with asyncio.TaskGroup() as tg:
            for worker_id, worker in self.coordinator.worker_pool.get_workers().items():
                tg.create_task(self.protocol_networking.send_message(worker[0], worker[2],
                                                                     msg=b'',
                                                                     msg_type=MessageType.RemoteWantsToProceed,
                                                                     serializer=Serializer.NONE))

    async def heartbeat_monitor_coroutine(self):
        interval_time = HEARTBEAT_CHECK_INTERVAL / 1000
        while True:
            await asyncio.sleep(interval_time)
            workers_to_remove = set()
            heartbeat_check_time = timer()
            for worker_id, heartbeat_rcv_time in self.coordinator.worker_heartbeats.items():
                if worker_id in self.healthy_workers and \
                        (heartbeat_check_time - heartbeat_rcv_time) * 1000 > HEARTBEAT_LIMIT:
                    logging.error(f"Did not receive heartbeat for worker: {worker_id} \n"
                                  f"Initiating automatic recovery...at time: {time.time() * 1000}")
                    workers_to_remove.add(worker_id)
                    self.healthy_workers.remove(worker_id)
            if workers_to_remove:
                self.worker_is_healthy = {peer_id: asyncio.Event()
                                          for peer_id in self.coordinator.worker_heartbeats.keys()}
                await self.coordinator.remove_workers(workers_to_remove)
                await self.networking.close_all_connections()
                await self.protocol_networking.close_all_connections()
                self.aio_task_scheduler.create_task(self.cluster_became_healthy_monitor())

    async def cluster_became_healthy_monitor(self):
        logging.info('waiting for cluster to be ready')
        # wait for everyone to recover
        async with asyncio.TaskGroup() as tg:
            for event in self.worker_is_healthy.values():
                tg.create_task(event.wait())
        await self.aria_metadata.cleanup()
        # notify that everyone is ready after recovery
        async with asyncio.TaskGroup() as tg:
            for worker_id, worker in self.coordinator.worker_pool.get_workers().items():
                tg.create_task(self.networking.send_message(worker[0], worker[1],
                                                            msg=b'',
                                                            msg_type=MessageType.ReadyAfterRecovery,
                                                            serializer=Serializer.NONE))
        logging.info('ready events sent')

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

    @staticmethod
    def init_snapshot_minio_bucket():
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        try:
            if not minio_client.bucket_exists(SNAPSHOT_BUCKET_NAME):
                minio_client.make_bucket(SNAPSHOT_BUCKET_NAME)
        except minio.error.S3Error:
            # BUCKET ALREADY EXISTS
            pass

    async def main(self):
        self.init_snapshot_minio_bucket()
        self.aio_task_scheduler.create_task(self.heartbeat_monitor_coroutine())
        self.start_networking_tasks()
        if PROTOCOL == Protocols.Unsafe or PROTOCOL == Protocols.MVCC:
            self.aio_task_scheduler.create_task(self.send_snapshot_marker())
        await self.tcp_service()
        # TODO: No closing of connections in the end.


if __name__ == "__main__":
    coordinator_service = CoordinatorService()
    uvloop.run(coordinator_service.main())

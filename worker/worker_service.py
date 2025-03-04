import asyncio
import gc
import multiprocessing
import os
import struct
import time
from asyncio import StreamReader, StreamWriter
from copy import deepcopy
import socket
from timeit import default_timer as timer

import uvloop
from aiokafka import TopicPartition, AIOKafkaConsumer
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

from styx.common.local_state_backends import LocalStateBackend
from styx.common.logging import logging
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.operator import Operator
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer, msgpack_deserialization
from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition
from styx.common.util.aio_task_scheduler import AIOTaskScheduler


from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.transactional_protocols.aria import AriaProtocol
from worker.util.container_monitor import ContainerMonitor

SERVER_PORT: int = 5000
PROTOCOL_PORT: int = 6000
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
KAFKA_URL: str = os.environ['KAFKA_URL']
HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 500))  # 500ms

PROTOCOL = Protocols.Aria

# TODO Check dynamic r/w sets
# TODO networking can take a lot of optimization (i.e., batching, backpreasure e.t.c.)
# TODO when compactions happen recovery should hold and vice versa

class Worker(object):

    def __init__(self, thread_idx: int):

        self.thread_idx = thread_idx
        self.server_port = SERVER_PORT + thread_idx
        self.protocol_port = PROTOCOL_PORT + thread_idx

        self.worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                 struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.worker_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.worker_socket.bind(('0.0.0.0', self.server_port))
        self.worker_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                 struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.protocol_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.protocol_socket.bind(('0.0.0.0', self.protocol_port))
        self.protocol_socket.setblocking(False)

        self.id: int = -1
        self.networking = NetworkingManager(self.server_port)
        self.protocol_networking = NetworkingManager(self.protocol_port,
                                                     mode=MessagingMode.PROTOCOL_PROTOCOL)

        self.operator_state_backend: LocalStateBackend = ...
        self.registered_operators: dict[OperatorPartition, Operator] = {}
        self.dns: dict[str, dict[str, tuple[str, int, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int, int]] = {}
        self.local_state: InMemoryOperatorState | Stateless = Stateless()

        # Primary tasks used for processing
        self.heartbeat_proc: multiprocessing.Process = ...

        self.function_execution_protocol: AriaProtocol | None = None

        self.aio_task_scheduler = AIOTaskScheduler()

        self.async_snapshots: AsyncSnapshotsMinio = ...
        self.protocol_task: asyncio.Task = ...
        self.protocol_task_scheduler = AIOTaskScheduler()

        self.worker_operators: dict[OperatorPartition, Operator] | None = None

    async def worker_controller(self, data: bytes):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case MessageType.ReceiveExecutionPlan:
                gc.disable()
                # This contains all the operators of a job assigned to this worker
                message = self.networking.decode_message(data)
                await self.handle_execution_plan(message)
            case MessageType.InitRecovery:
                start_time = timer()
                message = self.networking.decode_message(data)
                (self.id, self.worker_operators, self.dns, self.peers,
                 self.operator_state_backend, snapshot_id) = message
                if self.function_execution_protocol is not None:
                    # This worker did not fail and needs to clean up
                    await self.function_execution_protocol.stop()
                    await self.protocol_networking.close_all_connections()
                    del (self.function_execution_protocol,
                         self.local_state,
                         self.protocol_networking)
                    gc.collect()
                    self.protocol_networking = NetworkingManager(self.protocol_port,
                                                                 mode=MessagingMode.PROTOCOL_PROTOCOL)
                    self.protocol_networking.set_worker_id(self.id)

                del self.peers[self.id]

                self.registered_operators = {}
                self.topic_partitions = []
                for operator_partition, operator in self.worker_operators.items():
                    operator_name, partition = operator_partition
                    self.registered_operators[(operator_name, partition)] = deepcopy(operator)
                    if INGRESS_TYPE == 'KAFKA':
                        self.topic_partitions.append(TopicPartition(operator_name, partition))

                self.async_snapshots = AsyncSnapshotsMinio(self.id,
                                                           n_assigned_partitions=len(self.registered_operators))
                (data, topic_partition_offsets, topic_partition_output_offsets, epoch,
                 t_counter) = self.async_snapshots.retrieve_snapshot(snapshot_id, self.registered_operators.keys())
                topic_partition_offsets = {k: v for k, v in topic_partition_offsets.items()
                                           if k in self.registered_operators}
                topic_partition_output_offsets = {k: v for k, v in topic_partition_output_offsets.items()
                                           if k in self.registered_operators}
                self.attach_state_to_operators_after_snapshot(data)

                request_id_to_t_id_map = await self.get_sequencer_assignments_before_failure(epoch)

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.protocol_networking,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                topic_partition_offsets=topic_partition_offsets,
                                                                output_offsets=topic_partition_output_offsets,
                                                                epoch_counter=epoch,
                                                                t_counter=t_counter,
                                                                request_id_to_t_id_map=request_id_to_t_id_map,
                                                                restart_after_recovery=True)
                self.function_execution_protocol.start()

                await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                   msg=(self.id,),
                                                   msg_type=MessageType.ReadyAfterRecovery,
                                                   serializer=Serializer.MSGPACK)
                end_time = timer()
                logging.warning(f'Worker: {self.id} | Recovered snapshot: {snapshot_id} '
                                f'| took: {round((end_time - start_time) * 1000, 4)}ms')
            case MessageType.ReadyAfterRecovery:
                # SYNC after recovery (Everyone is healthy)
                self.function_execution_protocol.started.set()
                logging.warning(f'Worker: {self.id} recovered and ready at : {time.time() * 1000}')
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    @staticmethod
    async def get_sequencer_assignments_before_failure(epoch_at_snapshot: int) -> dict[bytes, int] | None:
        consumer = AIOKafkaConsumer(bootstrap_servers=[KAFKA_URL],
                                    enable_auto_commit=False,
                                    auto_offset_reset="earliest")
        tp = [TopicPartition('sequencer-wal', 0)]
        consumer.assign(tp)
        request_id_to_t_id_map: dict[bytes, int] = {}
        while True:
            try:
                await consumer.start()
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(f'Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second')
                continue
            break
        try:
            current_offsets: dict[TopicPartition, int] = await consumer.end_offsets(tp)
            running = True
            while running:
                batch = await consumer.getmany(timeout_ms=1)
                for records in batch.values():
                    for record in records:
                        logged_sequence: dict[bytes, int] = msgpack_deserialization(record.value)
                        epoch: int = msgpack_deserialization(record.key)
                        if epoch >= epoch_at_snapshot:
                            request_id_to_t_id_map.update(logged_sequence)
                        if record.offset >= current_offsets[tp[0]] - 1:
                            running = False
        except Exception as e:
            logging.warning(f"Error: {e}")
        finally:
            await consumer.stop()
        return request_id_to_t_id_map

    def attach_state_to_operators_after_snapshot(self, data):
        operator_partitions: set[OperatorPartition] = set(self.registered_operators.keys())
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_partitions)
            self.local_state.set_data_from_snapshot(data)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.protocol_networking, self.dns)

    def attach_state_to_operators(self):
        operator_partitions: set[OperatorPartition] = set(self.registered_operators.keys())
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.async_snapshots = AsyncSnapshotsMinio(self.id, n_assigned_partitions=len(operator_partitions))
            if PROTOCOL == Protocols.Aria:
                self.local_state = InMemoryOperatorState(operator_partitions)
            else:
                logging.error(f"Invalid protocol: {PROTOCOL}")
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.protocol_networking, self.dns)

    async def handle_execution_plan(self, message):
        self.worker_operators, self.dns, self.peers, self.operator_state_backend = message
        del self.peers[self.id]
        operator: Operator
        for operator_partition, operator in self.worker_operators.items():
            operator_name, partition = operator_partition
            self.registered_operators[(operator_name, partition)] = deepcopy(operator)
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions.append(TopicPartition(operator_name, partition))
        self.attach_state_to_operators()
        if PROTOCOL == Protocols.Aria:
            self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                            peers=self.peers,
                                                            networking=self.protocol_networking,
                                                            registered_operators=self.registered_operators,
                                                            topic_partitions=self.topic_partitions,
                                                            state=self.local_state,
                                                            async_snapshots=self.async_snapshots)
        else:
            logging.error(f"Invalid protocol: {PROTOCOL}")

        self.function_execution_protocol.start()
        self.function_execution_protocol.started.set()

        logging.info(
            f'Registered operators: {self.registered_operators} \n'
            f'Peers: {self.peers} \n'
            f'Operator locations: {self.dns}'
        )

    async def start_tcp_service(self):

        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(self.worker_controller(message))
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(request_handler, sock=self.worker_socket, limit=2**32)
        async with server:
            await server.serve_forever()

    async def start_protocol_tcp_service(self):

        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.protocol_task_scheduler.create_task(
                        self.function_execution_protocol.protocol_tcp_controller(message)
                    )
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

    def start_networking_tasks(self):
        self.networking.start_networking_tasks()
        self.protocol_networking.start_networking_tasks()

    async def register_to_coordinator(self):
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT,
            msg=(self.networking.host_name, self.server_port, self.protocol_port),
            msg_type=MessageType.RegisterWorker,
            serializer=Serializer.MSGPACK
        )
        logging.warning(f"Worker id received from coordinator: {self.id}")
        self.protocol_networking.set_worker_id(self.id)
        self.networking.set_worker_id(self.id)

    @staticmethod
    async def heartbeat_coroutine(worker_id: int):
        networking = NetworkingManager(None, size=1, mode=MessagingMode.HEARTBEAT)
        monitor: ContainerMonitor = ContainerMonitor()
        sleep_in_seconds = HEARTBEAT_INTERVAL / 1000
        while True:
            await asyncio.sleep(sleep_in_seconds)
            cpu_perc, mem_util, rx_net, tx_net = monitor.get_stats()
            await networking.send_message(
                DISCOVERY_HOST, DISCOVERY_PORT,
                msg=(worker_id, cpu_perc, mem_util, rx_net, tx_net),
                msg_type=MessageType.Heartbeat,
                serializer=Serializer.MSGPACK
            )

    def start_heartbeat_process(self, worker_id: int):
        uvloop.run(self.heartbeat_coroutine(worker_id))

    async def main(self):
        try:
            await self.register_to_coordinator()
            self.heartbeat_proc = multiprocessing.Process(target=self.start_heartbeat_process, args=(self.id,))
            self.heartbeat_proc.start()
            self.start_networking_tasks()
            self.protocol_task = asyncio.create_task(self.start_protocol_tcp_service())
            await self.start_tcp_service()
            self.heartbeat_proc.join()
        finally:
            await self.protocol_networking.close_all_connections()
            await self.networking.close_all_connections()

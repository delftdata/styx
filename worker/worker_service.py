import asyncio
import gc
import multiprocessing
import os
import time
from asyncio import StreamReader, StreamWriter
from copy import deepcopy
import socket
from struct import unpack
from timeit import default_timer as timer

import uvloop
from aiokafka import TopicPartition

from styx.common.local_state_backends import LocalStateBackend
from styx.common.logging import logging
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.operator import Operator
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition
from styx.common.util.aio_task_scheduler import AIOTaskScheduler


from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.transactional_protocols.aria import AriaProtocol

SERVER_PORT: int = 5000
PROTOCOL_PORT: int = 6000
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']

HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 500))  # 500ms

PROTOCOL = Protocols.Aria

# TODO Check dynamic r/w sets
# TODO garbage collect the snapshots
# TODO delta map could write deltas after the first snapshot


class Worker(object):

    def __init__(self, thread_idx: int):

        self.thread_idx = thread_idx
        self.server_port = SERVER_PORT + thread_idx
        self.protocol_port = PROTOCOL_PORT + thread_idx

        self.worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_socket.bind(('0.0.0.0', self.server_port))
        self.worker_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
        self.local_state: InMemoryOperatorState | Stateless = ...

        # Primary tasks used for processing
        self.heartbeat_proc: multiprocessing.Process = ...

        self.function_execution_protocol: AriaProtocol = ...

        self.aio_task_scheduler = AIOTaskScheduler()

        self.async_snapshots: AsyncSnapshotsMinio = ...
        self.protocol_task: asyncio.Task = ...
        self.protocol_task_scheduler = AIOTaskScheduler()

        self.worker_operators = None

    # Refactoring candidate
    async def worker_controller(self, data: bytes):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case MessageType.ReceiveExecutionPlan:
                gc.disable()
                # This contains all the operators of a job assigned to this worker
                message = self.networking.decode_message(data)
                await self.handle_execution_plan(message)
            case MessageType.RecoveryOther:
                # RECOVERY_OTHER
                start_time = timer()
                logging.warning('RECOVERY_OTHER')
                await self.function_execution_protocol.stop()
                await self.networking.close_all_connections()
                await self.protocol_networking.close_all_connections()
                del (self.function_execution_protocol,
                     self.local_state,
                     self.networking,
                     self.protocol_networking,
                     self.async_snapshots,
                     self.registered_operators)
                gc.collect()

                self.networking = NetworkingManager(self.server_port)
                self.protocol_networking = NetworkingManager(self.protocol_port,
                                                             mode=MessagingMode.PROTOCOL_PROTOCOL)
                self.protocol_networking.set_worker_id(self.id)
                self.networking.set_worker_id(self.id)

                message = self.networking.decode_message(data)
                self.dns, self.peers, self.operator_state_backend, snapshot_id = message
                del self.peers[self.id]

                self.registered_operators = {}
                for tup in self.worker_operators:
                    operator, partition = tup
                    self.registered_operators[(operator.name, partition)] = deepcopy(operator)

                self.async_snapshots = AsyncSnapshotsMinio(self.id, snapshot_id=snapshot_id + 1)
                (data, topic_partition_offsets, topic_partition_output_offsets, epoch,
                 t_counter) = self.async_snapshots.retrieve_snapshot(snapshot_id, self.registered_operators.keys())
                self.attach_state_to_operators_after_snapshot(data)

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.protocol_networking,
                                                                protocol_socket=self.protocol_socket,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                topic_partition_offsets=topic_partition_offsets,
                                                                output_offsets=topic_partition_output_offsets,
                                                                epoch_counter=epoch,
                                                                t_counter=t_counter,
                                                                restart_after_recovery=True)
                self.function_execution_protocol.start()

                await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                   msg=(self.id, ),
                                                   msg_type=MessageType.ReadyAfterRecovery,
                                                   serializer=Serializer.MSGPACK)
                end_time = timer()
                logging.warning(f'Worker: {self.id} | Recovered snapshot: {snapshot_id} '
                                f'| took: {round((end_time - start_time) * 1000, 4)}ms | OTHER FAILURE')
            case MessageType.RecoveryOwn:
                # RECOVERY_OWN
                start_time = timer()
                logging.warning('RECOVERY_OWN')
                message = self.networking.decode_message(data)
                self.id, self.worker_operators, self.dns, self.peers, self.operator_state_backend, snapshot_id = message
                del self.peers[self.id]

                operator: Operator
                for tup in self.worker_operators:
                    operator, partition = tup
                    self.registered_operators[(operator.name, partition)] = deepcopy(operator)
                    if INGRESS_TYPE == 'KAFKA':
                        self.topic_partitions.append(TopicPartition(operator.name, partition))

                self.async_snapshots = AsyncSnapshotsMinio(self.id, snapshot_id=snapshot_id + 1)
                (data, topic_partition_offsets, topic_partition_output_offsets, epoch,
                 t_counter) = self.async_snapshots.retrieve_snapshot(snapshot_id, self.registered_operators.keys())
                self.attach_state_to_operators_after_snapshot(data)

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.protocol_networking,
                                                                protocol_socket=self.protocol_socket,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                topic_partition_offsets=topic_partition_offsets,
                                                                output_offsets=topic_partition_output_offsets,
                                                                epoch_counter=epoch,
                                                                t_counter=t_counter,
                                                                restart_after_recovery=True)
                self.function_execution_protocol.start()
                # send that we are ready to the coordinator
                await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                   msg=(self.id, ),
                                                   msg_type=MessageType.ReadyAfterRecovery,
                                                   serializer=Serializer.MSGPACK)
                end_time = timer()
                logging.warning(f'Worker: {self.id} | Recovered snapshot: {snapshot_id} '
                                f'| took: {round((end_time - start_time) * 1000, 4)}ms | OWN FAILURE')
            case MessageType.ReadyAfterRecovery:
                # SYNC after recovery (Everyone is healthy)
                self.function_execution_protocol.started.set()
                logging.warning(f'Worker recovered and ready at : {time.time()*1000}')
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

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
            self.async_snapshots = AsyncSnapshotsMinio(self.id)
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
        for tup in self.worker_operators:
            operator, partition = tup
            self.registered_operators[(operator.name, partition)] = deepcopy(operator)
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions.append(TopicPartition(operator.name, partition))
        self.attach_state_to_operators()
        if PROTOCOL == Protocols.Aria:
            self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                            peers=self.peers,
                                                            networking=self.protocol_networking,
                                                            protocol_socket=self.protocol_socket,
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
                    (size,) = unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(self.worker_controller(message))
            except asyncio.IncompleteReadError as e:
                logging.warning(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.warning("Closing the connection")
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
                    (size,) = unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.protocol_task_scheduler.create_task(
                        self.function_execution_protocol.protocol_tcp_controller(message)
                    )
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
        await server.wait_closed()

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
        networking = NetworkingManager(None)
        sleep_in_seconds = HEARTBEAT_INTERVAL / 1000
        while True:
            await asyncio.sleep(sleep_in_seconds)
            await networking.send_message(
                DISCOVERY_HOST, DISCOVERY_PORT,
                msg=(worker_id, ),
                msg_type=MessageType.Heartbeat,
                serializer=Serializer.MSGPACK
            )

    def start_heartbeat_process(self, worker_id: int):
        uvloop.run(self.heartbeat_coroutine(worker_id))

    async def main(self):
        try:
            await self.register_to_coordinator()
            # Instead: create_task(heartbeat_coroutine)
            self.heartbeat_proc = multiprocessing.Process(target=self.start_heartbeat_process, args=(self.id,))
            self.heartbeat_proc.start()
            self.start_networking_tasks()
            self.protocol_task = asyncio.create_task(self.start_protocol_tcp_service())
            await self.start_tcp_service()
            self.heartbeat_proc.join()
        finally:
            await self.protocol_networking.close_all_connections()
            await self.networking.close_all_connections()

import asyncio
import multiprocessing
import concurrent.futures
import os
import struct
import threading
import time
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
from copy import deepcopy
import socket
from timeit import default_timer as timer
import logging as sync_logging
from typing import Any

from minio import Minio
import uvloop
from aiokafka import TopicPartition, AIOKafkaConsumer
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

from styx.common.local_state_backends import LocalStateBackend
from styx.common.logging import logging
from styx.common.partitioning.hash_partitioner import HashPartitioner
from styx.common.stateflow_graph import StateflowGraph
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.operator import Operator
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer, msgpack_deserialization
from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.async_snapshotting import AsyncSnapshottingProcess
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.transactional_protocols.aria import AriaProtocol
from worker.util.container_monitor import ContainerMonitor

SERVER_PORT: int = 5000
PROTOCOL_PORT: int = 6000
SNAPSHOTTING_PORT: int = 7000
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
KAFKA_URL: str = os.environ['KAFKA_URL']
HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 500))  # 500ms
SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")
MIGRATION_THREADS = int(os.getenv('MIGRATION_THREADS', 4))

PROTOCOL = Protocols.Aria

# TODO Check dynamic r/w sets
# TODO networking can take a lot of optimization (i.e., batching, backpreasure e.t.c.)
# TODO when compactions happen recovery should hold and vice versa

class Worker(object):

    def __init__(self, thread_idx: int):

        self.thread_idx = thread_idx
        self.server_port = SERVER_PORT + thread_idx
        self.protocol_port = PROTOCOL_PORT + thread_idx
        self.snapshotting_port = SNAPSHOTTING_PORT + thread_idx

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
        self.dns: dict[str, dict[int, tuple[str, int, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int, int]] = {}
        self.local_state: InMemoryOperatorState | Stateless = Stateless()

        # Primary tasks used for processing
        self.heartbeat_proc: multiprocessing.Process = ...
        self.async_snapshotting_proc : multiprocessing.Process = ...

        self.function_execution_protocol: AriaProtocol | None = None

        self.aio_task_scheduler = AIOTaskScheduler()

        self.async_snapshots: AsyncSnapshotsMinio = ...
        self.protocol_task: asyncio.Task = ...
        self.protocol_task_scheduler = AIOTaskScheduler()

        self.worker_operators: dict[OperatorPartition, Operator] | None = None

        self.minio_client: Minio = Minio(MINIO_URL,
                                         access_key=MINIO_ACCESS_KEY,
                                         secret_key=MINIO_SECRET_KEY,
                                         secure=False)

        self.migration_repartitioning_done: asyncio.Event = asyncio.Event()
        self.migration_completed: asyncio.Event = asyncio.Event()

        self.m_input_offsets: dict[OperatorPartition, int] = {}
        self.m_output_offsets: dict[OperatorPartition, int] = {}
        self.m_epoch_counter: int = -1
        self.m_t_counter: int = -1

        self.pool: concurrent.futures.ProcessPoolExecutor | None = None
        self.total_repartitioning: int = -1
        self.completed_repartitioning: int = 0
        self.completed_repartitioning_event: asyncio.Event = asyncio.Event()

        self.deployed_graph: StateflowGraph | None = None

        self.final_keys_to_send = defaultdict(set)
        self.results_lock = threading.Lock()

        self.networking_locks: dict[MessageType, asyncio.Lock] = {
            MessageType.ReceiveMigrationHashes: asyncio.Lock(),
            MessageType.ReceiveRemoteKey: asyncio.Lock(),
            MessageType.RequestRemoteKey: asyncio.Lock()
        }

    @staticmethod
    def rehash_and_store(operator_partition: OperatorPartition,
                         partitioner: HashPartitioner,
                         operator_partition_keys: list[Any],
                         dns: dict[str, dict[int, tuple[str, int, int]]],
                         worker_id: int):
        ser_time = 0.0
        wire_time = 0.0
        operator_name, previous_partition = operator_partition
        # Start hashing
        start_hashing = timer()
        keys_to_send = defaultdict(set)
        new_partitions: dict[OperatorPartition, dict[Any, tuple[int, int]]] = {
            (operator_name, partition): {} for partition in range(partitioner.partitions)
        }
        for key in operator_partition_keys:
            new_partition: int = partitioner.get_partition_no_cache(key)
            operator_partition = (operator_name, new_partition)
            if new_partition != previous_partition:
                # If this key is already in the correct partition no need to transfer it
                new_partitions[operator_partition][key] = (worker_id, previous_partition)
                keys_to_send[(operator_name, previous_partition)].add((key, new_partition))
        end_hashing = timer()

        def upload_partition(msg: bytes, worker_info: tuple[str, int, int]):
            s_wire = timer()
            try:
                s = socket.socket()
                s.connect((worker_info[0], worker_info[1]))
                s.send(msg)
                s.close()
            except Exception as e:
                sync_logging.error(f"MIGRATION | NETWORK THREAD ERROR: {e}")
            e_wire = timer()
            return e_wire - s_wire

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for n_op, data in new_partitions.items():
                if not data:
                    continue  # Skip empty partitions
                n_o, n_p = n_op
                s_ser = timer()
                serialized_hashes: bytes = NetworkingManager.encode_message(msg=(n_op, data),
                                                                            msg_type=MessageType.ReceiveMigrationHashes,
                                                                            serializer=Serializer.MSGPACK)
                e_ser = timer()
                ser_time += e_ser - s_ser
                # sync_logging.warning(f"MIGRATION | SENDING: {n_op} FROM: {worker_id}")
                futures.append(executor.submit(upload_partition, serialized_hashes, dns[n_o][n_p]))

            for fut in futures:
                wire_time += fut.result()

        sync_logging.warning(f"MIGRATION of {operator_name}:{previous_partition} | "
                             f"hashing_time: {end_hashing - start_hashing:.3f}s | "
                             f"ser_time: {ser_time:.3f}s | "
                             f"wire_time: {wire_time:.3f}s")
        return keys_to_send

    def repartitioning_callback(self, future):
        with self.results_lock:
            self.completed_repartitioning += 1
            keys_to_send = future.result()
            for op_partition, keys in keys_to_send.items():
                self.final_keys_to_send[op_partition].update(keys)
            if self.completed_repartitioning == self.total_repartitioning:
                self.completed_repartitioning_event.set()
                self.completed_repartitioning = 0

    async def worker_controller(self, data: bytes):
        message_type: MessageType = self.networking.get_msg_type(data)
        match message_type:
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case MessageType.ReceiveExecutionPlan:
                # This contains all the operators of a job assigned to this worker
                message = self.networking.decode_message(data)

                self.worker_operators, self.dns, self.peers, self.operator_state_backend, self.deployed_graph = message
                del self.peers[self.id]
                self.networking.set_peers(self.peers)
                self.protocol_networking.set_peers(self.peers)
                operator: Operator
                for operator_partition, operator in self.worker_operators.items():
                    operator_name, partition = operator_partition
                    self.registered_operators[(operator_name, partition)] = deepcopy(operator)
                    if INGRESS_TYPE == 'KAFKA':
                        self.topic_partitions.append(TopicPartition(operator_name, partition))
                self.async_snapshots = AsyncSnapshotsMinio(self.id,
                                                           n_assigned_partitions=len(self.registered_operators))
                await self.networking.send_message(self.networking.host_name, self.snapshotting_port,
                                                   msg=(list(self.registered_operators.keys()), 0),
                                                   msg_type=MessageType.SnapNAssigned,
                                                   serializer=Serializer.MSGPACK)
                (data, topic_partition_offsets, topic_partition_output_offsets, epoch,
                 t_counter) = self.async_snapshots.retrieve_snapshot(0, self.registered_operators.keys())
                topic_partition_offsets = {k: v for k, v in topic_partition_offsets.items()
                                           if k in self.registered_operators}
                topic_partition_output_offsets = {k: v for k, v in topic_partition_output_offsets.items()
                                                  if k in self.registered_operators}
                self.attach_state_to_operators_after_snapshot(data)
                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                dns=self.dns,
                                                                networking=self.protocol_networking,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                snapshotting_port=self.snapshotting_port,
                                                                topic_partition_offsets=topic_partition_offsets,
                                                                output_offsets=topic_partition_output_offsets,
                                                                epoch_counter=epoch,
                                                                t_counter=t_counter)
                self.function_execution_protocol.start()
                self.function_execution_protocol.started.set()

                # logging.info(
                #     f'Registered operators: {self.registered_operators} \n'
                #     f'Peers: {self.peers} \n'
                #     f'Operator locations: {self.dns}'
                # )
            case MessageType.InitMigration:
                try:
                    logging.warning(f"MIGRATION | START at {time.time_ns() // 1_000_000}")
                    start_time = timer()
                    # 1) Wait for the transactional protocol to get stopped gracefully
                    await self.function_execution_protocol.wait_stopped()
                    t1 = timer()
                    logging.warning(f"MIGRATION | Function Execution Protocol Stopped! |"
                                    f" @Epoch {self.function_execution_protocol.sequencer.epoch_counter} |"
                                    f" took: {t1 - start_time}")
                    (self.deployed_graph, new_worker_operators, self.dns,
                     self.peers, self.operator_state_backend) = self.networking.decode_message(data)
                    del self.peers[self.id]
                    self.networking.set_peers(self.peers)
                    self.protocol_networking.set_peers(self.peers)
                    logging.warning("MIGRATION | ARIA STOPPED")
                    # 2) Once stopped take the hashes and store stage
                    t2 = timer()
                    operator_partitions_to_repartition = self.local_state.get_operator_partitions_to_repartition()
                    self.total_repartitioning = sum([len(operator_partitions)
                                                     for operator_partitions in operator_partitions_to_repartition.values()])
                    loop = asyncio.get_running_loop()
                    for operator_name, operator_partitions in operator_partitions_to_repartition.items():
                        for operator_partition in operator_partitions:
                            new_operator_partitioner: HashPartitioner = self.deployed_graph.get_operator_by_name(operator_name).get_partitioner()
                            logging.warning(f'Sending: {operator_partition} for repartitioning')
                            loop.run_in_executor(self.pool,
                                                 self.rehash_and_store,
                                                 operator_partition,
                                                 new_operator_partitioner,
                                                 list(self.local_state.get_operator_data_for_repartitioning(operator_partition).keys()),
                                                 self.dns,
                                                 self.id).add_done_callback(self.repartitioning_callback)
                    await self.completed_repartitioning_event.wait()
                    self.completed_repartitioning_event.clear()
                    self.local_state.add_keys_to_send(self.final_keys_to_send)
                    # 3) Coordinate: Everyone done with repartitioning
                    self.worker_operators = new_worker_operators
                    await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                       msg=(self.function_execution_protocol.sequencer.epoch_counter,
                                                            self.function_execution_protocol.sequencer.t_counter,
                                                            self.function_execution_protocol.topic_partition_offsets,
                                                            self.function_execution_protocol.egress.topic_partition_output_offsets),
                                                       msg_type=MessageType.MigrationRepartitioningDone,
                                                       serializer=Serializer.MSGPACK)
                    await self.migration_repartitioning_done.wait()
                    t3 = timer()
                    logging.warning(f"MIGRATION | REPARTITIONING DONE | took: {t3 - t2}")
                    # 4) Deploy new_graph, dns, networking, state, protocol, kafka topics
                    await self.protocol_networking.close_all_connections()
                    del (self.function_execution_protocol,
                         self.protocol_networking)
                    self.protocol_networking = NetworkingManager(self.protocol_port,
                                                                 mode=MessagingMode.PROTOCOL_PROTOCOL)
                    self.protocol_networking.set_worker_id(self.id)
                    self.protocol_networking.set_peers(self.peers)
                    self.registered_operators = {}
                    self.topic_partitions = []
                    for operator_partition, operator in self.worker_operators.items():
                        operator_name, partition = operator_partition
                        self.registered_operators[(operator_name, partition)] = deepcopy(operator)
                        if INGRESS_TYPE == 'KAFKA':
                            self.topic_partitions.append(TopicPartition(operator_name, partition))
                    self.async_snapshots.update_n_assigned_partitions(n_assigned_partitions=len(self.registered_operators))
                    await self.networking.send_message(self.networking.host_name, self.snapshotting_port,
                                                       msg=(list(self.registered_operators.keys()),-1),
                                                       msg_type=MessageType.SnapNAssigned,
                                                       serializer=Serializer.MSGPACK)
                    # Read from the stored state
                    t4 = timer()
                    logging.warning(f"MIGRATION | LOADING DATA START | took: {t4 - t3}")
                    operator_partitions: set[OperatorPartition] = set(self.registered_operators.keys())
                    for operator_partition in operator_partitions:
                        if operator_partition not in self.local_state.operator_partitions:
                            # check if operator partition is in local state, if not, set it to empty
                            self.local_state.add_new_operator_partition(operator_partition)
                    for operator in self.registered_operators.values():
                        operator.attach_state_networking(self.local_state, self.protocol_networking, self.dns,
                                                         self.deployed_graph)
                    t5 = timer()
                    logging.warning(f"MIGRATION | LOADING DATA DONE | took: {t5 - t4}")
                    topic_partition_offsets = {}
                    topic_partition_output_offsets = {}
                    for operator_partition in self.registered_operators.keys():
                        if operator_partition in self.m_input_offsets:
                            topic_partition_offsets[operator_partition] = self.m_input_offsets[operator_partition]
                        else:
                            topic_partition_offsets[operator_partition] = - 1
                        if operator_partition in self.m_output_offsets:
                            topic_partition_output_offsets[operator_partition] = self.m_output_offsets[operator_partition]
                        else:
                            topic_partition_output_offsets[operator_partition] = - 1
                    self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                    peers=self.peers,
                                                                    dns=self.dns,
                                                                    networking=self.protocol_networking,
                                                                    registered_operators=self.registered_operators,
                                                                    topic_partitions=self.topic_partitions,
                                                                    state=self.local_state,
                                                                    snapshotting_port=self.snapshotting_port,
                                                                    topic_partition_offsets=topic_partition_offsets,
                                                                    output_offsets=topic_partition_output_offsets,
                                                                    epoch_counter=self.m_epoch_counter,
                                                                    t_counter=self.m_t_counter)
                    # 5) Coordinate everyone ready to resume processing
                    logging.warning(f"MIGRATION | SENDING MigrationDone TO COORDINATOR")
                    await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                       msg=b'',
                                                       msg_type=MessageType.MigrationDone,
                                                       serializer=Serializer.NONE)
                    logging.warning(f"MIGRATION | WAITING SYNC")
                    await self.migration_completed.wait()
                    t6 = timer()
                    logging.warning(f"MIGRATION | PROCESSING CONTINUES | took: {t6 - t5}")
                    # 6) Start the function_execution_protocol
                    self.function_execution_protocol.start()
                    self.function_execution_protocol.started.set()
                    self.migration_completed.clear()
                    self.migration_repartitioning_done.clear()
                    end_time = timer()
                    logging.warning(f'Worker: {self.id} | Migration took: {round((end_time - start_time) * 1000, 4)}ms')
                except Exception as e:
                    logging.error(f"Uncaught exception while migrating: {e}")
            case MessageType.ReceiveMigrationHashes:
                (n_op, data) = self.networking.decode_message(data)
                # logging.warning(f"MIGRATION ReceiveMigrationHashes | {n_op} {len(data.keys())}")
                async with self.networking_locks[message_type]:
                    self.local_state.add_remote_keys(n_op, data)
            case MessageType.RequestRemoteKey:
                (operator_partition, key, old_partition, host, port) = self.networking.decode_message(data)
                # logging.warning(f"MIGRATION RequestRemoteKey | {key}:{old_partition} to {operator_partition}")
                async with self.networking_locks[message_type]:
                    data_to_send = self.local_state.get_key_to_migrate(operator_partition, key, old_partition)
                    await self.networking.send_message(host, port,
                                                       msg=(operator_partition, key, data_to_send),
                                                       msg_type=MessageType.ReceiveRemoteKey,
                                                       serializer=Serializer.MSGPACK)
            case MessageType.ReceiveRemoteKey:
                (operator_partition, key, value) = self.networking.decode_message(data)
                # logging.warning(f"MIGRATION ReceiveRemoteKey | {key} to {operator_partition}")
                async with self.networking_locks[message_type]:
                    self.local_state.set_data_from_migration(operator_partition, key, value)
                    self.protocol_networking.key_received(operator_partition, key)
            case MessageType.MigrationRepartitioningDone:
                (self.m_epoch_counter, self.m_t_counter,
                 self.m_input_offsets, self.m_output_offsets) = self.networking.decode_message(data)
                self.migration_repartitioning_done.set()
            case MessageType.MigrationDone:
                self.migration_completed.set()
            case MessageType.InitRecovery:
                start_time = timer()
                message = self.networking.decode_message(data)
                (self.id, self.worker_operators, self.dns, self.peers,
                 self.operator_state_backend, snapshot_id, self.deployed_graph) = message
                if self.function_execution_protocol is not None:
                    # This worker did not fail and needs to clean up
                    await self.function_execution_protocol.stop()
                    await self.protocol_networking.close_all_connections()
                    del (self.function_execution_protocol,
                         self.local_state,
                         self.protocol_networking)
                    self.protocol_networking = NetworkingManager(self.protocol_port,
                                                                 mode=MessagingMode.PROTOCOL_PROTOCOL)
                    self.protocol_networking.set_worker_id(self.id)

                del self.peers[self.id]
                self.protocol_networking.set_peers(self.peers)

                self.registered_operators = {}
                self.topic_partitions = []
                for operator_partition, operator in self.worker_operators.items():
                    operator_name, partition = operator_partition
                    self.registered_operators[(operator_name, partition)] = deepcopy(operator)
                    if INGRESS_TYPE == 'KAFKA':
                        self.topic_partitions.append(TopicPartition(operator_name, partition))

                self.async_snapshots = AsyncSnapshotsMinio(self.id,
                                                           n_assigned_partitions=len(self.registered_operators))
                await self.networking.send_message(self.networking.host_name, self.snapshotting_port,
                                                   msg=(list(self.registered_operators.keys()), snapshot_id),
                                                   msg_type=MessageType.SnapNAssigned,
                                                   serializer=Serializer.MSGPACK)
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
                                                                dns=self.dns,
                                                                networking=self.protocol_networking,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                snapshotting_port=self.snapshotting_port,
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
            operator.attach_state_networking(self.local_state, self.protocol_networking, self.dns, self.deployed_graph)

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
        logging.warning("Starting Worker TCP Service")
        with concurrent.futures.ProcessPoolExecutor(MIGRATION_THREADS) as self.pool:
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
                    if self.function_execution_protocol is not None:
                        self.protocol_task_scheduler.create_task(
                            self.function_execution_protocol.protocol_tcp_controller(message)
                        )
                    else:
                        logging.debug(f"Dropped message_type: {self.networking.get_msg_type(message)} "
                                      f"due to protocol service restart (This is the expected behaviour)")
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()
        logging.warning("Starting Protocol TCP Service")
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
    async def heartbeat_coroutine(worker_id: int, worker_pid: int):
        networking = NetworkingManager(None, size=1, mode=MessagingMode.HEARTBEAT)
        monitor: ContainerMonitor = ContainerMonitor(worker_pid)
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

    def start_heartbeat_process(self, worker_id: int, worker_pid: int):
        uvloop.run(self.heartbeat_coroutine(worker_id, worker_pid))

    async def main(self):
        try:
            await self.register_to_coordinator()
            worker_pid: int = os.getpid()
            self.heartbeat_proc = multiprocessing.Process(target=self.start_heartbeat_process, args=(self.id,
                                                                                                     worker_pid))
            async_snapshotting_process = AsyncSnapshottingProcess(self.snapshotting_port, self.id)
            self.async_snapshotting_proc = multiprocessing.Process(target=async_snapshotting_process.start_snapshot_process)
            self.async_snapshotting_proc.start()
            self.heartbeat_proc.start()
            self.start_networking_tasks()
            self.protocol_task = asyncio.create_task(self.start_protocol_tcp_service())
            await self.start_tcp_service()
            self.heartbeat_proc.join()
            self.async_snapshotting_proc.join()
        finally:
            await self.protocol_networking.close_all_connections()
            await self.networking.close_all_connections()

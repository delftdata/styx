from __future__ import annotations

import asyncio
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
import concurrent.futures
from copy import deepcopy
import gc
import logging as sync_logging
import multiprocessing
import os
import random
import socket
import struct
import sys
import time
from timeit import default_timer as timer
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from concurrent.futures import Future

    from styx.common.operator import Operator
    from styx.common.partitioning.hash_partitioner import HashPartitioner
    from styx.common.stateflow_graph import StateflowGraph
    from styx.common.types import KVPairs, OperatorPartition

import contextlib

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaConnectionError, UnknownTopicOrPartitionError
from styx.common.local_state_backends import LocalStateBackend
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer, msgpack_deserialization
from styx.common.tcp_networking import MessagingMode, NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler
import uvloop

from worker.async_snapshotting import AsyncSnapshottingProcess
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.transactional_protocols.aria import AriaProtocol
from worker.util.container_monitor import ContainerMonitor

SERVER_PORT: int = 5000
PROTOCOL_PORT: int = 6000
SNAPSHOTTING_PORT: int = 7000
DISCOVERY_HOST: str = os.environ["DISCOVERY_HOST"]
DISCOVERY_PORT: int = int(os.environ["DISCOVERY_PORT"])
INGRESS_TYPE = os.getenv("INGRESS_TYPE", None)

KAFKA_URL: str = os.environ["KAFKA_URL"]
HEARTBEAT_INTERVAL: int = int(os.getenv("HEARTBEAT_INTERVAL", "500"))  # 500ms
SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")
MIGRATION_THREADS = int(os.getenv("MIGRATION_THREADS", "4"))

# Backpressure-related sizes
PROTOCOL_QUEUE_SIZE: int = int(os.getenv("PROTOCOL_QUEUE_SIZE", "10000"))
CONTROL_QUEUE_SIZE: int = int(os.getenv("CONTROL_QUEUE_SIZE", "10000"))
PROTOCOL_WORKERS: int = int(os.getenv("PROTOCOL_WORKERS", "100"))

PROTOCOL = Protocols.Aria


def repair_stdio() -> None:
    with contextlib.suppress(Exception):
        sys.stdout = os.fdopen(1, "w", buffering=1, closefd=False)
    with contextlib.suppress(Exception):
        sys.stderr = os.fdopen(2, "w", buffering=1, closefd=False)


class Worker:
    def __init__(self, thread_idx: int) -> None:
        self.thread_idx = thread_idx
        self.server_port = SERVER_PORT + thread_idx
        self.protocol_port = PROTOCOL_PORT + thread_idx
        self.snapshotting_port = SNAPSHOTTING_PORT + thread_idx

        self.worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.worker_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        self.worker_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.worker_socket.bind(("0.0.0.0", self.server_port))  # noqa: S104
        self.worker_socket.setblocking(False)

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
        self.protocol_socket.bind(("0.0.0.0", self.protocol_port))  # noqa: S104
        self.protocol_socket.setblocking(False)

        self.id: int = -1
        self.networking = NetworkingManager(self.server_port)
        self.protocol_networking = NetworkingManager(
            self.protocol_port,
            mode=MessagingMode.PROTOCOL_PROTOCOL,
        )

        self.operator_state_backend: LocalStateBackend | None = None
        self.registered_operators: dict[OperatorPartition, Operator] = {}
        self.dns: dict[str, dict[int, tuple[str, int, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        # worker_id: host, port
        self.peers: dict[int, tuple[str, int, int]] = {}
        self.local_state: InMemoryOperatorState | Stateless = Stateless()

        # Primary tasks used for processing
        self.heartbeat_proc: multiprocessing.Process | None = None
        self.async_snapshotting_proc: multiprocessing.Process | None = None

        self.function_execution_protocol: AriaProtocol | None = None

        self.aio_task_scheduler = AIOTaskScheduler()
        self.protocol_task_scheduler = AIOTaskScheduler()

        self.async_snapshots: AsyncSnapshotsS3 | None = None
        self.protocol_task: asyncio.Task | None = None

        self.worker_operators: dict[OperatorPartition, Operator] | None = None

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
        self._pending_hash_metadata: dict[OperatorPartition, dict[Any, tuple[int, int]]] = defaultdict(dict)
        self._pending_migration_data: bytes | None = None
        self.migration_error: BaseException | None = None

        self.networking_locks: dict[MessageType, asyncio.Lock] = {
            MessageType.ReceiveMigrationHashes: asyncio.Lock(),
            MessageType.ReceiveRemoteKey: asyncio.Lock(),
            MessageType.RequestRemoteKey: asyncio.Lock(),
        }

        # Bounded queues for backpressure
        # - protocol_queue: for protocol TCP messages
        # - control_queue: for control-plane / worker_controller messages
        self.protocol_queue: asyncio.Queue[bytes] = asyncio.Queue(
            maxsize=PROTOCOL_QUEUE_SIZE,
        )
        self.control_queue: asyncio.Queue[bytes] = asyncio.Queue(
            maxsize=CONTROL_QUEUE_SIZE,
        )

        self.mp_ctx = multiprocessing.get_context("spawn")
        self.control_queue_worker_task: asyncio.Task | None = None

        self._message_handlers: dict[MessageType, Callable[[bytes, MessageType], Awaitable[None]]] = {
            MessageType.ReceiveExecutionPlan: self._handle_receive_execution_plan,
            MessageType.UpdateExecutionGraph: self._handle_update_execution_plan,
            MessageType.InitMigration: self._handle_init_migration,
            MessageType.ReceiveMigrationHashes: self._handle_receive_migration_hashes,
            MessageType.RequestRemoteKey: self._handle_request_remote_key,
            MessageType.ReceiveRemoteKey: self._handle_receive_remote_key,
            MessageType.MigrationRepartitioningDone: self._handle_migration_repartitioning_done,
            MessageType.MigrationDone: self._handle_migration_done,
            MessageType.InitRecovery: self._handle_init_recovery,
            MessageType.ReadyAfterRecovery: self._handle_ready_after_recovery,
        }

    @staticmethod
    def rehash_only(
        operator_partition: OperatorPartition,
        partitioner: HashPartitioner,
        operator_partition_keys: list[Any],
        worker_id: int,
    ) -> tuple[dict[OperatorPartition, set[tuple[Any, int]]], dict[OperatorPartition, dict[Any, tuple[int, int]]]]:
        """Pure CPU rehashing — no network I/O.

        Returns:
            (keys_to_send, hash_metadata)
            - keys_to_send: {(op_name, old_partition): {(key, new_partition), ...}}
            - hash_metadata: {(op_name, new_partition): {key: (worker_id, old_partition), ...}}
        """
        operator_name, previous_partition = operator_partition
        start_hashing = timer()
        keys_to_send: dict[OperatorPartition, set[tuple[Any, int]]] = defaultdict(set)
        hash_metadata: dict[OperatorPartition, dict[Any, tuple[int, int]]] = defaultdict(dict)
        for key in operator_partition_keys:
            new_partition: int = partitioner.get_partition_no_cache(key)
            if new_partition != previous_partition:
                hash_metadata[(operator_name, new_partition)][key] = (
                    worker_id,
                    previous_partition,
                )
                keys_to_send[(operator_name, previous_partition)].add(
                    (key, new_partition),
                )
        end_hashing = timer()
        sync_logging.warning(
            "MIGRATION of %s:%s | hashing_time: %.3fs",
            operator_name,
            previous_partition,
            end_hashing - start_hashing,
        )
        return keys_to_send, hash_metadata

    def repartitioning_callback(self, future: Future) -> None:
        # This callback runs on the event loop thread (scheduled via call_soon
        # by asyncio.Future.add_done_callback), so no threading lock is needed.
        self.completed_repartitioning += 1
        try:
            keys_to_send, hash_metadata = future.result()
        except Exception:
            logging.exception("MIGRATION | Repartitioning subprocess failed")
            self.migration_error = future.exception()
            self.completed_repartitioning_event.set()
            return
        for op_partition, keys in keys_to_send.items():
            self.final_keys_to_send[op_partition].update(keys)
        for dest_partition, key_map in hash_metadata.items():
            self._pending_hash_metadata[dest_partition].update(key_map)
        if self.completed_repartitioning == self.total_repartitioning:
            self.completed_repartitioning_event.set()

    async def worker_controller(self, data: bytes) -> None:
        msg_type = self.networking.get_msg_type(data)
        handler = self._message_handlers.get(msg_type, self._handle_unknown)
        await handler(data, msg_type)

    # -----------------------
    # Small shared helpers
    # -----------------------
    def _update_peers(self, peers: dict) -> None:
        del peers[self.id]
        self.peers = peers
        self.networking.set_peers(peers)
        self.protocol_networking.set_peers(peers)

    def _build_registered_operators(self, worker_operators: dict) -> None:
        self.registered_operators = {}
        self.topic_partitions = []
        for (op_name, partition), operator in worker_operators.items():
            self.registered_operators[(op_name, partition)] = deepcopy(operator)
            if INGRESS_TYPE == "KAFKA":
                self.topic_partitions.append(TopicPartition(op_name, partition))

    async def _reset_protocol_networking(self) -> None:
        await self.protocol_networking.close_all_connections()
        del self.protocol_networking
        gc.collect()
        self.protocol_networking = NetworkingManager(
            self.protocol_port,
            mode=MessagingMode.PROTOCOL_PROTOCOL,
        )
        self.protocol_networking.set_worker_id(self.id)
        self.protocol_networking.set_peers(self.peers)

    async def _send_snap_assigned(self, snapshot_id: int) -> None:
        await self.networking.send_message(
            self.networking.host_name,
            self.snapshotting_port,
            msg=(list(self.registered_operators.keys()), snapshot_id),
            msg_type=MessageType.SnapNAssigned,
            serializer=Serializer.MSGPACK,
        )

    def _attach_operator_networking(self) -> None:
        for operator in self.registered_operators.values():
            operator.attach_state_networking(
                self.local_state,
                self.protocol_networking,
                self.dns,
                self.deployed_graph,
            )

    def _ensure_local_state_partitions(self) -> None:
        for op_part in set(self.registered_operators.keys()):
            if op_part not in self.local_state.operator_partitions:
                self.local_state.add_new_operator_partition(op_part)

    # -----------------------
    # Handlers
    # -----------------------
    async def _handle_receive_execution_plan(self, data: bytes, _: MessageType) -> None:
        gc.disable()

        (
            self.worker_operators,
            self.dns,
            peers,
            self.operator_state_backend,
            self.deployed_graph,
            snapshot_id,
        ) = self.networking.decode_message(data)

        self._update_peers(peers)
        self._build_registered_operators(self.worker_operators)

        self.async_snapshots = AsyncSnapshotsS3(
            self.id,
            n_assigned_partitions=len(self.registered_operators),
        )

        await self._send_snap_assigned(snapshot_id=snapshot_id)

        logging.warning("Retrieving Snapshot from S3")
        (snap_data, _in_off, _out_off, epoch, t_counter) = self.async_snapshots.retrieve_snapshot(
            snapshot_id,
            self.registered_operators.keys(),
        )
        self.attach_state_to_operators_after_snapshot(snap_data)
        logging.warning("State attached to operators")
        self.function_execution_protocol = AriaProtocol(
            worker_id=self.id,
            peers=self.peers,
            dns=self.dns,
            networking=self.protocol_networking,
            registered_operators=self.registered_operators,
            topic_partitions=self.topic_partitions,
            state=self.local_state,
            snapshotting_port=self.snapshotting_port,
            epoch_counter=epoch,
            t_counter=t_counter,
        )
        logging.warning("Starting Function Execution Protocol")
        self.function_execution_protocol.start()
        self.function_execution_protocol.started.set()

    async def _handle_update_execution_plan(self, data: bytes, _: MessageType) -> None:
        # TODO add code
        logging.warning("Graph code updates not implemented yet! %s", data)

    async def _handle_init_migration(self, data: bytes, _: MessageType) -> None:
        """Phase A: rehash in background while protocol keeps running."""
        try:
            logging.warning(f"MIGRATION | PHASE A START at {time.time_ns() // 1_000_000}")
            self._pending_migration_data = data

            t_repart_start = timer()
            await self._migration_background_repartition()
            t_repart_end = timer()
            logging.warning(
                f"MIGRATION | PHASE A REHASHING DONE | took: {t_repart_end - t_repart_start}",
            )

            # Signal coordinator that rehashing is done (no counters — those come in Phase B)
            await self.networking.send_message(
                DISCOVERY_HOST,
                DISCOVERY_PORT,
                msg=b"",
                msg_type=MessageType.MigrationRepartitioningDone,
                serializer=Serializer.NONE,
            )
            logging.warning("MIGRATION | PHASE A COMPLETE — waiting for coordinator to stop protocol")
        except Exception as e:
            logging.error(f"Uncaught exception during migration Phase A: {e}")

    async def _migration_stop_protocol(self) -> None:
        await self.function_execution_protocol.wait_stopped()
        logging.warning("MIGRATION | ARIA STOPPED")

    async def _migration_decode_and_apply_plan(self, data: bytes) -> None:
        (
            self.deployed_graph,
            new_worker_operators,
            self.dns,
            peers,
            self.operator_state_backend,
        ) = self.networking.decode_message(data)

        self._update_peers(peers)
        self.worker_operators = new_worker_operators
        await asyncio.sleep(0)

    async def _migration_background_repartition(self) -> None:
        """Phase A repartitioning: rehash keys in subprocesses (no network I/O).

        Decodes the pending plan to get the new graph, but does NOT apply
        the plan (dns, peers, worker_operators) — that happens in Phase B.
        """
        # Reset state from any previous migration
        self.final_keys_to_send.clear()
        self._pending_hash_metadata.clear()
        self.completed_repartitioning = 0
        self.completed_repartitioning_event.clear()
        self.migration_error = None

        # Decode the plan to get the new graph (for partitioners) without applying it
        (
            pending_graph,
            _new_worker_operators,
            _dns,
            _peers,
            _operator_state_backend,
        ) = self.networking.decode_message(self._pending_migration_data)

        operator_partitions_to_repartition = self.local_state.get_operator_partitions_to_repartition()
        self.total_repartitioning = sum(len(parts) for parts in operator_partitions_to_repartition.values())

        if self.total_repartitioning == 0:
            self.completed_repartitioning_event.set()
            return

        loop = asyncio.get_running_loop()
        for (
            operator_name,
            operator_partitions,
        ) in operator_partitions_to_repartition.items():
            new_partitioner: HashPartitioner = pending_graph.get_operator_by_name(
                operator_name,
            ).get_partitioner()
            for operator_partition in operator_partitions:
                logging.warning(f"Sending: {operator_partition} for repartitioning")
                # Offload key list copy to a thread (Step 8)
                key_list = await loop.run_in_executor(
                    None,
                    lambda op=operator_partition: list(
                        self.local_state.get_operator_data_for_repartitioning(op).keys(),
                    ),
                )
                loop.run_in_executor(
                    self.pool,
                    self.rehash_only,
                    operator_partition,
                    new_partitioner,
                    key_list,
                    self.id,
                ).add_done_callback(self.repartitioning_callback)

        await self.completed_repartitioning_event.wait()

        if self.migration_error is not None:
            raise self.migration_error

        logging.warning("Background repartitioning completed")

    async def _migration_catchup_rehash(self) -> None:
        """Catch-up pass: rehash keys created during Phase A that weren't in the original snapshot."""
        operator_partitions_to_repartition = self.local_state.get_operator_partitions_to_repartition()
        for operator_name, operator_partitions in operator_partitions_to_repartition.items():
            new_partitioner: HashPartitioner = self.deployed_graph.get_operator_by_name(
                operator_name,
            ).get_partitioner()
            for operator_partition in operator_partitions:
                current_keys = set(
                    self.local_state.get_operator_data_for_repartitioning(operator_partition).keys(),
                )
                # Keys already in final_keys_to_send for this partition
                already_rehashed = {k for k, _ in self.final_keys_to_send.get(operator_partition, set())}
                new_keys = current_keys - already_rehashed
                if not new_keys:
                    continue
                _, previous_partition = operator_partition
                for key in new_keys:
                    new_partition: int = new_partitioner.get_partition_no_cache(key)
                    if new_partition != previous_partition:
                        self._pending_hash_metadata[(operator_name, new_partition)][key] = (
                            self.id,
                            previous_partition,
                        )
                        self.final_keys_to_send[operator_partition].add(
                            (key, new_partition),
                        )
                logging.warning(
                    f"MIGRATION | Catch-up rehashed {len(new_keys)} keys for {operator_partition}",
                )

    async def _migration_send_hash_metadata(self) -> None:
        """Send hash metadata to destination workers via NetworkingManager in parallel."""
        send_tasks = []
        for dest_partition, key_map in self._pending_hash_metadata.items():
            if not key_map:
                continue
            operator_name, partition = dest_partition
            worker_info = self.dns[operator_name][partition]
            send_tasks.append(
                self.networking.send_message(
                    worker_info[0],
                    worker_info[1],
                    msg=(dest_partition, key_map),
                    msg_type=MessageType.ReceiveMigrationHashes,
                    serializer=Serializer.MSGPACK,
                ),
            )
        if send_tasks:
            await asyncio.gather(*send_tasks)
        self._pending_hash_metadata.clear()

    async def _migration_rebuild_runtime(self) -> None:
        await self._reset_protocol_networking()

        # Rebuild operators and topic partitions for new assignment
        self._build_registered_operators(self.worker_operators)

        # Update snapshot helper
        self.async_snapshots.update_n_assigned_partitions(
            n_assigned_partitions=len(self.registered_operators),
        )

        # Snapshot id -1 indicates "migration"
        await self._send_snap_assigned(snapshot_id=-1)

        # Ensure local state has all newly assigned partitions
        self._ensure_local_state_partitions()

        # Attach networking/state to operator objects
        self._attach_operator_networking()

        # Build offsets dicts with defaults
        topic_partition_offsets = {op: self.m_input_offsets.get(op, -1) for op in self.registered_operators}
        topic_partition_output_offsets = {op: self.m_output_offsets.get(op, -1) for op in self.registered_operators}

        # Recreate protocol (restart_after_migration)
        self.function_execution_protocol = AriaProtocol(
            worker_id=self.id,
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
            t_counter=self.m_t_counter,
            restart_after_migration=True,
        )

    async def _migration_sync_and_resume(self) -> None:
        logging.warning("MIGRATION | SENDING MigrationInitDone TO COORDINATOR")
        await self.networking.send_message(
            DISCOVERY_HOST,
            DISCOVERY_PORT,
            msg=b"",
            msg_type=MessageType.MigrationInitDone,
            serializer=Serializer.NONE,
        )

        logging.warning("MIGRATION | WAITING SYNC")
        await self.migration_completed.wait()

        # Start protocol after sync
        self.function_execution_protocol.start()
        self.function_execution_protocol.started.set()

        # Reset sync events for next time
        self.migration_completed.clear()

    async def _handle_receive_migration_hashes(
        self,
        data: bytes,
        msg_type: MessageType,
    ) -> None:
        n_op, payload = self.networking.decode_message(data)
        async with self.networking_locks[msg_type]:
            self.local_state.add_remote_keys(n_op, payload)

    async def _handle_request_remote_key(
        self,
        data: bytes,
        msg_type: MessageType,
    ) -> None:
        operator_partition, key, old_partition, host, port = self.networking.decode_message(data)
        async with self.networking_locks[msg_type]:
            value = self.local_state.get_key_to_migrate(
                operator_partition,
                key,
                old_partition,
            )
            await self.networking.send_message(
                host,
                port,
                msg=(operator_partition, key, value),
                msg_type=MessageType.ReceiveRemoteKey,
                serializer=Serializer.MSGPACK,
            )

    async def _handle_receive_remote_key(
        self,
        data: bytes,
        msg_type: MessageType,
    ) -> None:
        operator_partition, key, value = self.networking.decode_message(data)
        async with self.networking_locks[msg_type]:
            self.local_state.set_data_from_migration(operator_partition, key, value)
            # Guard: the event may have been already signaled by the async
            # migration handler, or may not exist if no transaction requested it.
            op = tuple(operator_partition)
            if (
                op in self.protocol_networking.wait_remote_key_event
                and key in self.protocol_networking.wait_remote_key_event[op]
            ):
                self.protocol_networking.key_received(op, key)

    async def _handle_migration_repartitioning_done(
        self,
        _data: bytes,
        _: MessageType,
    ) -> None:
        """Phase B: protocol stops, catch-up, send hashes, rebuild, resume."""
        try:
            logging.warning(f"MIGRATION | PHASE B START at {time.time_ns() // 1_000_000}")
            phase_b_start = timer()

            # 1. Stop the protocol (coordinator already set stop_in_next_epoch)
            t_stop_start = timer()
            await self._migration_stop_protocol()
            t_stop_end = timer()
            logging.warning(
                "MIGRATION | PHASE B Protocol Stopped |"
                f" @Epoch {self.function_execution_protocol.sequencer.epoch_counter} |"
                f" took: {t_stop_end - t_stop_start}",
            )

            # 2. Decode and apply the plan (dns, peers, worker_operators)
            await self._migration_decode_and_apply_plan(self._pending_migration_data)
            self._pending_migration_data = None

            # 3. Catch-up pass: find keys created during Phase A that weren't rehashed
            t_catchup_start = timer()
            await self._migration_catchup_rehash()
            t_catchup_end = timer()
            logging.warning(
                f"MIGRATION | PHASE B Catch-up rehash | took: {t_catchup_end - t_catchup_start}",
            )

            # 4. Register keys_to_send in local_state
            self.local_state.add_keys_to_send(self.final_keys_to_send)

            # 5. Send hash metadata to destination workers via NetworkingManager (parallel)
            t_hash_send_start = timer()
            await self._migration_send_hash_metadata()
            t_hash_send_end = timer()
            logging.warning(
                f"MIGRATION | PHASE B Hash metadata sent | took: {t_hash_send_end - t_hash_send_start}",
            )

            # 6. Send MigrationInitDone with actual stop-time counters
            await self.networking.send_message(
                DISCOVERY_HOST,
                DISCOVERY_PORT,
                msg=(
                    self.function_execution_protocol.sequencer.epoch_counter,
                    self.function_execution_protocol.sequencer.t_counter,
                    self.function_execution_protocol.topic_partition_offsets,
                    self.function_execution_protocol.egress.topic_partition_output_offsets,
                ),
                msg_type=MessageType.MigrationInitDone,
                serializer=Serializer.MSGPACK,
            )

            # 7. Wait for MigrationDone (carries merged counters from coordinator)
            logging.warning("MIGRATION | PHASE B WAITING FOR MigrationDone")
            await self.migration_completed.wait()

            # 8. Rebuild runtime (now has correct merged counters from MigrationDone)
            t_deploy_start = timer()
            await self._migration_rebuild_runtime()
            t_deploy_end = timer()
            logging.warning(
                f"MIGRATION | PHASE B Runtime rebuilt | took: {t_deploy_end - t_deploy_start}",
            )

            # 9. Start protocol with background migration
            self.function_execution_protocol.start()
            self.function_execution_protocol.started.set()

            # Reset sync events for next time
            self.migration_completed.clear()

            phase_b_end = timer()
            logging.warning(
                f"Worker: {self.id} | PHASE B took: {round((phase_b_end - phase_b_start) * 1000, 4)}ms",
            )
        except Exception as e:
            logging.error(f"Uncaught exception during migration Phase B: {e}")

    async def _handle_migration_done(self, data: bytes, _: MessageType) -> None:
        (
            self.m_epoch_counter,
            self.m_t_counter,
            self.m_input_offsets,
            self.m_output_offsets,
        ) = self.networking.decode_message(data)
        self.migration_completed.set()
        await asyncio.sleep(0)

    async def _handle_init_recovery(self, data: bytes, _: MessageType) -> None:
        start_time = timer()

        (
            self.id,
            self.worker_operators,
            self.dns,
            peers,
            self.operator_state_backend,
            snapshot_id,
            self.deployed_graph,
        ) = self.networking.decode_message(data)

        if self.function_execution_protocol is not None:
            await self.function_execution_protocol.stop()
            await self.protocol_networking.close_all_connections()
            del self.function_execution_protocol
            del self.local_state
            del self.protocol_networking
            gc.collect()

            self.protocol_networking = NetworkingManager(
                self.protocol_port,
                mode=MessagingMode.PROTOCOL_PROTOCOL,
            )
            self.protocol_networking.set_worker_id(self.id)

        self._update_peers(peers)
        self.protocol_networking.set_peers(self.peers)

        self._build_registered_operators(self.worker_operators)

        self.async_snapshots = AsyncSnapshotsS3(
            self.id,
            n_assigned_partitions=len(self.registered_operators),
        )
        await self._send_snap_assigned(snapshot_id=snapshot_id)

        (snap_data, tp_offsets, tp_out_offsets, epoch, t_counter) = self.async_snapshots.retrieve_snapshot(
            snapshot_id,
            self.registered_operators.keys(),
        )

        tp_offsets = {k: v for k, v in tp_offsets.items() if k in self.registered_operators}
        tp_out_offsets = {k: v for k, v in tp_out_offsets.items() if k in self.registered_operators}

        self.attach_state_to_operators_after_snapshot(snap_data)

        request_id_to_t_id_map = await self.get_sequencer_assignments_before_failure(
            epoch,
        )

        self.function_execution_protocol = AriaProtocol(
            worker_id=self.id,
            peers=self.peers,
            dns=self.dns,
            networking=self.protocol_networking,
            registered_operators=self.registered_operators,
            topic_partitions=self.topic_partitions,
            state=self.local_state,
            snapshotting_port=self.snapshotting_port,
            topic_partition_offsets=tp_offsets,
            output_offsets=tp_out_offsets,
            epoch_counter=epoch,
            t_counter=t_counter,
            request_id_to_t_id_map=request_id_to_t_id_map,
            restart_after_recovery=True,
        )
        self.function_execution_protocol.start()

        await self.networking.send_message(
            DISCOVERY_HOST,
            DISCOVERY_PORT,
            msg=(self.id,),
            msg_type=MessageType.ReadyAfterRecovery,
            serializer=Serializer.MSGPACK,
        )

        end_time = timer()
        logging.warning(
            f"Worker: {self.id} | Recovered snapshot: {snapshot_id} "
            f"| took: {round((end_time - start_time) * 1000, 4)}ms",
        )

    async def _handle_ready_after_recovery(self, _data: bytes, _: MessageType) -> None:
        self.function_execution_protocol.started.set()
        logging.warning(
            f"Worker: {self.id} recovered and ready at : {time.time() * 1000}",
        )
        await asyncio.sleep(0)

    async def _handle_unknown(self, _data: bytes, msg_type: MessageType) -> None:
        logging.error(f"Worker Service: Non supported command message type: {msg_type}")
        await asyncio.sleep(0)

    @staticmethod
    async def get_sequencer_assignments_before_failure(
        epoch_at_snapshot: int,
    ) -> dict[bytes, int] | None:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=[KAFKA_URL],
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        tp = [TopicPartition("sequencer-wal", 0)]
        consumer.assign(tp)
        request_id_to_t_id_map: dict[bytes, int] = {}
        while True:
            try:
                await consumer.start()
            except UnknownTopicOrPartitionError, KafkaConnectionError:
                await asyncio.sleep(1)
                logging.warning(
                    f"Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second",
                )
                continue
            break
        try:
            current_offsets: dict[TopicPartition, int] = await consumer.end_offsets(tp)
            running = True
            while running:
                batch = await consumer.getmany(timeout_ms=1)
                for records in batch.values():
                    for record in records:
                        logged_sequence: dict[bytes, int] = msgpack_deserialization(
                            record.value,
                        )
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

    def attach_state_to_operators_after_snapshot(
        self,
        data: dict[OperatorPartition, KVPairs],
    ) -> None:
        operator_partitions: set[OperatorPartition] = set(
            self.registered_operators.keys(),
        )
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_partitions)
            self.local_state.set_data_from_snapshot(data)
        else:
            logging.error(
                "Invalid operator state backend type",
            )
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(
                self.local_state,
                self.protocol_networking,
                self.dns,
                self.deployed_graph,
            )

    #
    # Queue workers for backpressure
    #

    async def control_queue_worker(self) -> None:
        """
        Single worker that processes control-plane messages (coordinator → worker)
        in strict arrival order.
        """
        while True:
            message: bytes = await self.control_queue.get()
            try:
                self.aio_task_scheduler.create_task(self.worker_controller(message))
            except Exception as e:
                logging.error(f"Error while processing control-plane message: {e}")
            finally:
                self.control_queue.task_done()

    async def protocol_queue_worker(self) -> None:
        """
        Worker that pulls protocol messages from the bounded queue and
        hands them to the function_execution_protocol. This is where
        backpressure terminates for the protocol path.
        """
        while True:
            message: bytes = await self.protocol_queue.get()
            try:
                if self.function_execution_protocol is not None:
                    self.protocol_task_scheduler.create_task(
                        self.function_execution_protocol.protocol_tcp_controller(
                            message,
                        ),
                    )
                else:
                    msg_type = self.protocol_networking.get_msg_type(message)
                    logging.debug(
                        f"Dropped message_type: {msg_type} due to protocol service restart (expected behaviour)",
                    )
            except Exception as e:
                logging.error(f"Error while processing protocol message: {e}")
            finally:
                self.protocol_queue.task_done()

    async def start_tcp_service(self) -> None:
        async def request_handler(reader: StreamReader, writer: StreamWriter) -> None:
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack(">Q", data)
                    message = await reader.readexactly(size)
                    # Backpressure-aware enqueue:
                    await self.control_queue.put(message)
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                # Graceful shutdown
                pass
            except Exception as e:
                logging.error(f"Unexpected error in worker TCP request_handler: {e}")
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()

        logging.warning("Starting Worker TCP Service")
        self.pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=MIGRATION_THREADS,
            mp_context=self.mp_ctx,
            initializer=repair_stdio,
        )
        try:
            server = await asyncio.start_server(
                request_handler,
                sock=self.worker_socket,
                limit=2**32,
            )
            async with server:
                await server.serve_forever()
        finally:
            self.pool.shutdown(wait=False, cancel_futures=True)

    async def start_protocol_tcp_service(self) -> None:
        async def request_handler(reader: StreamReader, writer: StreamWriter) -> None:
            try:
                while True:
                    header = await reader.readexactly(8)
                    (size,) = struct.unpack(">Q", header)
                    message = await reader.readexactly(size)
                    # backpressure-aware enqueue
                    await self.protocol_queue.put(message)
            except asyncio.IncompleteReadError as e:
                logging.info(f"Protocol client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                # Graceful shutdown
                pass
            except Exception as e:
                logging.error(f"Unexpected error in protocol TCP request_handler: {e}")
            finally:
                logging.info("Closing protocol connection")
                writer.close()
                await writer.wait_closed()

        logging.warning("Starting Protocol TCP Service")
        server = await asyncio.start_server(
            request_handler,
            sock=self.protocol_socket,
            limit=2**32,
        )
        async with server:
            await server.serve_forever()

    async def register_to_coordinator(self) -> None:
        """
        Production-grade retry:
          - Exponential backoff with full jitter
          - Optional infinite retries (recommended for bootstrapping)
          - Cancellation-aware (won't swallow CancelledError)
          - Caps backoff to avoid unbounded sleeps
          - Logs with attempt counters and next delay
        """

        # ---- policy knobs  ----
        infinite_retries: bool = True  # for worker bootstrapping this is often correct
        max_retries: int = 30  # used only if infinite_retries=False
        base_delay_s: float = 0.5  # initial backoff
        max_delay_s: float = 30.0  # cap backoff
        timeout_s: float | None = 5.0  # per-attempt timeout; set None to disable
        # -----------------------

        attempt = 0
        while True:
            attempt += 1
            try:
                coro = self.networking.send_message_request_response(
                    DISCOVERY_HOST,
                    DISCOVERY_PORT,
                    msg=(self.networking.host_name, self.server_port, self.protocol_port),
                    msg_type=MessageType.RegisterWorker,
                    serializer=Serializer.MSGPACK,
                )

                if timeout_s is not None:
                    self.id = await asyncio.wait_for(coro, timeout=timeout_s)
                else:
                    self.id = await coro

                logging.warning(f"Worker id received from coordinator: {self.id}")
                self.protocol_networking.set_worker_id(self.id)
                self.networking.set_worker_id(self.id)
            except asyncio.CancelledError:
                # Never swallow cancellation; let shutdowns be fast/clean.
                raise
            except Exception as e:
                # Stop if finite retries and we've exhausted them.
                if not infinite_retries and attempt >= max_retries:
                    logging.exception(f"Failed to register to coordinator after {attempt} attempts.")
                    raise

                # Exponential backoff with *full jitter*:
                cap = min(max_delay_s, base_delay_s * (2 ** (attempt - 1)))
                delay = random.uniform(0.0, cap)  # noqa: S311

                # Use exception() occasionally to keep a stacktrace in logs without spamming too hard.
                # (You can tune this: e.g., every N attempts.)
                if attempt == 1 or attempt % 10 == 0:
                    logging.exception(
                        f"Register attempt {attempt} failed; retrying in {delay:.2f}s (cap={cap:.2f}s).",
                        exc_info=e,
                    )
                else:
                    logging.warning(
                        f"Register attempt {attempt} failed ({type(e).__name__}: {e}); "
                        f"retrying in {delay:.2f}s (cap={cap:.2f}s)."
                    )

                await asyncio.sleep(delay)
            else:
                return

    @staticmethod
    async def heartbeat_coroutine(worker_id: int, worker_pid: int) -> None:
        networking = NetworkingManager(None, size=1, mode=MessagingMode.HEARTBEAT)
        monitor: ContainerMonitor = ContainerMonitor(worker_pid)
        sleep_in_seconds = HEARTBEAT_INTERVAL / 1000
        while True:
            await asyncio.sleep(sleep_in_seconds)
            cpu_perc, mem_util, rx_net, tx_net = monitor.get_stats()
            await networking.send_message(
                DISCOVERY_HOST,
                DISCOVERY_PORT,
                msg=(worker_id, cpu_perc, mem_util, rx_net, tx_net),
                msg_type=MessageType.Heartbeat,
                serializer=Serializer.MSGPACK,
            )

    @staticmethod
    def heartbeat_entry(worker_id: int, worker_pid: int) -> None:
        repair_stdio()
        uvloop.run(Worker.heartbeat_coroutine(worker_id, worker_pid))

    @staticmethod
    def snapshot_entry(port: int, worker_id: int) -> None:
        repair_stdio()
        proc = AsyncSnapshottingProcess(port, worker_id)
        proc.start_snapshot_process()

    async def main(self) -> None:
        try:
            await self.register_to_coordinator()
            worker_pid: int = os.getpid()
            self.heartbeat_proc = self.mp_ctx.Process(
                target=self.heartbeat_entry,
                args=(self.id, worker_pid),
            )
            self.async_snapshotting_proc = self.mp_ctx.Process(
                target=self.snapshot_entry,
                args=(self.snapshotting_port, self.id),
            )
            self.async_snapshotting_proc.start()
            logging.warning("Worker snapshotting process online")
            self.heartbeat_proc.start()
            logging.warning("Worker heartbeat process online")

            # Start queue workers:
            # - control_queue: single worker for ordered control-plane traffic
            # - protocol_queue: multiple workers for data-plane / protocol processing
            self.control_queue_worker_task = asyncio.create_task(
                self.control_queue_worker(),
            )
            protocol_queue_worker_tasks = set()
            for _ in range(PROTOCOL_WORKERS):
                protocol_queue_worker_tasks.add(
                    asyncio.create_task(self.protocol_queue_worker()),
                )

            # Start TCP services
            self.protocol_task = asyncio.create_task(self.start_protocol_tcp_service())
            await self.start_tcp_service()

            self.heartbeat_proc.join()
            self.async_snapshotting_proc.join()
        finally:
            await self.protocol_networking.close_all_connections()
            await self.networking.close_all_connections()

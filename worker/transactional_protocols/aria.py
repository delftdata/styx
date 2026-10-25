import asyncio
import contextlib
import itertools
import os
import random
import time
from timeit import default_timer as timer
from typing import TYPE_CHECKING

from setuptools._distutils.util import strtobool
from styx.common.base_protocol import BaseTransactionalProtocol
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.run_func_payload import RunFuncPayload, SequencedItem
from styx.common.serialization import Serializer, msgpack_serialization
from styx.common.tcp_networking import NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress
from worker.ingress.styx_kafka_ingress import StyxKafkaIngress
from worker.operator_state.aria.conflict_detection_types import (
    AriaConflictDetectionType,
)
from worker.sequencer.sequencer import Sequencer

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from aiokafka import AIOKafkaConsumer, TopicPartition
    from styx.common.operator import Operator
    from styx.common.types import OperatorPartition

    from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
    from worker.operator_state.stateless import Stateless

DISCOVERY_HOST: str = os.environ["DISCOVERY_HOST"]
DISCOVERY_PORT: int = int(os.environ["DISCOVERY_PORT"])

CONFLICT_DETECTION_METHOD: AriaConflictDetectionType = AriaConflictDetectionType(
    int(os.getenv("CONFLICT_DETECTION_METHOD", "0")),
)
# if more than 10% aborts use fallback strategy
FALLBACK_STRATEGY_PERCENTAGE: float = float(
    os.getenv("FALLBACK_STRATEGY_PERCENTAGE", "-0.1"),
)
SNAPSHOTTING_THREADS: int = int(os.getenv("SNAPSHOTTING_THREADS", "4"))
SEQUENCE_MAX_SIZE: int = int(os.getenv("SEQUENCE_MAX_SIZE", "1_000"))
KAFKA_URL: str = os.environ["KAFKA_URL"]
USE_ASYNC_MIGRATION: bool = bool(strtobool(os.getenv("USE_ASYNC_MIGRATION", "true")))
# Small batches keep each sender step short, so epochs are not held up behind it.
ASYNC_MIGRATION_BATCH_SIZE: int = int(os.getenv("ASYNC_MIGRATION_BATCH_SIZE", "500"))
# Keys moved per epoch. Everything moved in an epoch is serialized into that
# epoch's snapshot delta on its critical path, so the transfer is spread out.
ASYNC_MIGRATION_KEYS_PER_EPOCH: int = int(os.getenv("ASYNC_MIGRATION_KEYS_PER_EPOCH", "3_000"))
# With no epoch running, the budget refills after this long instead.
ASYNC_MIGRATION_IDLE_REFILL_S: float = 0.01
# An epoch wait (transactions, chains, a barrier) that lasts this long is logged
# with what the worker is still waiting for. A stalled epoch stalls every worker
# at the next barrier, and nothing else would be logged.
EPOCH_STALL_WARN_S: float = float(os.getenv("EPOCH_STALL_WARN_S", "10"))


class AriaProtocol(BaseTransactionalProtocol):
    def __init__(  # noqa: PLR0913
        self,
        worker_id: int,
        peers: dict[int, tuple[str, int, int]],
        dns: dict[str, dict[int, tuple[str, int, int]]],
        networking: NetworkingManager,
        registered_operators: dict[OperatorPartition, Operator],
        topic_partitions: list[TopicPartition],
        state: InMemoryOperatorState | Stateless,
        snapshotting_port: int,
        topic_partition_offsets: dict[OperatorPartition, int] | None = None,
        output_offsets: dict[OperatorPartition, int] | None = None,
        epoch_counter: int = 0,
        t_counter: int = 0,
        request_id_to_t_id_map: dict[bytes, int] | None = None,
        restart_after_recovery: bool = False,
        restart_after_migration: bool = False,
        *,
        dedup_output_offsets: dict[OperatorPartition, int] | None = None,
        egress: StyxKafkaBatchEgress | None = None,
        kafka_consumer: AIOKafkaConsumer | None = None,
        incarnation: int = 0,
    ) -> None:
        if topic_partition_offsets is None:
            topic_partition_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}
        if output_offsets is None:
            output_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}

        self.id: int = worker_id
        # Migration messages from another incarnation (before a recovery) are dropped.
        self.incarnation: int = incarnation

        self.topic_partitions = topic_partitions
        self.networking = networking
        self.snapshotting_networking_manager = NetworkingManager(None, size=1)

        self.local_state: InMemoryOperatorState | Stateless = state
        self.aio_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()
        self.background_functions: AIOTaskScheduler = AIOTaskScheduler()

        # worker_id: host, port
        self.peers: dict[int, tuple[str, int, int]] = peers
        self.dns: dict[str, dict[int, tuple[str, int, int]]] = dns
        self.topic_partition_offsets: dict[OperatorPartition, int] = topic_partition_offsets
        # worker_id: set of aborted t_ids
        self.concurrency_aborts_everywhere: set[int] = set()
        self.t_ids_to_reschedule: set[int] = set()
        self.fallback_rescheduled_t_ids: set[int] = set()

        # ready_to_commit_events -> worker_id: Event that appears if the peer is ready to commit
        self.ready_to_reorder_events: dict[int, asyncio.Event] = {peer_id: asyncio.Event() for peer_id in self.peers}

        # FALLBACK LOCKING
        # t_id: its lock
        self.fallback_locking_event_map: dict[int, asyncio.Event] = {}
        self.fallback_locking_event_map_lock: asyncio.Lock = asyncio.Lock()
        # t_id: the t_ids it depends on
        self.waiting_on_transactions: dict[int, set[int]] = {}

        self.registered_operators: dict[OperatorPartition, Operator] = registered_operators

        self.sequencer = Sequencer(
            SEQUENCE_MAX_SIZE,
            t_counter=t_counter,
            epoch_counter=epoch_counter,
        )
        self.sequencer.set_sequencer_id(list(self.peers.keys()), self.id)
        self.sequencer.set_wal_values_after_recovery(request_id_to_t_id_map)

        self.ingress: StyxKafkaIngress = StyxKafkaIngress(
            networking=self.networking,
            sequencer=self.sequencer,
            state=self.local_state,
            registered_operators=self.registered_operators,
            worker_id=self.id,
            kafka_url=KAFKA_URL,
            sequence_max_size=SEQUENCE_MAX_SIZE,
            epoch_interval_ms=100,
        )
        if kafka_consumer is not None:
            # Rebuilt after a clean migration stop: keep the connected consumer.
            self.ingress.adopt_consumer(kafka_consumer)

        # Responses may duplicate ones sent before a recovery; scan for them first.
        # After a clean migration stop nothing is replayed, so there is nothing to scan.
        dedup_on_start = restart_after_recovery or dedup_output_offsets is not None
        if egress is None:
            self.egress: StyxKafkaBatchEgress = StyxKafkaBatchEgress(
                output_offsets,
                dedup_on_start,
                dedup_output_offsets=dedup_output_offsets,
            )
        else:
            # Rebuilt after a clean migration stop: keep the running Kafka producer.
            egress.prepare_for_new_protocol(output_offsets, dedup_on_start, dedup_output_offsets)
            self.egress = egress
        # Primary task used for processing
        self.function_scheduler_task: asyncio.Task | None = None
        self.communication_task: asyncio.Task | None = None
        self.migration_sender_task: asyncio.Task | None = None
        self._stall_watchdog_task: asyncio.Task | None = None
        # What the epoch is waiting on, and since when (for _stall_watchdog).
        self._waiting_on: str | None = None
        self._waiting_since: float = 0.0

        self.max_t_counter: int = -1
        self.total_processed_seq_size: int = -1

        self.sync_workers_event: dict[MessageType, asyncio.Event] = {
            MessageType.AriaProcessingDone: asyncio.Event(),
            MessageType.SyncCleanup: asyncio.Event(),
            MessageType.AriaFallbackStart: asyncio.Event(),
            MessageType.AriaFallbackDone: asyncio.Event(),
            MessageType.AriaCommit: asyncio.Event(),
            MessageType.DeterministicReordering: asyncio.Event(),
        }

        self.remote_wants_to_proceed: bool = False
        self.currently_processing: bool = False

        self.started = asyncio.Event()
        self.wait_responses_to_be_sent = asyncio.Event()

        self.running: bool = True
        self._stopping: bool = False
        self.stopped: asyncio.Event = asyncio.Event()
        self.snapshot_marker_received: bool = False
        self.snapshotting_port: int = snapshotting_port

        self.migrating_state: bool = restart_after_migration
        self._init_async_migration_state()

        # Requests consumed here whose key now lives on a peer run there as proxied
        # calls: t_id -> the future their ProxyResult resolves.
        self._proxy_results: dict[int, asyncio.Future] = {}
        # Proxied transactions whose read/write set changed in their fallback run.
        self._proxy_rw_changed: set[int] = set()
        # Proxied calls sent this epoch, logged when there were any.
        self._proxied_calls: int = 0

        self.protocol_handlers_map: dict[MessageType, Callable[[bytes], Awaitable[None]]] = {
            MessageType.RunFunRemote: self._handle_run_fun_remote,
            MessageType.RunFunRemoteBatch: self._handle_run_fun_remote_batch,
            MessageType.RunFunProxy: self._handle_run_fun_proxy,
            MessageType.ProxyResult: self._handle_proxy_result,
            MessageType.RunFunRqRsRemote: self._handle_deprecated_rqrs,
            MessageType.AriaCommit: self._handle_aria_commit,
            MessageType.AriaFallbackDone: self._handle_sync_event_only,
            MessageType.AriaFallbackStart: self._handle_sync_event_only,
            MessageType.SyncCleanup: self._handle_sync_cleanup,
            MessageType.AriaProcessingDone: self._handle_aria_processing_done,
            MessageType.Ack: self._handle_ack,
            MessageType.AckBatch: self._handle_ack_batch,
            MessageType.ChainAbort: self._handle_chain_abort,
            MessageType.ResponseToRoot: self._handle_response_to_root,
            MessageType.Unlock: self._handle_unlock,
            MessageType.DeterministicReordering: self._handle_deterministic_reordering,
            MessageType.RemoteWantsToProceed: self._handle_remote_wants_to_proceed,
            MessageType.AsyncMigration: self._handle_async_migration,
            MessageType.AsyncMigrationAck: self._handle_async_migration_ack,
        }

    def _init_async_migration_state(self) -> None:
        # A migration batch must leave its source and land at its destination in
        # the same epoch, otherwise a snapshot cut can record the source-side
        # tombstone without the destination-side insert. The epoch closes this
        # gate and waits for every unacknowledged batch before its barriers.
        self._migration_gate: asyncio.Event = asyncio.Event()
        self._migration_gate.set()
        self._migration_all_acked: asyncio.Event = asyncio.Event()
        self._migration_all_acked.set()
        self._migration_unacked: set[int] = set()
        self._migration_batch_ids = itertools.count()
        # Distinguishes acks for this protocol's batches from late acks for a
        # previous protocol instance (recovery or migration rebuilds the protocol).
        self._migration_nonce: int = random.getrandbits(62)
        self._migration_budget: int = ASYNC_MIGRATION_KEYS_PER_EPOCH
        self._migration_epoch_ended: asyncio.Event = asyncio.Event()

    async def wait_stopped(self) -> None:
        await self.stopped.wait()

    async def stop(self, *, keep_kafka_clients: bool = False) -> None:
        """Stop the protocol. keep_kafka_clients flushes the egress but leaves its
        producer and the ingress consumer connected, so the protocol rebuilt after
        a migration can reuse them."""
        if self._stopping:
            # Another caller is already tearing down — just wait for completion.
            await self.stopped.wait()
            if not keep_kafka_clients:
                # A graceful (migration) stop may have left them connected.
                await self.ingress.close_consumer()
                await self.egress.stop()
            return
        self._stopping = True
        await self.ingress.stop(keep_consumer=keep_kafka_clients)
        if keep_kafka_clients:
            await self.egress.flush()
        else:
            await self.egress.stop()
        await self.aio_task_scheduler.close()
        await self.background_functions.close()
        await self.snapshotting_networking_manager.close_all_connections()
        self.function_scheduler_task.cancel()
        self.communication_task.cancel()
        if self.migration_sender_task is not None:
            self.migration_sender_task.cancel()
        if self._stall_watchdog_task is not None:
            self._stall_watchdog_task.cancel()
        try:
            await self.function_scheduler_task
            await self.communication_task
            if self.migration_sender_task is not None:
                await self.migration_sender_task
        except asyncio.CancelledError:
            logging.warning("Protocol coroutines stopped")
        logging.info(f"Active tasks: {asyncio.all_tasks()}")
        self.stopped.set()
        logging.warning(f"Aria protocol stopped at: {self.topic_partition_offsets}")

    def start(self) -> None:
        self.function_scheduler_task = asyncio.create_task(self.function_scheduler())
        self.communication_task = asyncio.create_task(self.communication_protocol())
        if self.migrating_state and USE_ASYNC_MIGRATION:
            self.migration_sender_task = asyncio.create_task(self._continuous_migration_sender())
        self._stall_watchdog_task = asyncio.create_task(self._stall_watchdog())
        logging.warning(
            f"Aria protocol started with operator partitions: {list(self.registered_operators.keys())}",
        )

    def _begin_wait(self, what: str) -> None:
        self._waiting_on = what
        self._waiting_since = timer()

    def _end_wait(self) -> None:
        self._waiting_on = None

    async def _stall_watchdog(self) -> None:
        while True:
            await asyncio.sleep(EPOCH_STALL_WARN_S)
            what = self._waiting_on
            if what is None:
                continue
            waited = timer() - self._waiting_since
            if waited < EPOCH_STALL_WARN_S:
                continue
            unacked = [t_id for t_id, ack in self.networking.waited_ack_events.items() if not ack.is_set()]
            proxied = list(self._proxy_results)
            logging.warning(
                f"{self.id} ||| STALL @epoch {self.sequencer.epoch_counter}: waiting on {what} for {waited:.0f}s"
                f" | unfinished chains: {len(unacked)} {unacked[:10]}"
                f" | proxied calls pending: {len(proxied)} {proxied[:10]}",
            )

    async def run_function(
        self,
        t_id: int,
        payload: RunFuncPayload,
        fallback_mode: bool = False,
    ) -> bool:
        # logging.info(f"Running function: {payload.function_name} with T_ID {t_id} with params {payload.params}"
        #              f" and ack payload {payload.ack_payload}")

        operator_partition = self.registered_operators.get((payload.operator_name, payload.partition))
        if operator_partition is None and payload.ack_payload is None:
            # A request consumed here for a key that migrated to a peer's partition.
            return await self._run_proxied(t_id, payload, fallback_mode)

        success: bool = await operator_partition.run_function(
            payload.key,
            t_id,
            payload.request_id,
            payload.function_name,
            payload.partition,
            payload.ack_payload,
            fallback_mode,
            payload.params,
            self,
        )
        return success

    async def take_snapshot(self) -> None:
        if self.snapshot_marker_received:
            logging.warning(
                f"ARIA | Snapshot marker received @epoch: {self.sequencer.epoch_counter}",
            )
            await self.snapshotting_networking_manager.send_message(
                self.networking.host_name,
                self.snapshotting_port,
                msg=(
                    self.topic_partition_offsets,
                    self.egress.topic_partition_output_offsets,
                    self.sequencer.epoch_counter,
                    self.sequencer.t_counter,
                ),
                msg_type=MessageType.SnapTakeSnapshot,
                serializer=Serializer.MSGPACK,
            )
            self.snapshot_marker_received = False

    async def communication_protocol(self) -> None:
        await self.ingress.start(self.topic_partitions, self.topic_partition_offsets)
        logging.warning("Ingress started")
        await self.egress.start(self.id)
        logging.warning("Egress started")
        await self.started.wait()

    async def protocol_tcp_controller(self, data: bytes) -> None:
        """Legacy entry point retained for symmetry with the rest of the codebase.

        The hot path now dispatches inline in `worker_service.protocol_queue_worker`
        (one fewer coroutine frame per message). Kept here so direct callers
        (tests, recovery, snapshotting) still work without changes.
        """
        message_type: MessageType = self.networking.get_msg_type(data)
        handler = self.protocol_handlers_map.get(message_type)
        if handler is None:
            logging.error(
                f"Aria protocol: Non supported command message type: {message_type}",
            )
            return
        await handler(data)

    # -------------------
    # Handlers
    # -------------------
    async def _handle_run_fun_remote(self, data: bytes) -> None:
        # Lock-free: no awaits in this handler; single-threaded asyncio gives atomicity.
        logging.debug("CALLED RUN FUN FROM PEER")
        (
            t_id,
            request_id,
            operator_name,
            function_name,
            key,
            partition,
            fallback_enabled,
            params,
            ack,
        ) = self.networking.decode_message(data)

        payload = RunFuncPayload(
            request_id=request_id,
            key=key,
            operator_name=operator_name,
            partition=partition,
            function_name=function_name,
            params=params,
            ack_payload=ack,
        )

        if fallback_enabled:
            # Bypass the semaphore for fallback chain participants: their
            # dominant time is `await fallback_locking_event_map[d].wait()`,
            # and those events fire only when other participants (queued
            # behind the very same semaphore) get to run. Holding a slot
            # while sleeping causes a cluster-wide deadlock under load.
            self.background_functions.create_unbounded_task(
                self.run_fallback_function(t_id, payload, internal=True),
            )
            return

        self.background_functions.create_task(self.run_function(t_id, payload))

    async def _handle_run_fun_remote_batch(self, data: bytes) -> None:
        # Lock-free: no awaits. Decodes a batch of remote-call payloads and
        # schedules each as a task, mirroring _handle_run_fun_remote per entry.
        batch = self.networking.decode_message(data)
        for entry in batch:
            (
                t_id,
                request_id,
                operator_name,
                function_name,
                key,
                partition,
                fallback_enabled,
                params,
                ack,
            ) = entry
            payload = RunFuncPayload(
                request_id=request_id,
                key=key,
                operator_name=operator_name,
                partition=partition,
                function_name=function_name,
                params=params,
                ack_payload=ack,
            )
            if fallback_enabled:
                self.background_functions.create_unbounded_task(
                    self.run_fallback_function(t_id, payload, internal=True),
                )
            else:
                self.background_functions.create_task(self.run_function(t_id, payload))

    async def _run_proxied(self, t_id: int, payload: RunFuncPayload, fallback_mode: bool) -> bool:
        """Run a request consumed here on the peer whose partition now holds its key,
        and take its outcome as this root's own.

        The request stays in this worker's epoch, so its ingress offset advances only
        here and in consumption order: every snapshot or migration stop cuts each
        ingress partition at one offset. (Forwarding it into the peer's epoch let two
        workers advance one partition's offset, each by max, past requests the other
        had not run yet.)
        """
        self._proxied_calls += 1
        host, _, protocol_port = self.dns[payload.operator_name][payload.partition]
        result: asyncio.Future = asyncio.get_running_loop().create_future()
        self._proxy_results[t_id] = result
        await self.networking.send_message(
            host,
            protocol_port,
            msg=(
                t_id,
                self.id,
                payload.request_id,
                payload.operator_name,
                payload.function_name,
                payload.key,
                payload.partition,
                fallback_mode,
                payload.params,
                self.incarnation,
            ),
            msg_type=MessageType.RunFunProxy,
            serializer=Serializer.MSGPACK,
        )
        response, abort, participants, rw_changed = await result
        self.networking.add_chain_participants(t_id, participants)
        if abort is not None:
            self.networking.abort_chain(t_id, abort)
            return False
        if rw_changed:
            self._proxy_rw_changed.add(t_id)
        if response is not None:
            self.networking.add_response(t_id, response)
        return True

    async def _handle_run_fun_proxy(self, data: bytes) -> None:
        # Lock-free: no awaits.
        (
            t_id,
            root_id,
            request_id,
            operator_name,
            function_name,
            key,
            partition,
            fallback_mode,
            params,
            incarnation,
        ) = self.networking.decode_message(data)
        if incarnation != self.incarnation:
            # Sent before a recovery; the root replays it with the same t_id.
            logging.warning(f"Dropping proxied call {t_id} from W{root_id} of incarnation {incarnation}")
            return
        payload = RunFuncPayload(
            request_id=request_id,
            key=key,
            operator_name=operator_name,
            partition=partition,
            function_name=function_name,
            params=params,
        )
        # Unbounded: the call holds its task until its whole chain is done, and
        # that chain may need bounded slots on this worker.
        self.background_functions.create_unbounded_task(
            self._run_proxied_call(t_id, root_id, payload, fallback_mode),
        )

    async def _run_proxied_call(
        self,
        t_id: int,
        root_id: int,
        payload: RunFuncPayload,
        fallback_mode: bool,
    ) -> None:
        """Run a peer's request here on its behalf. The chain acknowledges, answers
        and aborts to this worker, and one ProxyResult then carries the outcome to
        the root, so the root cannot commit before it has the response."""
        networking = self.networking
        networking.prepare_function_chain(t_id)
        payload.ack_payload = (networking.host_name, networking.host_port, t_id, "1.0", [])
        try:
            if fallback_mode:
                await self.run_fallback_function(t_id, payload, internal=True)
            else:
                await self.run_function(t_id, payload)
            await networking.waited_ack_events[t_id].wait()
        except Exception as e:
            logging.exception(f"Proxied call {t_id} from W{root_id} failed")
            networking.abort_chain(t_id, str(e))
        host, _, protocol_port = self.peers[root_id]
        await networking.send_message(
            host,
            protocol_port,
            msg=(
                t_id,
                networking.client_responses.get(t_id),
                networking.aborted_events.get(t_id),
                [self.id, *networking.chain_participants.get(t_id, ())],
                fallback_mode and self.local_state.has_fallback_rw_set_changed(t_id),
                self.incarnation,
            ),
            msg_type=MessageType.ProxyResult,
            serializer=Serializer.MSGPACK,
        )

    async def _handle_proxy_result(self, data: bytes) -> None:
        # Lock-free: no awaits.
        t_id, response, abort, participants, rw_changed, incarnation = self.networking.decode_message(data)
        if incarnation != self.incarnation:
            # From before a recovery: a replayed request may have the same t_id.
            logging.warning(f"Dropping ProxyResult for T_ID {t_id} of incarnation {incarnation}")
            return
        result = self._proxy_results.pop(t_id, None)
        if result is None or result.done():
            # Its call would never complete; worth seeing even at the default level.
            logging.warning(f"Dropping ProxyResult for T_ID {t_id} with no pending proxied call")
            return
        result.set_result((response, abort, participants, rw_changed))

    def _response_partition(self, payload: RunFuncPayload) -> int:
        """Output partition for a root's response: its own partition, or for a
        proxied request (partition on a peer) the ingress partition it came from,
        which this worker owns."""
        if (payload.operator_name, payload.partition) in self.registered_operators:
            return payload.partition
        return payload.kafka_ingress_partition

    async def _handle_deprecated_rqrs(self, _: bytes) -> None:
        logging.error("REQUEST RESPONSE HAS BEEN DEPRECATED")

    async def _handle_aria_commit(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.AriaCommit
        (
            self.concurrency_aborts_everywhere,
            self.total_processed_seq_size,
            self.max_t_counter,
            self.snapshot_marker_received,
        ) = self.networking.decode_message(data)
        self.sync_workers_event[mt].set()

    async def _handle_sync_event_only(self, data: bytes) -> None:
        # Used for AriaFallbackDone / AriaFallbackStart. Lock-free: no awaits.
        mt: MessageType = self.networking.get_msg_type(data)
        self.sync_workers_event[mt].set()

    async def _handle_sync_cleanup(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.SyncCleanup
        (stop_gracefully,) = self.networking.decode_message(data)
        if stop_gracefully:
            logging.warning(f"Worker {self.id} | SyncCleanup received stop_gracefully=True")
            self.running = False
        self.sync_workers_event[mt].set()

    async def _handle_aria_processing_done(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.AriaProcessingDone
        (self.networking.logic_aborts_everywhere,) = self.networking.decode_message(data)
        self.sync_workers_event[mt].set()

    async def _handle_ack(self, data: bytes) -> None:
        # Lock-free: no awaits.
        ack_id, fraction_str, chain_participants = self.networking.decode_message(data)
        self.networking.add_ack_fraction_str(
            ack_id,
            fraction_str,
            chain_participants,
        )

    async def _handle_ack_batch(self, data: bytes) -> None:
        # Lock-free: no awaits. Decodes a coalesced batch from a peer and
        # applies each entry through the same single-entry codepath.
        batch = self.networking.decode_message(data)
        add = self.networking.add_ack_fraction_str
        for ack_id, fraction_str, chain_participants in batch:
            add(ack_id, fraction_str, chain_participants)

    async def _handle_chain_abort(self, data: bytes) -> None:
        # Lock-free: no awaits.
        ack_id, exception_str = self.networking.decode_message(data)
        self.networking.abort_chain(ack_id, exception_str)

    async def _handle_response_to_root(self, data: bytes) -> None:
        # Lock-free: no awaits.
        ack_id, resp = self.networking.decode_message(data)
        self.networking.add_response(ack_id, resp)

    async def _handle_unlock(self, data: bytes) -> None:
        # fallback phase
        # here we handle the logic to unlock locks held by the provided distributed transaction
        # The outer lock is unnecessary: commit_fallback_transaction is sync, and
        # unlock_tid acquires its own fallback_locking_event_map_lock around the await.
        t_id, success = self.networking.decode_message(data)
        if success:
            # commit changes
            self.local_state.commit_fallback_transaction(t_id)
        # unlock
        await self.unlock_tid(t_id)

    async def _handle_deterministic_reordering(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.DeterministicReordering
        _, global_read_reservations, global_write_set, global_read_set = self.networking.decode_message(data)
        self.local_state.set_global_read_write_sets(
            global_read_reservations,
            global_write_set,
            global_read_set,
        )
        self.sync_workers_event[mt].set()

    async def _handle_remote_wants_to_proceed(self, _: bytes) -> None:
        if not self.currently_processing:
            self.remote_wants_to_proceed = True
            self.ingress.messages_available.set()

    async def _handle_async_migration(self, data: bytes) -> None:
        operator_partition, batch, sender_id, nonce, batch_id, incarnation = self.networking.decode_message(data)
        if incarnation != self.incarnation:
            # Sent by a protocol from before a recovery: its keys are not ours to take.
            logging.warning(
                f"MIGRATION | Dropping batch for {operator_partition} from W{sender_id} "
                f"of incarnation {incarnation} (now {self.incarnation})",
            )
            return
        logging.debug(f"MIGRATION | Received migration batch for: {operator_partition}")

        self._apply_migration_batch(operator_partition, batch)

        # Acknowledge only after the batch is applied (and so in this epoch's delta).
        if sender_id == self.id:
            self._ack_migration_batch(nonce, batch_id)
            return
        host, _, protocol_port = self.peers[sender_id]
        await self.networking.send_message(
            host,
            protocol_port,
            msg=(nonce, batch_id),
            msg_type=MessageType.AsyncMigrationAck,
            serializer=Serializer.MSGPACK,
        )

    def _apply_migration_batch(self, operator_partition: OperatorPartition, batch: dict) -> None:
        op = tuple(operator_partition)
        self.local_state.set_batch_data_from_migration(op, batch)
        # Unblock any transactions waiting for these keys
        waiting = self.networking.wait_remote_key_event.get(op)
        if waiting:
            for key in batch:
                if key in waiting:
                    waiting[key].set()

    async def _handle_async_migration_ack(self, data: bytes) -> None:
        # Lock-free: no awaits.
        nonce, batch_id = self.networking.decode_message(data)
        self._ack_migration_batch(nonce, batch_id)

    def _ack_migration_batch(self, nonce: int, batch_id: int) -> None:
        if nonce != self._migration_nonce:
            # Ack for a batch sent by a protocol instance that no longer exists.
            return
        self._migration_unacked.discard(batch_id)
        if not self._migration_unacked:
            self._migration_all_acked.set()

    async def _drain_migration_batches(self) -> None:
        """Stop the async sender and wait until every batch it sent has been applied
        at its destination, so the coming snapshot cut sees each migrated key on
        exactly one side."""
        self._migration_gate.clear()
        self._begin_wait("migration batch acks")
        await self._migration_all_acked.wait()
        self._end_wait()

    async def _write_to_wal(self, sequence: list[SequencedItem]) -> tuple[float, float]:
        start_wal = timer()
        sequence_to_log = msgpack_serialization(
            {seq_item.payload.request_id: seq_item.t_id for seq_item in sequence},
        )
        await self.egress.send_message_to_topic(
            key=msgpack_serialization(self.sequencer.epoch_counter),
            message=sequence_to_log,
            topic="sequencer-wal",
        )
        end_wal = timer()
        logging.debug(
            f"Write to WAL successful at epoch: {self.sequencer.epoch_counter}",
        )
        return start_wal, end_wal

    async def _send_migration_batch(
        self,
        batch: dict,
    ) -> None:
        """Send a migration batch to destination workers in parallel.

        A batch for a partition on this worker is applied in place: nothing is in
        flight, so it needs no acknowledgement. A remote batch stays unacknowledged
        until the destination has applied it."""
        send_tasks = []
        for operator_partition, k_v_pairs in batch.items():
            operator_name, partition = operator_partition
            if (operator_name, partition) in self.registered_operators:
                self._apply_migration_batch(operator_partition, k_v_pairs)
                continue
            worker = self.dns[operator_name][partition]
            # Register before the first await so a concurrent drain waits for it.
            batch_id = next(self._migration_batch_ids)
            self._migration_unacked.add(batch_id)
            self._migration_all_acked.clear()
            send_tasks.append(
                self.networking.send_message(
                    worker[0],
                    worker[2],
                    msg=(operator_partition, k_v_pairs, self.id, self._migration_nonce, batch_id, self.incarnation),
                    msg_type=MessageType.AsyncMigration,
                    serializer=Serializer.MSGPACK,
                ),
            )
        if send_tasks:
            await asyncio.gather(*send_tasks)

    async def _continuous_migration_sender(self) -> None:
        """Background coroutine that continuously sends migration batches
        alongside epoch processing, completing migration faster.

        It pauses while an epoch is between its drain (before the processing-done
        barrier) and its cleanup barrier, so no batch is in flight across a
        snapshot cut.

        Completion is based only on keys_remaining_to_send() — the coordinator's
        global barrier (all workers report MigrationDone) ensures all data is
        fully transferred.  We do NOT check keys_remaining_to_remote() because
        hash metadata from other workers may not have arrived yet.
        """
        # Peers may still be loading state until the protocol is started
        # (ReadyAfterRecovery, or the rebuild after MigrationDone).
        await self.started.wait()
        logging.warning("MIGRATION | Continuous migration sender started")
        try:
            while self.migrating_state:
                if not self._migration_gate.is_set():
                    await self._migration_gate.wait()
                    continue
                if not self.local_state.has_keys_to_send():
                    if self.local_state.keys_remaining_to_send() == 0:
                        # Report done only once every batch has landed. The last one
                        # landed in this epoch or earlier (a batch never crosses an
                        # epoch's drain), so snapshots cut after this epoch are complete.
                        await self._migration_all_acked.wait()
                        self.migrating_state = False
                        await self.networking.send_message(
                            DISCOVERY_HOST,
                            DISCOVERY_PORT + 1,
                            msg=(self.sequencer.epoch_counter,),
                            msg_type=MessageType.MigrationDone,
                            serializer=Serializer.MSGPACK,
                        )
                        logging.warning(
                            f"MIGRATION_FINISHED at epoch: {self.sequencer.epoch_counter}"
                            f" at time: {time.time_ns() // 1_000_000}",
                        )
                        return
                    # keys_to_send dict is empty but keys_remaining_to_send > 0
                    # shouldn't normally happen, but yield and retry
                    await asyncio.sleep(0.01)
                    continue

                if self._migration_budget <= 0:
                    await self._wait_for_migration_budget()
                    continue

                batch = self.local_state.get_async_migrate_batch(
                    min(ASYNC_MIGRATION_BATCH_SIZE, self._migration_budget),
                )
                self._migration_budget -= sum(len(kv) for kv in batch.values())
                await self._send_migration_batch(batch)
                # Yield to event loop between batches
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            logging.warning("MIGRATION | Continuous migration sender cancelled")
            raise

    async def _wait_for_migration_budget(self) -> None:
        """Wait for the next epoch to end, or a short while if none is running."""
        self._migration_epoch_ended.clear()
        if self.currently_processing:
            await self._migration_epoch_ended.wait()
        else:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._migration_epoch_ended.wait(), ASYNC_MIGRATION_IDLE_REFILL_S)
        self._migration_budget = ASYNC_MIGRATION_KEYS_PER_EPOCH

    async def function_scheduler(self) -> None:
        await self.started.wait()
        logging.warning("STARTED function scheduler")

        graceful = False
        try:
            while self.running:
                # Wait until the ingress signals that messages are available,
                # or a remote peer wants to proceed, instead of busy-spinning.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self.ingress.messages_available.wait(),
                        timeout=0.1,
                    )
                self.ingress.messages_available.clear()

                async with self.sequencer.lock:
                    sequence: list[SequencedItem] = self.sequencer.get_epoch()
                    if not sequence and not self.remote_wants_to_proceed:
                        continue

                    self.currently_processing = True
                    await self._process_epoch(sequence)

            graceful = True
            logging.warning(f"Worker {self.id} | function_scheduler exiting (running=False)")
        except asyncio.CancelledError:
            logging.warning(f"Worker {self.id} | function_scheduler cancelled")
            raise
        except Exception:
            logging.exception(f"Worker {self.id} | function_scheduler crashed")
        finally:
            # Only call stop() if no external caller (e.g. recovery) is
            # already tearing down.  Otherwise we deadlock: the external
            # stop() awaits this task, while this task awaits stopped.
            if not self._stopping:
                # Only a migration stops the protocol gracefully; its rebuilt
                # protocol takes over the Kafka clients.
                await self.stop(keep_kafka_clients=graceful)

    async def _process_epoch(self, sequence: list[SequencedItem]) -> None:
        epoch_start = timer()

        timings = await self._run_epoch_functions_and_chain(sequence)

        # Every peer's commit flush and snapshot cut follow this barrier, so batches
        # that land before it belong to this epoch at both ends.
        await self._drain_migration_batches()

        sync_time = 0.0
        sync_time += await self._sync_processing_done()

        conflict_resolution_start = timer()
        # HERE WE KNOW ALL THE LOGIC ABORTS
        self.local_state.remove_aborted_from_rw_sets(
            self.networking.logic_aborts_everywhere,
        )
        concurrency_aborts = await self._compute_concurrency_aborts()
        conflict_resolution_end = timer()

        # Notify peers that we are ready to commit
        sync_time += await self._sync_commit(sequence, concurrency_aborts)

        # HERE WE KNOW ALL THE CONCURRENCY ABORTS
        commit_start = timer()
        n_committed = self._commit_and_prepare_responses(sequence)
        self._begin_wait("snapshot delta")
        await self.send_delta_to_snapshotting_proc()
        self._end_wait()
        commit_end = timer()

        # Share of this worker's own transactions aborted anywhere in the cluster.
        # (concurrency_aborts can't be used: it also holds remote transactions
        # that touched local keys, so dividing it by len(sequence) exceeds 1.)
        local_abort_rate = ((len(sequence) - n_committed) / len(sequence)) if sequence else 0.0

        fallback_start = timer()
        abort_rate = await self._maybe_run_fallback()
        fallback_end = timer()

        self._advance_offsets(sequence)

        self.sequencer.increment_epoch(self.max_t_counter, self.t_ids_to_reschedule)
        self._begin_wait("responses to Kafka")
        await self.wait_responses_to_be_sent.wait()
        self._end_wait()
        self.cleanup_after_epoch()

        snap_start = timer()
        self._begin_wait("snapshot request")
        await self.take_snapshot()
        self._end_wait()
        snap_end = timer()

        epoch_end = timer()

        await self._sync_cleanup(
            sequence=sequence,
            epoch_start=epoch_start,
            epoch_end=epoch_end,
            local_abort_rate=local_abort_rate,
            abort_rate=abort_rate,
            timings=timings,
            sync_time=sync_time,
            conflict_ms=(conflict_resolution_end - conflict_resolution_start) * 1000,
            commit_ms=(commit_end - commit_start) * 1000,
            fallback_ms=(fallback_end - fallback_start) * 1000,
            snap_ms=(snap_end - snap_start) * 1000,
        )
        # Every worker has passed this epoch's snapshot cut, so batches sent from
        # here on belong to the next epoch at both ends.
        self._migration_gate.set()
        self._migration_epoch_ended.set()

    async def _run_epoch_functions_and_chain(
        self,
        sequence: list[SequencedItem],
    ) -> dict[str, float]:
        if not sequence:
            return {"wal_ms": 0.0, "func_ms": 0.0, "chain_ms": 0.0}

        # The WAL write (acks=all) runs concurrently with execution. Execution is
        # speculative until commit, and nothing leaves the worker before then
        # (responses, fallback, and snapshots all follow the commit), so the
        # sequence only has to be durable before this epoch syncs for commit.
        wal_task = asyncio.create_task(self._write_to_wal(sequence))
        try:
            start_func = timer()
            self._begin_wait("transactions")
            await asyncio.gather(
                *[self.run_function(item.t_id, item.payload) for item in sequence],
            )
            end_func = timer()

            # Wait for chains to finish
            start_chain = timer()
            self._begin_wait("chains")
            await asyncio.gather(
                *[ack.wait() for ack in self.networking.waited_ack_events.values()],
            )
            end_chain = timer()
        finally:
            self._begin_wait("WAL write")
            start_wal, end_wal = await wal_task
            self._end_wait()

        return {
            "wal_ms": (end_wal - start_wal) * 1000,
            "func_ms": (end_func - start_func) * 1000,
            "chain_ms": (end_chain - start_chain) * 1000,
        }

    async def _sync_processing_done(self) -> float:
        start = timer()
        await self.sync_workers(
            msg_type=MessageType.AriaProcessingDone,
            message=(self.id, self.networking.logic_aborts_everywhere),
            serializer=Serializer.PICKLE,
        )
        end = timer()

        logging.debug(
            f"{self.id} ||| logic_aborts_everywhere: {self.networking.logic_aborts_everywhere}",
        )
        return end - start

    async def _compute_concurrency_aborts(self) -> set[int]:
        logging.debug(f"{self.id} ||| Checking conflicts...")

        handlers = {
            AriaConflictDetectionType.DEFAULT_SERIALIZABLE: self._conflicts_default,
            AriaConflictDetectionType.DETERMINISTIC_REORDERING: self._conflicts_deterministic_reordering,
            AriaConflictDetectionType.SNAPSHOT_ISOLATION: self._conflicts_snapshot_isolation,
        }

        try:
            return await handlers[CONFLICT_DETECTION_METHOD]()
        except KeyError as e:
            logging.error(
                "Invalid conflict detection method: %r",
                CONFLICT_DETECTION_METHOD,
            )
            raise e

    async def _conflicts_default(self) -> set[int]:
        return self.local_state.check_conflicts()

    async def _conflicts_deterministic_reordering(self) -> set[int]:
        await self.sync_workers(
            msg_type=MessageType.DeterministicReordering,
            message=(
                self.id,
                self.local_state.reads,
                self.local_state.write_sets,
                self.local_state.read_sets,
            ),
            serializer=Serializer.PICKLE,
        )
        return self.local_state.check_conflicts_deterministic_reordering()

    async def _conflicts_snapshot_isolation(self) -> set[int]:
        return self.local_state.check_conflicts_snapshot_isolation()

    async def _sync_commit(
        self,
        sequence: list[SequencedItem],
        concurrency_aborts: set[int],
    ) -> float:
        start = timer()
        await self.sync_workers(
            msg_type=MessageType.AriaCommit,
            message=(
                self.id,
                concurrency_aborts,
                self.sequencer.t_counter,
                len(sequence),
            ),
            serializer=Serializer.PICKLE,
        )
        end = timer()

        return end - start

    def _commit_and_prepare_responses(self, sequence: list[SequencedItem]) -> int:
        """Commit the epoch and schedule its responses; returns how many of ``sequence`` committed."""
        logging.debug(
            f"{self.id} ||| Starting commit! {self.concurrency_aborts_everywhere}",
        )

        self.local_state.commit(self.concurrency_aborts_everywhere)

        self.t_ids_to_reschedule = self.concurrency_aborts_everywhere - self.networking.logic_aborts_everywhere

        current_completed_t_ids: list[SequencedItem] = [
            item for item in sequence if item.t_id not in self.concurrency_aborts_everywhere
        ]

        # Shallow copies — values (response bytes / exception strings) are
        # immutable, and cleanup_after_epoch will rebind the originals rather
        # than mutating these dicts. Cheaper than the msgpack round-trip we
        # used to do here.
        self.aio_task_scheduler.create_task(
            self.send_responses(
                current_completed_t_ids,
                dict(self.networking.client_responses),
                dict(self.networking.aborted_events),
            ),
        )

        logging.debug(
            f"{self.id} ||| Sequence committed! | "
            f"{len(self.concurrency_aborts_everywhere)} / {self.total_processed_seq_size}",
        )
        return len(current_completed_t_ids)

    async def _maybe_run_fallback(self) -> float:
        abort_rate: float = (
            len(self.concurrency_aborts_everywhere) / self.total_processed_seq_size
            if self.total_processed_seq_size
            else 0.0
        )
        if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
            logging.debug(
                f"{self.id} ||| Epoch: {self.sequencer.epoch_counter} "
                f"Abort percentage: {int(abort_rate * 100)}% initiating fallback strategy...",
            )

            await self.run_fallback_strategy()
            await self.send_delta_to_snapshotting_proc()
            self.concurrency_aborts_everywhere = set()
            # Keep rescheduled t_ids (rw-set changed during fallback) for the next epoch
            self.t_ids_to_reschedule = self.fallback_rescheduled_t_ids.copy()
            self.fallback_rescheduled_t_ids.clear()
        return abort_rate

    def _advance_offsets(self, sequence: list[SequencedItem]) -> None:
        for item in sequence:
            if item.t_id in self.concurrency_aborts_everywhere:
                continue

            payload = item.payload
            tpo_key = (payload.operator_name, payload.kafka_ingress_partition)

            prev = self.topic_partition_offsets.get(tpo_key)
            if prev is None:
                self.topic_partition_offsets[tpo_key] = payload.kafka_offset
            else:
                self.topic_partition_offsets[tpo_key] = max(payload.kafka_offset, prev)

    async def _sync_cleanup(
        self,
        *,
        sequence: list[SequencedItem],
        epoch_start: float,
        epoch_end: float,
        local_abort_rate: float,
        abort_rate: float,
        timings: dict[str, float],
        sync_time: float,
        conflict_ms: float,
        commit_ms: float,
        fallback_ms: float,
        snap_ms: float,
    ) -> None:
        epoch_latency = max(round((epoch_end - epoch_start) * 1000, 4), 1)
        epoch_throughput = ((len(sequence) - len(self.concurrency_aborts_everywhere)) * 1000) // epoch_latency  # TPS

        logging.debug(
            f"{self.id} ||| Epoch: {self.sequencer.epoch_counter - 1} done in "
            f"{epoch_latency}ms "
            f"global logic aborts: {len(self.networking.logic_aborts_everywhere)} "
            f"concurrency aborts for next epoch: {len(self.concurrency_aborts_everywhere)} "
            f"abort rate: {abort_rate}",
        )

        await self.sync_workers(
            msg_type=MessageType.SyncCleanup,
            message=(
                self.id,
                epoch_throughput,
                epoch_latency,
                local_abort_rate,
                round(timings["wal_ms"], 4),
                round(timings["func_ms"], 4),
                round(timings["chain_ms"], 4),
                round(sync_time * 1000, 4),
                round(conflict_ms, 4),
                round(commit_ms, 4),
                round(fallback_ms, 4),
                round(snap_ms, 4),
            ),
            serializer=Serializer.MSGPACK,
        )

    async def send_delta_to_snapshotting_proc(self) -> None:
        delta_to_send = self.local_state.take_delta_map()

        await self.snapshotting_networking_manager.send_message(
            self.networking.host_name,
            self.snapshotting_port,
            msg=(delta_to_send,),
            msg_type=MessageType.SnapProcDelta,
            serializer=Serializer.MSGPACK,
        )

    def cleanup_after_epoch(self) -> None:
        self.concurrency_aborts_everywhere.clear()
        self.t_ids_to_reschedule.clear()
        self.fallback_rescheduled_t_ids.clear()
        self._proxy_rw_changed.clear()
        if self._proxied_calls:
            logging.warning(f"{self.id} ||| Proxied calls this epoch: {self._proxied_calls}")
            self._proxied_calls = 0
        self.wait_responses_to_be_sent.clear()
        self.networking.cleanup_after_epoch()
        self.local_state.cleanup()
        self.waiting_on_transactions.clear()
        self.fallback_locking_event_map.clear()
        self.remote_wants_to_proceed = False
        self.currently_processing = False

    async def run_fallback_function(
        self,
        t_id: int,
        payload: RunFuncPayload,
        internal: bool = False,
    ) -> None:
        # Wait for all transactions that this transaction depends on to finish
        if self.waiting_on_transactions.get(t_id):
            tasks = [
                self.fallback_locking_event_map[dependency_t_id].wait()
                for dependency_t_id in self.waiting_on_transactions[t_id]
                if dependency_t_id in self.fallback_locking_event_map
            ]
            await asyncio.gather(*tasks)

        # Run transaction
        success = await self.run_function(t_id, payload, fallback_mode=True)
        if not internal:
            if t_id in self.networking.waited_ack_events:
                # wait on ack of parts
                await self.networking.waited_ack_events[t_id].wait()
            transaction_failed: bool = t_id in self.networking.aborted_events or not success

            # Per the Aria paper: if the rw-set changed during fallback
            # re-execution, the dependency graph is invalid — reschedule
            # the transaction to the next epoch instead of committing.
            rw_changed = self.local_state.has_fallback_rw_set_changed(t_id) or t_id in self._proxy_rw_changed
            if not transaction_failed and not rw_changed:
                self.local_state.commit_fallback_transaction(t_id)
            elif rw_changed:
                self.fallback_rescheduled_t_ids.add(t_id)
                transaction_failed = True

            await self.fallback_unlock(t_id, success=not transaction_failed)

            # If the txn was rescheduled because the fallback rw-set drifted
            # from the optimistic one, the commit hasn't happened and the
            # response isn't durable yet. Defer egress to the next epoch's
            # run; otherwise we'd send the response now AND again when the
            # txn finally commits, producing duplicates.
            # Batched send: each `egress.send` enqueues a Kafka future and
            # returns immediately; the strategy flushes all of them with a
            # single `send_batch()` at the end of fallback. Previously we
            # used `send_immediate` here, which did a synchronous round-trip
            # per txn — fine with 1-2 fallback commits per epoch, but a major
            # bottleneck when fallback commits ~100 txns at once.
            if rw_changed:
                pass
            elif t_id in self.networking.aborted_events:
                await self.egress.send(
                    key=payload.request_id,
                    value=msgpack_serialization(self.networking.aborted_events[t_id]),
                    operator_name=payload.operator_name,
                    partition=self._response_partition(payload),
                )
            elif t_id in self.networking.client_responses:
                await self.egress.send(
                    key=payload.request_id,
                    value=msgpack_serialization(self.networking.client_responses[t_id]),
                    operator_name=payload.operator_name,
                    partition=self._response_partition(payload),
                )

    async def run_fallback_strategy(self) -> None:
        logging.debug("Starting fallback strategy...")

        (self.waiting_on_transactions, self.fallback_locking_event_map) = self.local_state.get_dep_transactions(
            self.t_ids_to_reschedule
        )

        fallback_tasks = []
        aborted_sequence: list[SequencedItem] = self.sequencer.get_aborted_sequence(
            self.t_ids_to_reschedule,
        )
        self.networking.clear_aborted_events_for_fallback()
        for sequenced_item in aborted_sequence:
            # current worker is the root of the chain
            self.networking.reset_ack_for_fallback(sequenced_item.t_id)
            fallback_tasks.append(
                self.run_fallback_function(
                    sequenced_item.t_id,
                    sequenced_item.payload,
                ),
            )

        await self.sync_workers(
            msg_type=MessageType.AriaFallbackStart,
            message=(self.id,),
            serializer=Serializer.MSGPACK,
        )
        if fallback_tasks:
            self._begin_wait("fallback")
            await asyncio.gather(*fallback_tasks)
            self._end_wait()
            # Flush all fallback egress sends in one batch (each root above
            # used `egress.send`, which enqueues; `send_batch` awaits the
            # producer futures together).
            await self.egress.send_batch()

        logging.debug(
            f"Epoch: {self.sequencer.epoch_counter} Fallback strategy done waiting for peers",
        )

        await self.sync_workers(
            msg_type=MessageType.AriaFallbackDone,
            message=(self.id,),
            serializer=Serializer.MSGPACK,
        )

    async def unlock_tid(self, t_id_to_unlock: int) -> None:
        if t_id_to_unlock in self.fallback_locking_event_map:
            async with self.fallback_locking_event_map_lock:
                self.fallback_locking_event_map[t_id_to_unlock].set()
        else:
            logging.error(
                f"{self.sequencer.epoch_counter} Unlock tid {t_id_to_unlock} not found. But should exist!",
            )

    async def send_responses(
        self,
        current_sequence_t_ids: list[SequencedItem],
        client_responses: dict[int, str],
        aborted_events: dict[int, str],
    ) -> None:
        for sequenced_item in current_sequence_t_ids:
            t_id = sequenced_item.t_id
            request_id = sequenced_item.payload.request_id
            operator_name = sequenced_item.payload.operator_name
            partition = self._response_partition(sequenced_item.payload)
            if t_id in aborted_events:
                await self.egress.send(
                    key=request_id,
                    value=msgpack_serialization(aborted_events[t_id]),
                    operator_name=operator_name,
                    partition=partition,
                )
            elif t_id in client_responses:
                await self.egress.send(
                    key=request_id,
                    value=msgpack_serialization(client_responses[t_id]),
                    operator_name=operator_name,
                    partition=partition,
                )
        await self.egress.send_batch()
        self.wait_responses_to_be_sent.set()

    async def fallback_unlock(self, t_id: int, success: bool) -> None:
        # Release the locks for local
        await self.unlock_tid(t_id)
        # Release the locks for remote participants
        if self.networking.chain_participants.get(t_id):
            async with asyncio.TaskGroup() as tg:
                for participant in self.networking.chain_participants[t_id]:
                    tg.create_task(
                        self.networking.send_message(
                            self.peers[participant][0],
                            self.peers[participant][2],
                            msg=(t_id, success),
                            msg_type=MessageType.Unlock,
                            serializer=Serializer.MSGPACK,
                        ),
                    )

    async def sync_workers(
        self,
        msg_type: MessageType,
        message: tuple | bytes,
        serializer: Serializer = Serializer.MSGPACK,
    ) -> None:
        await self.networking.send_message(
            DISCOVERY_HOST,
            DISCOVERY_PORT + 1,
            msg=message,
            msg_type=msg_type,
            serializer=serializer,
        )
        self._begin_wait(f"barrier {msg_type.name}")
        await self.sync_workers_event[msg_type].wait()
        self._end_wait()
        self.sync_workers_event[msg_type].clear()

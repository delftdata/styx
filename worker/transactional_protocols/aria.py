import asyncio
import os
import time

from timeit import default_timer as timer

from aiokafka import TopicPartition
from msgspec import msgpack

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition
from styx.common.tcp_networking import NetworkingManager
from styx.common.operator import Operator
from styx.common.serialization import Serializer, msgpack_serialization
from styx.common.base_protocol import BaseTransactionalProtocol
from styx.common.run_func_payload import RunFuncPayload, SequencedItem
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress
from worker.ingress.styx_kafka_ingress import StyxKafkaIngress
from worker.operator_state.aria.conflict_detection_types import AriaConflictDetectionType
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.sequencer.sequencer import Sequencer


DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])

CONFLICT_DETECTION_METHOD: AriaConflictDetectionType = AriaConflictDetectionType(os.getenv('CONFLICT_DETECTION_METHOD',
                                                                                           0))
# if more than 10% aborts use fallback strategy
FALLBACK_STRATEGY_PERCENTAGE: float = float(os.getenv('FALLBACK_STRATEGY_PERCENTAGE', -0.1))
SNAPSHOTTING_THREADS: int = int(os.getenv('SNAPSHOTTING_THREADS', 4))
SEQUENCE_MAX_SIZE: int = int(os.getenv('SEQUENCE_MAX_SIZE', 1_000))
USE_FALLBACK_CACHE: bool = bool(os.getenv('USE_FALLBACK_CACHE', True))
KAFKA_URL: str = os.environ['KAFKA_URL']
USE_ASYNC_MIGRATION: bool = bool(os.getenv('USE_ASYNC_MIGRATION', True))
ASYNC_MIGRATION_BATCH_SIZE: int = int(os.getenv('ASYNC_MIGRATION_BATCH_SIZE', 2_000))


class AriaProtocol(BaseTransactionalProtocol):

    def __init__(self,
                 worker_id,
                 peers: dict[int, tuple[str, int, int]],
                 dns: dict[str, dict[int, tuple[str, int, int]]],
                 networking: NetworkingManager,
                 registered_operators: dict[OperatorPartition, Operator],
                 topic_partitions: list[TopicPartition],
                 state: InMemoryOperatorState | Stateless,
                 snapshotting_port: int,
                 topic_partition_offsets: dict[OperatorPartition, int] = None,
                 output_offsets: dict[OperatorPartition, int] = None,
                 epoch_counter: int = 0,
                 t_counter: int = 0,
                 request_id_to_t_id_map: dict[bytes, int] = None,
                 restart_after_recovery: bool = False,
                 restart_after_migration: bool = False,):

        if topic_partition_offsets is None:
            topic_partition_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}
        if output_offsets is None:
            output_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}

        self.id = worker_id

        self.topic_partitions = topic_partitions
        self.networking = networking
        self.snapshotting_networking_manager = NetworkingManager(None, size=1)

        self.local_state: InMemoryOperatorState | Stateless = state
        self.aio_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()
        self.background_functions: AIOTaskScheduler = AIOTaskScheduler()

        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int, int]] = peers
        self.dns: dict[str, dict[int, tuple[str, int, int]]] = dns
        self.topic_partition_offsets: dict[OperatorPartition, int] = topic_partition_offsets
        # worker_id: set of aborted t_ids
        self.concurrency_aborts_everywhere: set[int] = set()
        self.t_ids_to_reschedule: set[int] = set()

        # ready_to_commit_events -> worker_id: Event that appears if the peer is ready to commit
        self.ready_to_reorder_events: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                                  for peer_id in self.peers.keys()}

        # FALLBACK LOCKING
        # t_id: its lock
        self.fallback_locking_event_map: dict[int, asyncio.Event] = {}
        self.fallback_locking_event_map_lock: asyncio.Lock = asyncio.Lock()
        # t_id: the t_ids it depends on
        self.waiting_on_transactions: dict[int, set[int]] = {}

        self.registered_operators: dict[OperatorPartition, Operator] = registered_operators

        self.sequencer = Sequencer(SEQUENCE_MAX_SIZE, t_counter=t_counter, epoch_counter=epoch_counter)
        self.sequencer.set_sequencer_id(list(self.peers.keys()), self.id)
        self.sequencer.set_wal_values_after_recovery(request_id_to_t_id_map)

        self.ingress: StyxKafkaIngress = StyxKafkaIngress(networking=self.networking,
                                                          sequencer=self.sequencer,
                                                          state=self.local_state,
                                                          registered_operators=self.registered_operators,
                                                          worker_id=self.id,
                                                          kafka_url=KAFKA_URL,
                                                          sequence_max_size=SEQUENCE_MAX_SIZE,
                                                          epoch_interval_ms=1)

        self.egress: StyxKafkaBatchEgress = StyxKafkaBatchEgress(output_offsets, restart_after_recovery)
        # Primary task used for processing
        self.function_scheduler_task: asyncio.Task = ...
        self.communication_task: asyncio.Task = ...

        self.max_t_counter: int = -1
        self.total_processed_seq_size: int = -1

        self.sync_workers_event: dict[MessageType, asyncio.Event] = {
            MessageType.AriaProcessingDone: asyncio.Event(),
            MessageType.SyncCleanup: asyncio.Event(),
            MessageType.AriaFallbackStart: asyncio.Event(),
            MessageType.AriaFallbackDone: asyncio.Event(),
            MessageType.AriaCommit: asyncio.Event(),
            MessageType.DeterministicReordering: asyncio.Event()
        }

        self.networking_locks: dict[MessageType, asyncio.Lock] = {
            MessageType.RunFunRemote: asyncio.Lock(),
            MessageType.AriaCommit: asyncio.Lock(),
            MessageType.AriaFallbackDone: asyncio.Lock(),
            MessageType.AriaFallbackStart: asyncio.Lock(),
            MessageType.SyncCleanup: asyncio.Lock(),
            MessageType.AriaProcessingDone: asyncio.Lock(),
            MessageType.Ack: asyncio.Lock(),
            MessageType.AckCache: asyncio.Lock(),
            MessageType.ChainAbort: asyncio.Lock(),
            MessageType.Unlock: asyncio.Lock(),
            MessageType.DeterministicReordering: asyncio.Lock(),
            MessageType.WrongPartitionRequest: asyncio.Lock(),
            MessageType.ResponseToRoot: asyncio.Lock(),
            MessageType.AsyncMigration: asyncio.Lock()
        }

        self.remote_wants_to_proceed: bool = False
        self.currently_processing: bool = False

        self.started = asyncio.Event()
        self.wait_responses_to_be_sent = asyncio.Event()

        self.running: bool = True
        self.stopped: asyncio.Event = asyncio.Event()
        self.snapshot_marker_received: bool = False
        self.snapshotting_port: int = snapshotting_port

        self.migrating_state: bool = restart_after_migration

    async def wait_stopped(self):
        await self.stopped.wait()

    async def stop(self):
        await self.ingress.stop()
        await self.egress.stop()
        await self.aio_task_scheduler.close()
        await self.background_functions.close()
        await self.snapshotting_networking_manager.close_all_connections()
        self.function_scheduler_task.cancel()
        self.communication_task.cancel()
        try:
            await self.function_scheduler_task
            await self.communication_task
        except asyncio.CancelledError:
            logging.warning("Protocol coroutines stopped")
        logging.info(f"Active tasks: {asyncio.all_tasks()}")
        self.stopped.set()
        logging.warning(f"Aria protocol stopped at: {self.topic_partition_offsets}")

    def start(self):
        self.function_scheduler_task = asyncio.create_task(self.function_scheduler())
        self.communication_task = asyncio.create_task(self.communication_protocol())
        logging.warning(f"Aria protocol started with operator partitions: {list(self.registered_operators.keys())}")

    async def run_function(
            self,
            t_id: int,
            payload: RunFuncPayload,
            fallback_mode: bool = False
    ) -> bool:
        # logging.info(f"Running function: {payload.function_name} with T_ID {t_id} with params {payload.params}"
        #              f" and ack payload {payload.ack_payload}")

        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]

        success: bool = await operator_partition.run_function(
            payload.key,
            t_id,
            payload.request_id,
            payload.function_name,
            payload.partition,
            payload.ack_payload,
            fallback_mode,
            USE_FALLBACK_CACHE,
            payload.params,
            self
        )
        return success

    async def take_snapshot(self):
        if self.snapshot_marker_received:
            logging.warning(f"ARIA | Snapshot marker received @epoch: {self.sequencer.epoch_counter}")
            await self.snapshotting_networking_manager.send_message(self.networking.host_name, self.snapshotting_port,
                                                                    msg=(self.topic_partition_offsets,
                                                                         self.egress.topic_partition_output_offsets,
                                                                         self.sequencer.epoch_counter, self.sequencer.t_counter),
                                                                    msg_type=MessageType.SnapTakeSnapshot,
                                                                    serializer=Serializer.MSGPACK)
            self.snapshot_marker_received = False

    async def communication_protocol(self):
        await self.ingress.start(self.topic_partitions, self.topic_partition_offsets)
        logging.warning('Ingress started')
        await self.egress.start(self.id)
        logging.warning('Egress started')
        await self.started.wait()

    # Refactoring candidate
    async def protocol_tcp_controller(self, data: bytes):
        message_type: MessageType = self.networking.get_msg_type(data)
        match message_type:
            case MessageType.RunFunRemote:
                async with self.networking_locks[message_type]:
                    # logging.info('CALLED RUN FUN FROM PEER')
                    (t_id, request_id, operator_name, function_name,
                     key, partition, fallback_enabled, params, ack) = self.networking.decode_message(data)
                    payload = RunFuncPayload(request_id=request_id, key=key,
                                             operator_name=operator_name, partition=partition,
                                             function_name=function_name, params=params, ack_payload=ack)

                    if fallback_enabled:
                        # Running in fallback mode
                        self.background_functions.create_task(
                            self.run_fallback_function(
                                t_id,
                                payload,
                                internal=True
                            )
                        )
                    else:
                        if USE_FALLBACK_CACHE:
                            # If fallback caching is enabled add the function call to the cache for a potential fallback
                            self.networking.add_remote_function_call(t_id, payload)
                        self.background_functions.create_task(
                            self.run_function(
                                t_id,
                                payload
                            )
                        )
            case MessageType.WrongPartitionRequest:
                async with self.networking_locks[message_type]:
                    # During migration a message might arrive from a different partition
                    (request_id, operator_name, function_name, key, partition, kafka_ingress_partition,
                     kafka_offset, params) = self.networking.decode_message(data)
                    # logging.warning(f"Aria WrongPartitionRequest: {request_id}:{operator_name}:{kafka_ingress_partition}")
                    payload = RunFuncPayload(request_id=request_id, key=key,
                                             operator_name=operator_name, partition=partition,
                                             function_name=function_name, params=params,
                                             kafka_ingress_partition=kafka_ingress_partition,
                                             kafka_offset=kafka_offset)
                    self.sequencer.sequence(payload)
            case MessageType.RunFunRqRsRemote:
                logging.error('REQUEST RESPONSE HAS BEEN DEPRECATED')
            case MessageType.AriaCommit:
                async with self.networking_locks[message_type]:
                    (self.concurrency_aborts_everywhere, self.total_processed_seq_size,
                     self.max_t_counter, self.snapshot_marker_received) = self.networking.decode_message(data)
                    self.sync_workers_event[message_type].set()
            case (MessageType.AriaFallbackDone | MessageType.AriaFallbackStart):
                async with self.networking_locks[message_type]:
                    self.sync_workers_event[message_type].set()
            case MessageType.SyncCleanup:
                async with self.networking_locks[message_type]:
                    (stop_gracefully, ) = self.networking.decode_message(data)
                    if stop_gracefully:
                        self.running = False
                    self.sync_workers_event[message_type].set()
            case MessageType.AriaProcessingDone:
                async with self.networking_locks[message_type]:
                    (self.networking.logic_aborts_everywhere, ) = self.networking.decode_message(data)
                    self.sync_workers_event[message_type].set()
            case MessageType.Ack:
                async with self.networking_locks[message_type]:
                    (ack_id, fraction_str,
                     chain_participants, partial_node_count, ) = self.networking.decode_message(data)
                    self.networking.add_ack_fraction_str(ack_id, fraction_str,
                                                         chain_participants, partial_node_count, )
            case MessageType.AckCache:
                async with self.networking_locks[message_type]:
                    (ack_id, ) = self.networking.decode_message(data)
                    self.networking.add_ack_cnt(ack_id, )
            case MessageType.ChainAbort:
                async with self.networking_locks[message_type]:
                    (ack_id, exception_str) = self.networking.decode_message(data)
                    self.networking.abort_chain(ack_id, exception_str)
            case MessageType.ResponseToRoot:
                async with self.networking_locks[message_type]:
                    (ack_id, resp) = self.networking.decode_message(data)
                    self.networking.add_response(ack_id, resp)
            case MessageType.Unlock:
                async with self.networking_locks[message_type]:
                    # fallback phase
                    # here we handle the logic to unlock locks held by the provided distributed transaction
                    (t_id, success) = self.networking.decode_message(data)
                    if success:
                        # commit changes
                        self.local_state.commit_fallback_transaction(t_id)
                    # unlock
                    await self.unlock_tid(t_id)
            case MessageType.DeterministicReordering:
                async with self.networking_locks[message_type]:
                    (global_read_reservations, global_write_set, global_read_set) = self.networking.decode_message(data)
                    self.local_state.set_global_read_write_sets(global_read_reservations,
                                                                global_write_set,
                                                                global_read_set)
                    self.sync_workers_event[message_type].set()
            case MessageType.RemoteWantsToProceed:
                if not self.currently_processing:
                    self.remote_wants_to_proceed = True
            case MessageType.AsyncMigration:
                (operator_partition, batch) = self.networking.decode_message(data)
                # logging.warning(f"MIGRATION | Received migration batch for: {operator_partition}")
                async with self.networking_locks[message_type]:
                    self.local_state.set_batch_data_from_migration(operator_partition, batch)
            case _:
                logging.error(f"Aria protocol: Non supported command message type: {message_type}")

    async def _write_to_wal(self, sequence):
        start_wal = timer()
        sequence_to_log = msgpack_serialization({seq_item.payload.request_id: seq_item.t_id for seq_item in sequence})
        await self.egress.send_message_to_topic(
            key=msgpack_serialization(self.sequencer.epoch_counter),
            message=sequence_to_log,
            topic='sequencer-wal'
        )
        end_wal = timer()
        # logging.info(f"Write to WAL successful at epoch: {self.sequencer.epoch_counter}")
        return start_wal, end_wal

    async def send_async_migrate_batch(self):
        if USE_ASYNC_MIGRATION and self.local_state.has_keys_to_send:
            batch = self.local_state.get_async_migrate_batch(ASYNC_MIGRATION_BATCH_SIZE)
            for operator_partition, k_v_pairs in batch.items():
                operator_name, partition = operator_partition
                worker = self.dns[operator_name][partition]
                if worker == self.id:
                    async with self.networking_locks[MessageType.AsyncMigration]:
                        self.local_state.set_batch_data_from_migration(operator_partition, k_v_pairs)
                else:
                    await self.networking.send_message(worker[0], worker[2],
                                                       msg=(operator_partition, k_v_pairs),
                                                       msg_type=MessageType.AsyncMigration,
                                                       serializer=Serializer.MSGPACK)

    async def function_scheduler(self):
        await self.started.wait()
        logging.warning('STARTED function scheduler')
        while self.running:
            # need to sleep to allow the kafka consumer coroutine to read data
            await asyncio.sleep(0)
            async with self.sequencer.lock:
                # GET SEQUENCE
                sequence: list[SequencedItem] = self.sequencer.get_epoch()
                if sequence or self.remote_wants_to_proceed:
                    self.currently_processing = True
                    logging.warning(f'{self.id} ||| Epoch: {self.sequencer.epoch_counter} starts '
                                    f'running {len(sequence)} functions...')
                    # Run all the epochs functions concurrently
                    epoch_start = timer()
                    # async with self.snapshot_state_lock:
                    if sequence:
                        start_wal, end_wal = await self._write_to_wal(sequence)
                        start_func = timer()
                        await asyncio.gather(*[self.run_function(sequenced_item.t_id, sequenced_item.payload)
                                               for sequenced_item in sequence])
                        end_func = timer()
                        # Wait for chains to finish

                        logging.warning(f'{self.id} ||| '
                                        f'Waiting on chained {len(self.networking.waited_ack_events)} functions...')
                        start_chain = timer()
                        await asyncio.gather(*[ack.wait()
                                               for ack in self.networking.waited_ack_events.values()])
                        end_chain = timer()
                    else:
                        start_wal = end_wal = start_func = end_func = start_chain = end_chain = 0.0

                    start_sync = timer()
                    # wait for all peers to be done processing (needed to know the aborts)
                    await self.sync_workers(msg_type=MessageType.AriaProcessingDone,
                                            message=(self.id, self.networking.logic_aborts_everywhere),
                                            serializer=Serializer.PICKLE)
                    end_sync = timer()
                    sync_time = 0.0
                    sync_time += end_sync - start_sync
                    logging.warning(f'{self.id} ||| '
                                    f'logic_aborts_everywhere: {self.networking.logic_aborts_everywhere}')
                    # HERE WE KNOW ALL THE LOGIC ABORTS
                    # removing the global logic abort transactions from the commit phase
                    conflict_resolution_start = timer()
                    self.local_state.remove_aborted_from_rw_sets(self.networking.logic_aborts_everywhere)
                    # Check for local state conflicts
                    logging.warning(f'{self.id} ||| Checking conflicts...')
                    if CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DEFAULT_SERIALIZABLE:
                        concurrency_aborts: set[int] = self.local_state.check_conflicts()
                    elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DETERMINISTIC_REORDERING:
                        await self.sync_workers(msg_type=MessageType.DeterministicReordering,
                                                message=(self.id,
                                                         self.local_state.reads,
                                                         self.local_state.write_sets,
                                                         self.local_state.read_sets),
                                                serializer=Serializer.PICKLE)
                        concurrency_aborts: set[int] = self.local_state.check_conflicts_deterministic_reordering()
                    elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SNAPSHOT_ISOLATION:
                        concurrency_aborts: set[int] = self.local_state.check_conflicts_snapshot_isolation()
                    else:
                        logging.error('This conflict detection method number is not a valid number')
                        exit()
                    conflict_resolution_end = timer()
                    if sequence:
                        local_abort_rate = len(concurrency_aborts) / len(sequence)
                    else:
                        local_abort_rate = 0.0
                    # Notify peers that we are ready to commit
                    logging.warning(f'{self.id} ||| Notify peers...')
                    start_sync = timer()
                    await self.sync_workers(msg_type=MessageType.AriaCommit,
                                            message=(self.id,
                                                     concurrency_aborts,
                                                     self.sequencer.t_counter,
                                                     len(sequence)),
                                            serializer=Serializer.PICKLE,
                                            send_async_migration_message=True)
                    end_sync = timer()
                    sync_time += end_sync - start_sync
                    # HERE WE KNOW ALL THE CONCURRENCY ABORTS
                    logging.warning(f'{self.id} ||| Starting commit! {self.concurrency_aborts_everywhere}')
                    start_commit = timer()
                    self.local_state.commit(self.concurrency_aborts_everywhere)

                    self.t_ids_to_reschedule = (self.concurrency_aborts_everywhere -
                                                self.networking.logic_aborts_everywhere)

                    current_completed_t_ids: list[SequencedItem] = [seq_i for seq_i in sequence
                                                                    if seq_i.t_id not in self.concurrency_aborts_everywhere]
                    self.aio_task_scheduler.create_task(self.send_responses(
                        current_completed_t_ids,
                        msgpack.decode(msgpack.encode(self.networking.client_responses)),
                        msgpack.decode(msgpack.encode(self.networking.aborted_events))
                    ))

                    await self.send_delta_to_snapshotting_proc()

                    end_commit = timer()

                    logging.warning(f'{self.id} ||| Sequence committed! | {len(self.concurrency_aborts_everywhere)} / {self.total_processed_seq_size}')

                    start_fallback = timer()
                    abort_rate: float = len(self.concurrency_aborts_everywhere) / self.total_processed_seq_size

                    if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
                        # Run Calvin
                        logging.warning(
                            f'{self.id} ||| Epoch: {self.sequencer.epoch_counter} '
                            f'Abort percentage: {int(abort_rate * 100)}% initiating fallback strategy...'
                        )

                        await self.run_fallback_strategy()
                        await self.send_delta_to_snapshotting_proc()
                        self.concurrency_aborts_everywhere = set()
                        concurrency_aborts = set()
                        self.t_ids_to_reschedule = set()
                    end_fallback = timer()
                    for sequenced_item in sequence:
                        if sequenced_item.t_id not in self.concurrency_aborts_everywhere:
                            payload = sequenced_item.payload
                            tpo_key = (payload.operator_name, payload.kafka_ingress_partition)
                            if tpo_key in self.topic_partition_offsets:
                                self.topic_partition_offsets[tpo_key] = (
                                    max(
                                        payload.kafka_offset,
                                        self.topic_partition_offsets[tpo_key]
                                    )
                                )
                            else:
                                self.topic_partition_offsets[tpo_key] = payload.kafka_offset

                    self.sequencer.increment_epoch(
                        self.max_t_counter,
                        self.t_ids_to_reschedule
                    )
                    await self.wait_responses_to_be_sent.wait()
                    self.cleanup_after_epoch()
                    snap_start = timer()
                    await self.take_snapshot()
                    snap_end = timer()
                    epoch_end = timer()
                    epoch_latency = max(round((epoch_end - epoch_start) * 1000, 4), 1)
                    epoch_throughput = ((len(sequence) - len(concurrency_aborts)) * 1000) // epoch_latency # TPS
                    logging.warning(
                        f'{self.id} ||| Epoch: {self.sequencer.epoch_counter - 1} done in '
                        f'{epoch_latency}ms '
                        f'global logic aborts: {len(self.networking.logic_aborts_everywhere)} '
                        f'concurrency aborts for next epoch: {len(self.concurrency_aborts_everywhere)} '
                        f'abort rate: {abort_rate}'
                    )
                    if self.migrating_state and USE_ASYNC_MIGRATION:
                        migration_progress = self.local_state.keys_remaining_to_send()
                        # logging.warning(f"Keys to be sent: {migration_progress}")
                        if migration_progress == 0:
                            self.migrating_state = False
                            await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT + 1,
                                                               msg=b"",
                                                               msg_type=MessageType.MigrationDone,
                                                               serializer=Serializer.NONE)
                            logging.warning(f"MIGRATION_FINISHED at epoch: {self.sequencer.epoch_counter}"
                                            f" at time: {time.time_ns() // 1_000_000}")
                    await self.sync_workers(msg_type=MessageType.SyncCleanup,
                                            message=(self.id,
                                                     epoch_throughput,
                                                     epoch_latency,
                                                     local_abort_rate,
                                                     round((end_wal - start_wal) * 1000, 4),
                                                     round((end_func - start_func) * 1000, 4),
                                                     round((end_chain - start_chain) * 1000, 4),
                                                     round(sync_time * 1000, 4),
                                                     round((conflict_resolution_end - conflict_resolution_start) * 1000, 4),
                                                     round((end_commit - start_commit) * 1000, 4),
                                                     round((end_fallback - start_fallback) * 1000, 4),
                                                     round((snap_end - snap_start) * 1000, 4)),
                                            serializer=Serializer.MSGPACK)
        await self.stop()

    async def send_delta_to_snapshotting_proc(self):
        delta_to_send = self.local_state.get_data_for_snapshot()
        await self.snapshotting_networking_manager.send_message(self.networking.host_name, self.snapshotting_port,
                                                                msg=(delta_to_send,),
                                                                msg_type=MessageType.SnapProcDelta,
                                                                serializer=Serializer.MSGPACK)
        self.local_state.clear_delta_map()

    def cleanup_after_epoch(self):
        self.concurrency_aborts_everywhere.clear()
        self.t_ids_to_reschedule.clear()
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
            collocated_same_t_id_functions: list[RunFuncPayload] = None # important when fallback cache is true
    ):
        # Wait for all transactions that this transaction depends on to finish
        if t_id in self.waiting_on_transactions:
            if self.waiting_on_transactions[t_id]:
                tasks = [self.fallback_locking_event_map[dependency_t_id].wait()
                         for dependency_t_id in self.waiting_on_transactions[t_id]
                         if dependency_t_id in self.fallback_locking_event_map]
                await asyncio.gather(*tasks)

        # Run transaction
        success = await self.run_function(t_id, payload, fallback_mode=True)
        if not internal:
            # if root of chain
            if collocated_same_t_id_functions is not None:
                await asyncio.gather(*[self.run_function(t_id,
                                                         c_payload,
                                                         fallback_mode=True)
                                       for c_payload in collocated_same_t_id_functions])


            if t_id in self.networking.waited_ack_events:
                # wait on ack of parts
                await self.networking.waited_ack_events[t_id].wait()
            transaction_failed: bool = t_id in self.networking.aborted_events or not success

            if not transaction_failed:
                self.local_state.commit_fallback_transaction(t_id)

            await self.fallback_unlock(t_id, success=not transaction_failed)

            if t_id in self.networking.aborted_events:
                await self.egress.send_immediate(key=payload.request_id,
                                                 value=msgpack_serialization(self.networking.aborted_events[t_id]),
                                                 operator_name=payload.operator_name,
                                                 partition=payload.partition)
            elif t_id in self.networking.client_responses:
                await self.egress.send_immediate(key=payload.request_id,
                                                 value=msgpack_serialization(self.networking.client_responses[t_id]),
                                                 operator_name=payload.operator_name,
                                                 partition=payload.partition)

    async def run_fallback_strategy(self):
        # logging.info('Starting fallback strategy...')

        (self.waiting_on_transactions,
         self.fallback_locking_event_map) = self.local_state.get_dep_transactions(self.t_ids_to_reschedule)

        fallback_tasks = []
        aborted_sequence: list[SequencedItem] = self.sequencer.get_aborted_sequence(self.t_ids_to_reschedule)
        self.networking.clear_aborted_events_for_fallback()
        for sequenced_item in aborted_sequence:
            # current worker is the root of the chain
            collocated_t_id_functions = None
            if USE_FALLBACK_CACHE:
                self.networking.reset_ack_for_fallback_cache(sequenced_item.t_id)
                if sequenced_item.t_id in self.networking.remote_function_calls:
                    collocated_t_id_functions = self.networking.remote_function_calls[sequenced_item.t_id]
                    del self.networking.remote_function_calls[sequenced_item.t_id]
            else:
                self.networking.reset_ack_for_fallback(sequenced_item.t_id)
            fallback_tasks.append(
                self.run_fallback_function(
                    sequenced_item.t_id,
                    sequenced_item.payload,
                    collocated_same_t_id_functions=collocated_t_id_functions
                )
            )

        if USE_FALLBACK_CACHE:
            remote_payloads = [(t_id, payloads) for t_id, payloads in self.networking.remote_function_calls.items()
                               if t_id in self.t_ids_to_reschedule]
            for t_id, payloads in remote_payloads:
                for payload in payloads:
                    fallback_tasks.append(
                        self.run_fallback_function(
                            t_id,
                            payload,
                            internal=True
                        )
                    )

        await self.sync_workers(msg_type=MessageType.AriaFallbackStart,
                                message=(self.id, ),
                                serializer=Serializer.MSGPACK,
                                send_async_migration_message=True)
        if fallback_tasks:
            await asyncio.gather(*fallback_tasks)

        # logging.info(
        #     f'Epoch: {self.sequencer.epoch_counter} '
        #     f'Fallback strategy done waiting for peers'
        # )

        await self.sync_workers(msg_type=MessageType.AriaFallbackDone,
                                message=(self.id, ),
                                serializer=Serializer.MSGPACK,
                                send_async_migration_message=True)

    async def unlock_tid(self, t_id_to_unlock: int):
        if t_id_to_unlock in self.fallback_locking_event_map:
            async with self.fallback_locking_event_map_lock:
                self.fallback_locking_event_map[t_id_to_unlock].set()
        else:
            logging.error(f"Unlock tid {t_id_to_unlock} not found. But should exist!")

    async def send_responses(
            self,
            current_sequence_t_ids: list[SequencedItem],
            client_responses: dict[int, str],
            aborted_events: dict[int, str]
    ):
        for sequenced_item in current_sequence_t_ids:
            t_id = sequenced_item.t_id
            request_id = sequenced_item.payload.request_id
            operator_name = sequenced_item.payload.operator_name
            partition = sequenced_item.payload.partition
            if t_id in aborted_events:
                await self.egress.send(key=request_id,
                                       value=msgpack_serialization(aborted_events[t_id]),
                                       operator_name=operator_name,
                                       partition=partition)
            elif t_id in client_responses:
                await self.egress.send(key=request_id,
                                       value=msgpack_serialization(client_responses[t_id]),
                                       operator_name=operator_name,
                                       partition=partition)
        await self.egress.send_batch()
        self.wait_responses_to_be_sent.set()


    async def fallback_unlock(self, t_id: int, success: bool):
        # Release the locks for local
        await self.unlock_tid(t_id)
        # Release the locks for remote participants
        if t_id in self.networking.chain_participants and self.networking.chain_participants[t_id]:
            async with asyncio.TaskGroup() as tg:
                for participant in self.networking.chain_participants[t_id]:
                    tg.create_task(self.networking.send_message(
                        self.peers[participant][0],
                        self.peers[participant][2],
                        msg=(t_id, success),
                        msg_type=MessageType.Unlock,
                        serializer=Serializer.MSGPACK)
                    )

    async def sync_workers(self,
                           msg_type: MessageType,
                           message: tuple | bytes,
                           serializer: Serializer = Serializer.MSGPACK,
                           send_async_migration_message: bool = False):
        await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT+1,
                                           msg=message,
                                           msg_type=msg_type,
                                           serializer=serializer)
        if send_async_migration_message:
            await self.send_async_migrate_batch()
        await self.sync_workers_event[msg_type].wait()
        self.sync_workers_event[msg_type].clear()

import asyncio
import os
import concurrent.futures

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
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
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
# snapshot each N epochs
SNAPSHOT_FREQUENCY: int = int(os.getenv('SNAPSHOT_FREQUENCY_SEC', 10))
SNAPSHOTTING_THREADS: int = int(os.getenv('SNAPSHOTTING_THREADS', 4))
SEQUENCE_MAX_SIZE: int = int(os.getenv('SEQUENCE_MAX_SIZE', 1_000))
USE_FALLBACK_CACHE: bool = bool(os.getenv('USE_FALLBACK_CACHE', True))
KAFKA_URL: str = os.environ['KAFKA_URL']


class AriaProtocol(BaseTransactionalProtocol):

    def __init__(self,
                 worker_id,
                 peers: dict[int, tuple[str, int, int]],
                 networking: NetworkingManager,
                 registered_operators: dict[OperatorPartition, Operator],
                 topic_partitions: list[TopicPartition],
                 state: InMemoryOperatorState | Stateless,
                 async_snapshots: AsyncSnapshotsMinio,
                 topic_partition_offsets: dict[OperatorPartition, int] = None,
                 output_offsets: dict[OperatorPartition, int] = None,
                 epoch_counter: int = 0,
                 t_counter: int = 0,
                 request_id_to_t_id_map: dict[bytes, int] = None,
                 restart_after_recovery: bool = False):

        if topic_partition_offsets is None:
            topic_partition_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}
        if output_offsets is None:
            output_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}

        self.id = worker_id

        self.topic_partitions = topic_partitions
        self.networking = networking

        self.local_state: InMemoryOperatorState | Stateless = state
        self.aio_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()
        self.background_functions: AIOTaskScheduler = AIOTaskScheduler()
        self.async_snapshots: AsyncSnapshotsMinio = async_snapshots

        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int, int]] = peers
        self.topic_partition_offsets: dict[OperatorPartition, int] = topic_partition_offsets
        # worker_id: set of aborted t_ids
        self.concurrency_aborts_everywhere: set[int] = set()
        self.t_ids_to_reschedule: set[int] = set()
        # t_id: (request_id, response)
        self.response_buffer: dict[int, tuple[bytes, str, str, int]] = {}

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
                                                          worker_id=self.id,
                                                          kafka_url=KAFKA_URL,
                                                          sequence_max_size=SEQUENCE_MAX_SIZE,
                                                          epoch_interval_ms=1)

        self.egress: StyxKafkaBatchEgress = StyxKafkaBatchEgress(output_offsets, restart_after_recovery)
        # Primary task used for processing
        self.function_scheduler_task: asyncio.Task = ...
        self.communication_task: asyncio.Task = ...

        self.snapshot_counter = 0

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
            MessageType.DeterministicReordering: asyncio.Lock()
        }

        self.remote_wants_to_proceed: bool = False
        self.currently_processing: bool = False

        self.snapshot_timer: float = -1.0

        self.started = asyncio.Event()

        # performance measurements
        # self.function_running_time = 0
        # self.chain_completion_time = 0
        # self.serialization_time = 0
        # self.sequencing_time = 0
        # self.conflict_resolution_time = 0
        # self.commit_time = 0
        # self.sync_time = 0
        # self.snapshot_time = 0
        # self.fallback_time = 0

    async def stop(self):
        await self.ingress.stop()
        await self.egress.stop()
        await self.aio_task_scheduler.close()
        self.function_scheduler_task.cancel()
        self.communication_task.cancel()
        try:
            await self.function_scheduler_task
            await self.communication_task
        except asyncio.CancelledError:
            logging.warning("Protocol coroutines stopped")
        logging.info(f"Active tasks: {asyncio.all_tasks()}")
        logging.warning("Aria protocol stopped")

    def start(self):
        self.function_scheduler_task = asyncio.create_task(self.function_scheduler())
        self.communication_task = asyncio.create_task(self.communication_protocol())
        logging.warning("Aria protocol started")
        self.snapshot_timer = timer()

    async def run_function(
            self,
            t_id: int,
            payload: RunFuncPayload,
            internal: bool = False,
            fallback_mode: bool = False
    ) -> bool:
        logging.info(f"Running function: {payload.function_name} with T_ID {t_id} with params {payload.params}"
                     f" and ack payload {payload.ack_payload}")
        success: bool = True
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]

        response = await operator_partition.run_function(
            payload.key,
            t_id,
            payload.request_id,
            payload.timestamp,
            payload.function_name,
            payload.partition,
            payload.ack_payload,
            fallback_mode,
            USE_FALLBACK_CACHE,
            payload.params,
            self
        )
        # If exception we need to add it to the application logic aborts
        if isinstance(response, Exception):
            success = False
            response = str(response)

            if not internal:
                self.networking.transaction_failed(t_id)
        # If request response send the response
        # if payload.response_socket is not None:
        #     self.router.write(
        #         (payload.response_socket, self.networking.encode_message(
        #             msg=response,
        #             msg_type=MessageType.RunFunRqRsRemote,
        #             serializer=Serializer.MSGPACK
        #         ))
        #     )
        # If we have a response, and it's not part of the chain send it to kafka
        # elif response is not None and not internal_msg:
        if response is not None:
            # If fallback add it to the fallback replies else to the response buffer
            self.response_buffer[t_id] = (payload.request_id, response, payload.operator_name, payload.partition)
        return success

    def take_snapshot(self, pool: concurrent.futures.ProcessPoolExecutor):
        is_snapshot_time: bool = timer() > self.snapshot_timer + SNAPSHOT_FREQUENCY
        if is_snapshot_time and not self.async_snapshots.snapshot_in_progress:
            logging.warning(f"Taking snapshot at epoch: {self.sequencer.epoch_counter}")
            self.snapshot_timer = timer()
            if InMemoryOperatorState.__name__ == self.local_state.__class__.__name__:
                loop = asyncio.get_running_loop()
                self.async_snapshots.start_snapshotting()
                data = self.local_state.get_data_for_snapshot()
                for operator_partition in self.registered_operators.keys():
                    operator_name, partition = operator_partition
                    loop.run_in_executor(pool,
                                         self.async_snapshots.store_snapshot,
                                         self.async_snapshots.snapshot_id,
                                         f"data/{operator_name}/{partition}/{self.async_snapshots.snapshot_id}.bin",
                                         (msgpack.decode(msgpack.encode(self.topic_partition_offsets[operator_partition])),
                                          msgpack.decode(msgpack.encode(self.egress.topic_partition_output_offsets[operator_partition])),
                                          msgpack.decode(msgpack.encode(data[operator_partition])))
                                         ).add_done_callback(self.async_snapshots.snapshot_completed_callback)
                loop.run_in_executor(pool,
                                     self.async_snapshots.store_snapshot,
                                     self.async_snapshots.snapshot_id,
                                     f"sequencer/{self.async_snapshots.snapshot_id}.bin",
                                     msgpack.decode(msgpack.encode((self.sequencer.epoch_counter, self.sequencer.t_counter)))
                                     ).add_done_callback(self.async_snapshots.snapshot_completed_callback)
                self.local_state.clear_delta_map()
            else:
                logging.warning("Snapshot currently supported only for in-memory and incremental operator state")

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
                    logging.info('CALLED RUN FUN FROM PEER')
                    (t_id, request_id, operator_name, function_name,
                     key, partition, timestamp, fallback_enabled, params, ack) = self.networking.decode_message(data)
                    payload = RunFuncPayload(request_id=request_id, key=key, timestamp=timestamp,
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
                                payload,
                                internal=True
                            )
                        )
            case MessageType.RunFunRqRsRemote:
                logging.error('REQUEST RESPONSE HAS BEEN DEPRECATED')
            case MessageType.AriaCommit:
                async with self.networking_locks[message_type]:
                    (self.concurrency_aborts_everywhere, self.total_processed_seq_size,
                     self.max_t_counter) = self.networking.decode_message(data)
                    self.sync_workers_event[message_type].set()
            case (MessageType.AriaFallbackDone | MessageType.AriaFallbackStart | MessageType.SyncCleanup):
                async with self.networking_locks[message_type]:
                    self.sync_workers_event[message_type].set()
            case MessageType.AriaProcessingDone:
                async with self.networking_locks[message_type]:
                    (self.networking.logic_aborts_everywhere, ) = self.networking.decode_message(data)
                    self.sync_workers_event[message_type].set()
            case MessageType.Ack:
                async with self.networking_locks[message_type]:
                    (ack_id, fraction_str,
                     chain_participants, partial_node_count) = self.networking.decode_message(data)
                    self.networking.add_ack_fraction_str(ack_id, fraction_str,
                                                         chain_participants, partial_node_count)
            case MessageType.AckCache:
                async with self.networking_locks[message_type]:
                    (ack_id, ) = self.networking.decode_message(data)
                    self.networking.add_ack_cnt(ack_id)
            case MessageType.ChainAbort:
                async with self.networking_locks[message_type]:
                    (ack_id, exception_str, request_id) = self.networking.decode_message(data)
                    self.networking.abort_chain(ack_id, exception_str, request_id)
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
            case _:
                logging.error(f"Aria protocol: Non supported command message type: {message_type}")

    async def function_scheduler(self):
        logging.warning('STARTED function scheduler')
        with (concurrent.futures.ProcessPoolExecutor(SNAPSHOTTING_THREADS) as pool):
            await self.started.wait()
            while True:
                # need to sleep to allow the kafka consumer coroutine to read data
                await asyncio.sleep(0)
                async with self.sequencer.lock:
                    # GET SEQUENCE
                    sequence: list[SequencedItem] = self.sequencer.get_epoch()
                    if sequence or self.remote_wants_to_proceed:
                        self.currently_processing = True
                        # logging.warning(f'Sequence tids: {[pld.t_id for pld in sequence]}')
                        logging.info(f'{self.id} ||| Epoch: {self.sequencer.epoch_counter} starts')
                        # Run all the epochs functions concurrently
                        epoch_start = timer()
                        logging.info(f'{self.id} ||| Running {len(sequence)} functions...')
                        # async with self.snapshot_state_lock:
                        if sequence:
                            start_wal = timer()
                            sequence_to_log = msgpack_serialization({seq_item.payload.request_id: seq_item.t_id
                                                                     for seq_item in sequence})
                            await self.egress.send_message_to_topic(key=msgpack_serialization(self.sequencer.epoch_counter),
                                                                    message=sequence_to_log,
                                                                    topic='sequencer-wal')
                            end_wal = timer()
                            logging.info(f"Write to WAL successful at epoch: {self.sequencer.epoch_counter} | took: {round((end_wal - start_wal) * 1000, 4)}ms")
                            async with asyncio.TaskGroup() as tg:
                                for sequenced_item in sequence:
                                    tg.create_task(self.run_function(sequenced_item.t_id, sequenced_item.payload))
                            # function_running_done = timer()
                            # self.function_running_time += function_running_done - epoch_start
                            # Wait for chains to finish
                            logging.info(f'{self.id} ||| '
                                         f'Waiting on chained {len(self.networking.waited_ack_events)} functions...')
                            async with asyncio.TaskGroup() as tg:
                                for ack in self.networking.waited_ack_events.values():
                                    tg.create_task(ack.wait())
                        # function_chains_done = timer()
                        # self.chain_completion_time += function_chains_done - function_running_done
                        # wait for all peers to be done processing (needed to know the aborts)
                        await self.sync_workers(msg_type=MessageType.AriaProcessingDone,
                                                message=(self.networking.logic_aborts_everywhere, ),
                                                serializer=Serializer.PICKLE)
                        # sync_time = timer()
                        # self.sync_time += sync_time - function_chains_done
                        logging.info(f'{self.id} ||| '
                                     f'logic_aborts_everywhere: {self.networking.logic_aborts_everywhere}')
                        # HERE WE KNOW ALL THE LOGIC ABORTS
                        # removing the global logic abort transactions from the commit phase
                        self.local_state.remove_aborted_from_rw_sets(self.networking.logic_aborts_everywhere)
                        # Check for local state conflicts
                        logging.info(f'{self.id} ||| Checking conflicts...')
                        if CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DEFAULT_SERIALIZABLE:
                            concurrency_aborts: set[int] = self.local_state.check_conflicts()
                        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DETERMINISTIC_REORDERING:
                            await self.sync_workers(msg_type=MessageType.DeterministicReordering,
                                                    message=(self.local_state.reads,
                                                             self.local_state.write_sets,
                                                             self.local_state.read_sets),
                                                    serializer=Serializer.PICKLE)
                            concurrency_aborts: set[int] = self.local_state.check_conflicts_deterministic_reordering()
                        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SNAPSHOT_ISOLATION:
                            concurrency_aborts: set[int] = self.local_state.check_conflicts_snapshot_isolation()
                        else:
                            logging.error('This conflict detection method number is not a valid number')
                            exit()
                        # self.concurrency_aborts_everywhere |= concurrency_aborts
                        # conflict_resolution_time = timer()
                        # self.conflict_resolution_time += conflict_resolution_time - sync_time
                        # Notify peers that we are ready to commit
                        logging.info(f'{self.id} ||| Notify peers...')
                        await self.sync_workers(msg_type=MessageType.AriaCommit,
                                                message=(concurrency_aborts,
                                                         self.sequencer.t_counter,
                                                         len(sequence)),
                                                serializer=Serializer.PICKLE)
                        # await self.send_commit_to_peers(concurrency_aborts, len(sequence))
                        # HERE WE KNOW ALL THE CONCURRENCY ABORTS
                        # Wait for remote to be ready to commit
                        logging.info(f'{self.id} ||| Waiting on remote commits...')
                        # await self.wait_commit()
                        # sync_time = timer()
                        # self.sync_time += sync_time - conflict_resolution_time
                        # Gather the remote concurrency aborts
                        # Commit the local while taking into account the aborts from remote
                        logging.info(f'{self.id} ||| Starting commit!')
                        self.local_state.commit(self.concurrency_aborts_everywhere)
                        logging.info(f'{self.id} ||| Sequence committed!')
                        # commit_done = timer()
                        # self.commit_time += commit_done - sync_time

                        self.t_ids_to_reschedule = (self.concurrency_aborts_everywhere -
                                                    self.networking.logic_aborts_everywhere)

                        current_completed_t_ids: list[int] = [seq_i.t_id for seq_i in sequence
                                                              if seq_i.t_id not in self.concurrency_aborts_everywhere]
                        self.aio_task_scheduler.create_task(self.send_responses(
                            current_completed_t_ids,
                            msgpack.decode(msgpack.encode(self.response_buffer)),
                            msgpack.decode(msgpack.encode(self.networking.aborted_events))
                        ))
                        self.response_buffer = {}
                        # total_processed_functions: int = sum(self.processed_seq_size.values()) + len(sequence)
                        abort_rate: float = len(self.concurrency_aborts_everywhere) / self.total_processed_seq_size

                        if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
                            # Run Calvin
                            logging.info(
                                f'{self.id} ||| Epoch: {self.sequencer.epoch_counter} '
                                f'Abort percentage: {int(abort_rate * 100)}% initiating fallback strategy...\n'
                                # f'reads: {self.local_state.reads}\n'
                                # f'writes: {self.local_state.writes}\n'
                                # f'c_aborts: {self.concurrency_aborts_everywhere - self.logic_aborts_everywhere}'
                            )
                            # start_fallback = timer()
                            # logging.warning(f'Logic abort ti_ds: {logic_aborts_everywhere}')
                            await self.run_fallback_strategy()
                            self.concurrency_aborts_everywhere = set()
                            self.t_ids_to_reschedule = set()
                            # end_fallback = timer()
                            # self.fallback_time += end_fallback - start_fallback

                        for sequenced_item in sequence:
                            # get the kafka offsets of the commited transactions in the batch
                            # hopefully offsets came in the correct order from the python kafka client
                            # if not we should modify the sequencer
                            if sequenced_item.t_id not in self.concurrency_aborts_everywhere:
                                payload = sequenced_item.payload
                                self.topic_partition_offsets[(payload.operator_name, payload.partition)] = (
                                    max(
                                        payload.kafka_offset,
                                        self.topic_partition_offsets[(payload.operator_name, payload.partition)]
                                    )
                                )

                        # Cleanup
                        # start_seq_incr = timer()
                        self.sequencer.increment_epoch(
                            self.max_t_counter,
                            self.t_ids_to_reschedule
                        )
                        # end_seq_incr = timer()
                        # self.sequencing_time += end_seq_incr - start_seq_incr
                        # self.t_counters = {}
                        # Re-sequence the aborted transactions due to concurrency
                        epoch_end = timer()

                        logging.info(
                            f'{self.id} ||| Epoch: {self.sequencer.epoch_counter - 1} done in '
                            f'{round((epoch_end - epoch_start) * 1000, 4)}ms '
                            f'global logic aborts: {len(self.networking.logic_aborts_everywhere)} '
                            f'concurrency aborts for next epoch: {len(self.concurrency_aborts_everywhere)} '
                            f'abort rate: {abort_rate}'
                        )
                        # logging.warning(f'Epoch: {self.sequencer.epoch_counter - 1} done in '
                        #                 f'{round((epoch_end - epoch_start) * 1000, 4)}ms '
                        #                 f'function_running_time: {self.function_running_time}\n'
                        #                 f'chain_completion_time: {self.chain_completion_time}\n'
                        #                 f'serialization_time: {self.serialization_time}\n'
                        #                 f'sequencing_time: {self.sequencing_time}\n'
                        #                 f'conflict_resolution_time: {self.conflict_resolution_time}\n'
                        #                 f'commit_time: {self.commit_time}\n'
                        #                 f'sync_time: {self.sync_time}\n'
                        #                 f'snapshot_time: {self.snapshot_time}\n'
                        #                 f'fallback_time: {self.fallback_time}\n')
                        self.cleanup_after_epoch()
                        # start_sn = timer()
                        self.take_snapshot(pool)
                        await self.sync_workers(msg_type=MessageType.SyncCleanup,
                                                message=b'',
                                                serializer=Serializer.NONE)
                        # end_sn = timer()
                        # self.snapshot_time += end_sn - start_sn

    def cleanup_after_epoch(self):
        self.concurrency_aborts_everywhere.clear()
        self.t_ids_to_reschedule.clear()
        self.networking.cleanup_after_epoch()
        self.local_state.cleanup()
        self.response_buffer.clear()
        self.waiting_on_transactions.clear()
        self.fallback_locking_event_map.clear()
        self.remote_wants_to_proceed = False
        self.currently_processing = False

    async def run_fallback_function(
            self,
            t_id: int,
            payload: RunFuncPayload,
            internal: bool = False
    ):
        # Wait for all transactions that this transaction depends on to finish
        if t_id in self.waiting_on_transactions:
            if self.waiting_on_transactions[t_id]:
                tasks = [self.fallback_locking_event_map[dependency_t_id].wait()
                         for dependency_t_id in self.waiting_on_transactions[t_id]
                         if dependency_t_id in self.fallback_locking_event_map]
                await asyncio.gather(*tasks)

        # Run transaction
        success = await self.run_function(t_id, payload, internal=internal, fallback_mode=True)
        if not internal:
            # if root of chain
            if t_id in self.networking.waited_ack_events:
                # wait on ack of parts
                await self.networking.waited_ack_events[t_id].wait()
            transaction_failed: bool = t_id in self.networking.aborted_events or not success

            if not transaction_failed:
                self.local_state.commit_fallback_transaction(t_id)

            await self.fallback_unlock(t_id, success=not transaction_failed)

            # if network failed send the buffered exception else the response buffer no need for this complexity
            if t_id in self.networking.aborted_events:
                exception_str, request_id = self.networking.aborted_events[t_id]
                await self.egress.send_immediate(key=request_id,
                                                 value=msgpack_serialization(exception_str),
                                                 operator_name=payload.operator_name,
                                                 partition=payload.partition)
            elif t_id in self.response_buffer:
                request_id, exception_str, operator_name, partition = self.response_buffer[t_id]
                await self.egress.send_immediate(key=request_id,
                                                 value=msgpack_serialization(exception_str),
                                                 operator_name=operator_name,
                                                 partition=partition)

    async def run_fallback_strategy(self):
        logging.info('Starting fallback strategy...')

        (self.waiting_on_transactions,
         self.fallback_locking_event_map) = self.local_state.get_dep_transactions(self.t_ids_to_reschedule)

        fallback_tasks = []
        aborted_sequence: list[SequencedItem] = self.sequencer.get_aborted_sequence(self.t_ids_to_reschedule)
        self.networking.clear_aborted_events_for_fallback()
        for sequenced_item in aborted_sequence:
            # current worker is the root of the chain
            if USE_FALLBACK_CACHE:
                self.networking.reset_ack_for_fallback_cache(sequenced_item.t_id)
            else:
                self.networking.reset_ack_for_fallback(sequenced_item.t_id)
            fallback_tasks.append(
                self.run_fallback_function(
                    sequenced_item.t_id,
                    sequenced_item.payload
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
                                message=b'',
                                serializer=Serializer.NONE)
        if fallback_tasks:
            async with asyncio.TaskGroup() as tg:
                for fallback_task in fallback_tasks:
                    tg.create_task(fallback_task)
        logging.info(
            f'Epoch: {self.sequencer.epoch_counter} '
            f'Fallback strategy done waiting for peers'
        )
        await self.sync_workers(msg_type=MessageType.AriaFallbackDone,
                                message=b'',
                                serializer=Serializer.NONE)

    async def unlock_tid(self, t_id_to_unlock: int):
        if t_id_to_unlock in self.fallback_locking_event_map:
            async with self.fallback_locking_event_map_lock:
                self.fallback_locking_event_map[t_id_to_unlock].set()
        else:
            logging.error(f"Unlock tid {t_id_to_unlock} not found. But should exist!")

    async def send_responses(
            self,
            current_sequence_t_ids: list[int],
            response_buffer: dict[int, tuple[bytes, str, str, int]],
            aborted_events: dict[int, [str, bytes]]
    ):
        for t_id in current_sequence_t_ids:
            if t_id in aborted_events:
                exception_str, request_id = aborted_events[t_id]
                _, _, operator_name, partition = response_buffer[t_id]
                await self.egress.send(key=request_id,
                                       value=msgpack_serialization(exception_str),
                                       operator_name=operator_name,
                                       partition=partition)
            elif t_id in response_buffer:
                request_id, response, operator_name, partition = response_buffer[t_id]
                await self.egress.send(key=request_id,
                                       value=msgpack_serialization(response),
                                       operator_name=operator_name,
                                       partition=partition)
        await self.egress.send_batch()

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
                           serializer: Serializer = Serializer.MSGPACK):
        await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT+1,
                                           msg=message,
                                           msg_type=msg_type,
                                           serializer=serializer)
        await self.sync_workers_event[msg_type].wait()
        self.sync_workers_event[msg_type].clear()

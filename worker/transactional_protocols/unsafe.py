import asyncio
import time
import concurrent.futures

from timeit import default_timer as timer

from aiozmq import ZmqStream
from aiokafka import TopicPartition
from styx.common.base_protocol import BaseTransactionalProtocol
from styx.common.message_types import MessageType
from styx.common.networking import NetworkingManager
from styx.common.operator import Operator
from styx.common.serialization import Serializer, msgpack_serialization
from styx.common.logging import logging
from styx.common.run_func_payload import RunFuncPayload

from worker.egress.styx_kafka_egress import StyxKafkaEgress
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.ingress.styx_kafka_ingress_pure import StyxKafkaIngressPure
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.operator_state.unsafe_state import UnsafeOperatorState
from worker.util.aio_task_scheduler import AIOTaskScheduler


# "Unaligned" unaligned should be with a write ahead log and sequence t_ids
SNAPSHOT_TYPE = "Aligned"


class UnsafeProtocol(BaseTransactionalProtocol):

    def __init__(self,
                 worker_id,
                 peers: dict[int, tuple[str, int]],
                 networking: NetworkingManager,
                 protocol_router: ZmqStream,
                 registered_operators: dict[tuple[str, int], Operator],
                 topic_partitions: list[TopicPartition],
                 state: InMemoryOperatorState | Stateless,
                 async_snapshots: AsyncSnapshotsMinio,
                 snapshot_metadata: dict = None):

        self.id = worker_id

        self.topic_partitions = topic_partitions
        self.topic_partition_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}
        self.networking = networking
        self.router = protocol_router

        self.local_state: InMemoryOperatorState | Stateless = state
        self.network_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()
        self.function_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()

        self.async_snapshots: AsyncSnapshotsMinio = async_snapshots
        self.peers: dict[int, tuple[str, int]] = peers
        self.registered_operators = registered_operators

        self.egress: StyxKafkaEgress = StyxKafkaEgress()

        self.ingress: StyxKafkaIngressPure = StyxKafkaIngressPure(networking=self.networking,
                                                                  worker_id=worker_id,
                                                                  n_workers=len(peers)+1)

        self.function_scheduler_task: asyncio.Task = ...
        self.communication_task: asyncio.Task = ...

        self.snapshot_alignment_event: asyncio.Event = asyncio.Event()
        self.snapshot_alignment_event.set()

        self.align_start: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                      for peer_id in self.peers.keys()}
        self.align_end: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                    for peer_id in self.peers.keys()}

    async def run_function(
            self,
            t_id: int,
            payload: RunFuncPayload,
            internal_msg: bool = False
    ):
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]

        response = await operator_partition.run_function(
            payload.key,
            t_id,
            payload.request_id,
            payload.timestamp,
            payload.function_name,
            payload.ack_payload,
            False,
            payload.params,
            self
        )
        # If request response send the response
        if payload.response_socket is not None:
            self.router.write(
                (payload.response_socket, self.networking.encode_message(
                    msg=response,
                    msg_type=MessageType.RunFunRqRsRemote,
                    serializer=Serializer.MSGPACK
                ))
            )
        # If we have a response, and it's not part of the chain send it to kafka
        elif response is not None and not internal_msg:
            # If Exception transform it to string for Kafka
            if t_id in self.networking.waited_ack_events:
                await self.networking.waited_ack_events[t_id].wait()
            if isinstance(response, Exception):
                kafka_response = str(response)
            elif t_id in self.networking.aborted_events:
                kafka_response = self.networking.aborted_events[t_id]
            else:
                kafka_response = response
            await self.egress.send(key=payload.request_id, value=msgpack_serialization(kafka_response))

    async def process_unsafe_function(self, payload, t_id):
        await self.run_function(t_id=t_id, payload=payload)

    async def function_scheduler(self):
        while True:
            await self.snapshot_alignment_event.wait()
            payload, t_id = await self.ingress.get_message()
            self.function_task_scheduler.create_task(self.process_unsafe_function(payload, t_id))

    async def communication_protocol(self):
        self.ingress.start(self.topic_partitions, self.topic_partition_offsets)
        logging.warning('Ingress started')
        await self.egress.start(self.id)
        logging.warning('Egress started')
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            while True:
                resp_adr, data = await self.router.read()
                self.network_task_scheduler.create_task(self.protocol_tcp_controller(data, resp_adr, pool))

    async def protocol_tcp_controller(self, data: bytes, resp_adr, pool: concurrent.futures.ProcessPoolExecutor):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            case MessageType.RunFunRemote:
                logging.info('CALLED RUN FUN FROM PEER')
                message = self.networking.decode_message(data)
                t_id, request_id, operator_name, function_name, key, partition, timestamp, params, ack = message
                payload = RunFuncPayload(request_id=request_id, key=key,
                                         operator_name=operator_name, partition=partition,
                                         function_name=function_name, params=params, ack_payload=ack)
                self.function_task_scheduler.create_task(
                    self.run_function(
                        t_id,
                        payload,
                        internal_msg=True
                    ))
            case MessageType.RunFunRqRsRemote:
                logging.info('CALLED RUN FUN RQ RS FROM PEER')
                message = self.networking.decode_message(data)
                t_id, request_id, operator_name, function_name, key, partition, timestamp, params = message
                payload = RunFuncPayload(request_id=request_id, key=key,
                                         operator_name=operator_name, partition=partition,
                                         function_name=function_name, params=params)
                self.function_task_scheduler.create_task(
                    self.run_function(
                        t_id,
                        payload,
                        internal_msg=True
                    ))
            case MessageType.Ack:
                message = self.networking.decode_message(data)
                ack_id, fraction_str, exception_str = message
                if fraction_str != '-1':
                    self.networking.add_ack_fraction_str(ack_id, fraction_str)
                    logging.info(f'ACK received for {ack_id} part: {fraction_str}')
                else:
                    logging.info(f'ABORT ACK received for {ack_id}')
                    self.networking.abort_chain(ack_id, exception_str)
            case MessageType.SnapMarker:
                logging.warning('Marker received')
                start1 = timer()
                self.snapshot_alignment_event.clear()
                start = timer()
                logging.warning(f'Passed lock took: {start - start1}')
                await self.function_task_scheduler.wait_all()
                end = timer()
                logging.warning(f'Waiting for inflight events took: {end - start}')
                await self.wait_align_start_sync()
                self.take_snapshot(pool)
                await self.wait_align_end_sync()
                end2 = timer()
                logging.warning(f'Waiting for alignment: {end2 - end}')
            case MessageType.AlignStart:
                message = self.networking.decode_message(data)
                remote_worker_id = message[0]
                self.align_start[remote_worker_id].set()
            case MessageType.AlignEnd:
                message = self.networking.decode_message(data)
                remote_worker_id = message[0]
                self.align_end[remote_worker_id].set()
            case _:
                logging.error(f"Unsafe protocol: Non supported command message type: {message_type}")

    def take_snapshot(self, pool: concurrent.futures.ProcessPoolExecutor):
        if UnsafeOperatorState.__name__ == self.local_state.__class__.__name__:
            loop = asyncio.get_running_loop()
            start = time.time() * 1000
            data = {"data":  self.local_state.data,
                    "metadata": {"offsets": self.topic_partition_offsets,
                                 "t_counter": self.ingress.t_counter,
                                 "output_offset": self.egress.output_offset}}
            loop.run_in_executor(pool, self.async_snapshots.store_snapshot, self.async_snapshots.snapshot_id,
                                 self.id, data, self.networking.encode_message,
                                 start).add_done_callback(self.async_snapshots.increment_snapshot_id)
        else:
            logging.warning("Snapshot currently supported only for in-memory operator state")

    async def wait_align_start_sync(self):
        await self.send_sync_to_peers(MessageType.AlignStart)  # ALIGN_START
        wait_remote_fallback = [event.wait()
                                for event in self.align_start.values()]
        await asyncio.gather(*wait_remote_fallback)
        [event.clear() for event in self.align_start.values()]

    async def wait_align_end_sync(self):
        await self.send_sync_to_peers(MessageType.AlignEnd)  # ALIGN_END
        wait_remote_fallback = [event.wait()
                                for event in self.align_end.values()]
        await asyncio.gather(*wait_remote_fallback)
        [event.clear() for event in self.align_end.values()]

    async def send_sync_to_peers(self, phase: int):
        msg = (self.id, )
        serializer = Serializer.MSGPACK
        tasks = [self.networking.send_message(
            url[0],
            url[1],
            msg=msg,
            msg_type=phase,
            serializer=serializer
        )
            for worker_id, url in self.peers.items()]
        await asyncio.gather(*tasks)

    async def stop(self, *args, **kwargs):
        await self.ingress.stop()
        await self.egress.stop()
        await self.function_task_scheduler.close()
        await self.network_task_scheduler.close()
        self.function_scheduler_task.cancel()
        self.communication_task.cancel()
        try:
            await self.function_scheduler_task
            await self.communication_task
        except asyncio.CancelledError:
            logging.warning("Protocol coroutines stopped")
        logging.warning("Unsafe protocol stopped")

    def start(self, *args, **kwargs):
        self.function_scheduler_task = asyncio.create_task(self.function_scheduler())
        self.communication_task = asyncio.create_task(self.communication_protocol())
        logging.warning("Unsafe protocol started")

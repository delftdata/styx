import asyncio
import multiprocessing
import os
import time
from timeit import default_timer as timer

import aiozmq
import uvloop
import zmq
from aiokafka import TopicPartition
from aiozmq import ZmqStream

from styx.common.local_state_backends import LocalStateBackend
from styx.common.logging import logging
from styx.common.networking import NetworkingManager, MessagingMode
from styx.common.operator import Operator
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.message_types import MessageType

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.operator_state.unsafe_state import UnsafeOperatorState
from worker.transactional_protocols.aria import AriaProtocol
from worker.transactional_protocols.unsafe import UnsafeProtocol
from worker.util.aio_task_scheduler import AIOTaskScheduler

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
        self.id: int = -1
        self.networking = NetworkingManager(self.server_port)
        self.protocol_networking = NetworkingManager(self.protocol_port,
                                                     size=4,
                                                     mode=MessagingMode.PROTOCOL_PROTOCOL)
        self.router: ZmqStream = ...
        self.protocol_router: ZmqStream = ...

        self.operator_state_backend: LocalStateBackend = ...
        self.registered_operators: dict[tuple[str, int], Operator] = {}
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

    async def worker_controller(self, data: bytes):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case MessageType.ReceiveExecutionPlan:
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

                self.networking = NetworkingManager(self.server_port)
                self.protocol_networking = NetworkingManager(self.protocol_port,
                                                             size=4,
                                                             mode=MessagingMode.PROTOCOL_PROTOCOL)
                self.protocol_networking.set_worker_id(self.id)
                self.networking.set_worker_id(self.id)

                message = self.networking.decode_message(data)
                self.dns, self.peers, self.operator_state_backend, snapshot_id = message
                del self.peers[self.id]

                metadata: dict | None = None
                if self.operator_state_backend is LocalStateBackend.DICT:
                    state, metadata = self.async_snapshots.retrieve_snapshot(snapshot_id)
                    self.attach_state_to_operators_after_snapshot(state)

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.protocol_networking,
                                                                protocol_router=self.protocol_router,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                snapshot_metadata=metadata,
                                                                restart_after_recovery=True)

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
                self.id, worker_operators, self.dns, self.peers, self.operator_state_backend, snapshot_id = message
                del self.peers[self.id]

                operator: Operator
                for tup in worker_operators:
                    operator, partition = tup
                    self.registered_operators[(operator.name, partition)] = operator
                    if INGRESS_TYPE == 'KAFKA':
                        self.topic_partitions.append(TopicPartition(operator.name, partition))

                metadata: dict | None = None
                if self.operator_state_backend is LocalStateBackend.DICT:
                    self.async_snapshots = AsyncSnapshotsMinio(self.id, snapshot_id=snapshot_id+1)
                    state, metadata = self.async_snapshots.retrieve_snapshot(snapshot_id)
                    self.attach_state_to_operators_after_snapshot(state)

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.protocol_networking,
                                                                protocol_router=self.protocol_router,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                snapshot_metadata=metadata,
                                                                restart_after_recovery=True)
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
                self.function_execution_protocol.start()
                logging.warning(f'Worker recovered and ready at : {time.time()*1000}')
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    def attach_state_to_operators_after_snapshot(self, data):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_names)
            self.local_state.set_data_from_snapshot(data)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.protocol_networking, self.dns)

    def attach_state_to_operators(self):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.async_snapshots = AsyncSnapshotsMinio(self.id)
            if PROTOCOL == Protocols.Aria:
                self.local_state = InMemoryOperatorState(operator_names)
            else:
                self.local_state = UnsafeOperatorState(operator_names)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.protocol_networking, self.dns)

    async def handle_execution_plan(self, message):
        worker_operators, self.dns, self.peers, self.operator_state_backend = message
        del self.peers[self.id]
        operator: Operator
        for tup in worker_operators:
            operator, partition = tup
            self.registered_operators[(operator.name, partition)] = operator
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions.append(TopicPartition(operator.name, partition))
        self.attach_state_to_operators()
        if PROTOCOL == Protocols.Aria:
            self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                            peers=self.peers,
                                                            networking=self.protocol_networking,
                                                            protocol_router=self.protocol_router,
                                                            registered_operators=self.registered_operators,
                                                            topic_partitions=self.topic_partitions,
                                                            state=self.local_state,
                                                            async_snapshots=self.async_snapshots)
        else:
            self.function_execution_protocol = UnsafeProtocol(worker_id=self.id,
                                                              peers=self.peers,
                                                              networking=self.protocol_networking,
                                                              protocol_router=self.protocol_router,
                                                              registered_operators=self.registered_operators,
                                                              topic_partitions=self.topic_partitions,
                                                              state=self.local_state,
                                                              async_snapshots=self.async_snapshots)

        self.function_execution_protocol.start()

        logging.info(
            f'Registered operators: {self.registered_operators} \n'
            f'Peers: {self.peers} \n'
            f'Operator locations: {self.dns}'
        )

    async def start_tcp_service(self):
        try:
            self.router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{self.server_port}",
                                                         high_read=0, high_write=0)
            self.protocol_router = await aiozmq.create_zmq_stream(zmq.PULL, bind=f"tcp://0.0.0.0:{self.protocol_port}",
                                                                  high_read=0, high_write=0)
            logging.warning(
                f"Worker: {self.id} TCP Server listening at 0.0.0.0:{self.server_port} "
                f"IP:{self.networking.host_name}"
            )
            while True:
                _, data = await self.router.read()
                self.aio_task_scheduler.create_task(self.worker_controller(data))
        finally:
            self.router.close()

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
            self.heartbeat_proc = multiprocessing.Process(target=self.start_heartbeat_process, args=(self.id,))
            self.heartbeat_proc.start()
            await self.start_tcp_service()
            self.heartbeat_proc.join()
        finally:
            await self.protocol_networking.close_all_connections()
            await self.networking.close_all_connections()
            self.router.close()
            self.protocol_router.close()

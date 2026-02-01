import asyncio
from copy import deepcopy
import io
import os
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from confluent_kafka.admin import AdminClient, KafkaException, NewTopic
from setuptools._distutils.util import strtobool
from snapshot_compactor import start_snapshot_compaction
from styx.common.exceptions import NotAStateflowGraphError
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import (
    Serializer,
    cloudpickle_serialization,
    msgpack_serialization,
    zstd_msgpack_serialization,
)
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from worker_pool import Worker, WorkerPool

if TYPE_CHECKING:
    import concurrent.futures

    from minio import Minio
    from styx.common.tcp_networking import NetworkingManager
    from styx.common.types import OperatorPartition

MAX_OPERATOR_PARALLELISM = int(os.getenv("MAX_OPERATOR_PARALLELISM", "10"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")
COMPACT_SNAPSHOTS: bool = bool(strtobool(os.getenv("COMPACT_SNAPSHOTS", "false")))
KAFKA_URL: str = os.getenv("KAFKA_URL", None)


class Coordinator:
    def __init__(self, networking: NetworkingManager, minio_client: Minio) -> None:
        self.networking = networking
        self.minio_client = minio_client
        self.graph_submitted: bool = False
        self.prev_completed_snapshot_id: int = -1
        self.completed_input_offsets: dict[OperatorPartition, int] = {}
        self.completed_out_offsets: dict[OperatorPartition, int] = {}
        self.completed_epoch_counter: int = 0
        self.completed_t_counter: int = 0
        self.worker_pool = WorkerPool()
        self.submitted_graph: StateflowGraph | None = None

        self.worker_snapshot_ids: dict[int, int] = {}
        self.worker_is_healthy: dict[int, asyncio.Event] = {}
        self.worker_ip_to_id: dict[tuple[str, int, int], int] = {}

        self.kafka_metadata_producer: AIOKafkaProducer | None = None

    async def start_kafka_metadata_producer(self) -> None:
        self.kafka_metadata_producer = AIOKafkaProducer(
            bootstrap_servers=[KAFKA_URL],
            client_id="Coordinator",
            enable_idempotence=True,
        )
        while True:
            try:
                await self.kafka_metadata_producer.start()
            except KafkaConnectionError:
                # Waiting for Kafka
                await asyncio.sleep(1)
                continue
            break

    def init_data_complete(self) -> None:
        self.worker_snapshot_ids = dict.fromkeys(self.worker_snapshot_ids.keys(), 0)

    def register_worker(
        self,
        worker_ip: str,
        worker_port: int,
        protocol_port: int,
    ) -> tuple[int, bool]:
        worker_key = (worker_ip, worker_port, protocol_port)
        if worker_key not in self.worker_ip_to_id:
            worker_id = self.worker_pool.register_worker(
                worker_ip,
                worker_port,
                protocol_port,
            )
            self.worker_ip_to_id[worker_key] = worker_id
            init_recovery: bool = False
        else:
            worker_id = self.worker_ip_to_id[worker_key]
            init_recovery: bool = True
        self.worker_snapshot_ids[worker_id] = self.get_current_completed_snapshot_id()
        return worker_id, init_recovery

    def get_worker_with_id(self, worker_id: int) -> Worker:
        return self.worker_pool.peek(worker_id)

    async def start_recovery_process(self, workers_to_remove: set[Worker]) -> None:
        await self.worker_pool.initiate_recovery(workers_to_remove)
        await self.send_recovery_to_participating_workers()
        for worker in workers_to_remove:
            del self.worker_snapshot_ids[worker.worker_id]

    async def send_recovery_to_participating_workers(self) -> None:
        operator_partition_locations = self.worker_pool.get_operator_partition_locations()
        snap_id: int = self.get_current_completed_snapshot_id()
        worker_assignments = self.worker_pool.get_worker_assignments()
        participating_workers: list[Worker] = self.worker_pool.get_participating_workers()
        self.worker_is_healthy = {worker.worker_id: asyncio.Event() for worker in participating_workers}
        async with asyncio.TaskGroup() as tg:
            for worker in participating_workers:
                tg.create_task(
                    self.networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=(
                            worker.worker_id,
                            worker_assignments[
                                (
                                    worker.worker_ip,
                                    worker.worker_port,
                                    worker.protocol_port,
                                )
                            ],
                            operator_partition_locations,
                            self.worker_pool.get_workers(),
                            self.submitted_graph.operator_state_backend,
                            snap_id,
                            self.submitted_graph,
                        ),
                        msg_type=MessageType.InitRecovery,
                    ),
                )

    def worker_is_ready_after_recovery(self, worker_id: int) -> None:
        self.worker_is_healthy[worker_id].set()

    async def wait_cluster_healthy(self) -> None:
        tasks = [event.wait() for event in self.worker_is_healthy.values()]
        await asyncio.gather(*tasks)

    async def notify_cluster_healthy(self) -> None:
        # notify that everyone is ready after recovery
        async with asyncio.TaskGroup() as tg:
            for worker in self.worker_pool.get_participating_workers():
                tg.create_task(
                    self.networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=b"",
                        msg_type=MessageType.ReadyAfterRecovery,
                        serializer=Serializer.NONE,
                    ),
                )

    def register_worker_heartbeat(self, worker_id: int, heartbeat_time: float) -> None:
        self.worker_pool.register_worker_heartbeat(worker_id, heartbeat_time)

    def check_heartbeats(
        self,
        heartbeat_check_time: float,
    ) -> tuple[set[Worker], dict[int, float]]:
        if not self.graph_submitted:
            return set(), {}
        return self.worker_pool.check_heartbeats(heartbeat_check_time)

    def register_snapshot(
        self,
        worker_id: int,
        snapshot_id: int,
        partial_input_offsets: dict[OperatorPartition, int],
        partial_output_offsets: dict[OperatorPartition, int],
        epoch_counter: int,
        t_counter: int,
        pool: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        self.worker_snapshot_ids[worker_id] = snapshot_id
        for operator_partition, pio in partial_input_offsets.items():
            self.completed_input_offsets[operator_partition] = max(
                pio,
                self.completed_input_offsets.get(operator_partition, pio),
            )
        for operator_partition, poo in partial_output_offsets.items():
            self.completed_out_offsets[operator_partition] = max(
                poo,
                self.completed_out_offsets.get(operator_partition, poo),
            )
        self.completed_epoch_counter = epoch_counter
        self.completed_t_counter = t_counter
        current_completed_snapshot: int = self.get_current_completed_snapshot_id()
        if current_completed_snapshot != self.prev_completed_snapshot_id:
            logging.warning(f"Cluster completed snapshot: {current_completed_snapshot}")
            # if we reached a complete snapshot we could compact its deltas with the previous one
            sn_data: bytes = zstd_msgpack_serialization(
                (
                    self.completed_input_offsets,
                    self.completed_out_offsets,
                    self.completed_epoch_counter,
                    self.completed_t_counter,
                ),
            )
            self.minio_client.put_object(
                SNAPSHOT_BUCKET_NAME,
                f"sequencer/{current_completed_snapshot}.bin",
                io.BytesIO(sn_data),
                len(sn_data),
            )
            self.prev_completed_snapshot_id = current_completed_snapshot
            if COMPACT_SNAPSHOTS:
                loop = asyncio.get_running_loop()
                loop.run_in_executor(
                    pool,
                    start_snapshot_compaction,
                    current_completed_snapshot,
                )

    def get_current_completed_snapshot_id(self) -> int:
        if self.worker_snapshot_ids:
            return min(self.worker_snapshot_ids.values())
        return -1

    async def update_stateflow_graph(self, new_stateflow_graph: StateflowGraph) -> None:
        if not isinstance(new_stateflow_graph, StateflowGraph):
            raise NotAStateflowGraphError
        # TODO the cluster was balnced by the previous deployment, if the graph is complex it might be
        #  unbalanced after the update
        for _, operator in iter(new_stateflow_graph):
            for partition in range(MAX_OPERATOR_PARALLELISM):
                operator_copy = deepcopy(operator)
                if partition > operator.n_partitions:
                    operator_copy.make_shadow()
                self.worker_pool.update_operator(
                    (operator_copy.name, partition),
                    operator_copy,
                )
        worker_assignments = self.worker_pool.get_worker_assignments()
        tasks = [
            self.networking.send_message(
                worker.worker_ip,
                worker.worker_port,
                msg=(
                    new_stateflow_graph,
                    worker_assignments[(worker.worker_ip, worker.worker_port, worker.protocol_port)],
                    self.worker_pool.get_operator_partition_locations(),
                    self.worker_pool.get_workers(),
                    new_stateflow_graph.operator_state_backend,
                ),
                msg_type=MessageType.InitMigration,
            )
            for worker in self.worker_pool.get_participating_workers()
        ]
        await asyncio.gather(*tasks)
        self.submitted_graph = new_stateflow_graph
        metadata_key = msgpack_serialization(self.submitted_graph.name)
        serialized_graph = cloudpickle_serialization(new_stateflow_graph)
        await self.kafka_metadata_producer.send_and_wait(
            "styx-metadata",
            key=metadata_key,
            value=serialized_graph,
        )

    async def submit_stateflow_graph(
        self,
        stateflow_graph: StateflowGraph,
        ingress_type: IngressTypes = IngressTypes.KAFKA,
    ) -> None:
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraphError
        if ingress_type == IngressTypes.KAFKA:
            await self.create_kafka_ingress_topics(stateflow_graph)
        for operator_name, operator in iter(stateflow_graph):
            for partition in range(operator.n_partitions):
                operator_copy = deepcopy(operator)
                self.worker_pool.schedule_operator_partition(
                    (operator_name, partition),
                    operator_copy,
                )
        # Also add the shadow partitions n_partitions - max parallelism
        for operator_name, operator in iter(stateflow_graph):
            for shadow_partition in range(
                operator.n_partitions,
                MAX_OPERATOR_PARALLELISM,
            ):
                operator_copy = deepcopy(operator)
                operator_copy.make_shadow()
                self.worker_pool.schedule_operator_partition(
                    (operator_name, shadow_partition),
                    operator_copy,
                )
        worker_assignments = self.worker_pool.get_worker_assignments()
        tasks = [
            self.networking.send_message(
                worker.worker_ip,
                worker.worker_port,
                msg=(
                    worker_assignments[(worker.worker_ip, worker.worker_port, worker.protocol_port)],
                    self.worker_pool.get_operator_partition_locations(),
                    self.worker_pool.get_workers(),
                    stateflow_graph.operator_state_backend,
                    stateflow_graph,
                ),
                msg_type=MessageType.ReceiveExecutionPlan,
                serializer=Serializer.CLOUDPICKLE,
            )
            for worker in self.worker_pool.get_participating_workers()
        ]
        await asyncio.gather(*tasks)
        self.graph_submitted = True
        self.submitted_graph = stateflow_graph
        metadata_key = msgpack_serialization(self.submitted_graph.name)
        serialized_graph = cloudpickle_serialization(stateflow_graph)
        await self.kafka_metadata_producer.send_and_wait(
            "styx-metadata",
            key=metadata_key,
            value=serialized_graph,
        )

    async def create_kafka_ingress_topics(
        self,
        stateflow_graph: StateflowGraph,
    ) -> None:
        if KAFKA_URL is None:
            logging.error("Kafka URL not given")
        while True:
            try:
                client = AdminClient({"bootstrap.servers": KAFKA_URL})
                break
            except KafkaException:
                logging.warning(
                    f"Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second",
                )
                await asyncio.sleep(1)
        topics = (
            [
                NewTopic(
                    topic="styx-metadata",
                    num_partitions=1,
                    replication_factor=KAFKA_REPLICATION_FACTOR,
                ),
            ]
            + [
                NewTopic(
                    topic="sequencer-wal",
                    num_partitions=1,
                    replication_factor=KAFKA_REPLICATION_FACTOR,
                ),
            ]
            + [
                NewTopic(
                    topic=operator.name,
                    num_partitions=MAX_OPERATOR_PARALLELISM,
                    replication_factor=KAFKA_REPLICATION_FACTOR,
                )
                for operator in stateflow_graph.nodes.values()
            ]
            + [
                NewTopic(
                    topic=operator.name + "--OUT",
                    num_partitions=MAX_OPERATOR_PARALLELISM,
                    replication_factor=KAFKA_REPLICATION_FACTOR,
                )
                for operator in stateflow_graph.nodes.values()
            ]
        )

        futures = client.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
                logging.warning(
                    f"Topic {topic} created with {MAX_OPERATOR_PARALLELISM} partitions "
                    f"and replication factor of {KAFKA_REPLICATION_FACTOR}",
                )
            except KafkaException as e:
                logging.warning(f"Failed to create topic {topic}: {e}")
        if self.kafka_metadata_producer is None:
            await self.start_kafka_metadata_producer()

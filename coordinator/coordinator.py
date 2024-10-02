import asyncio
import concurrent.futures
import os
import time

from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

from styx.common.base_operator import BaseOperator
from styx.common.local_state_backends import LocalStateBackend
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from styx.common.logging import logging
from styx.common.exceptions import NotAStateflowGraph

from snapshot_compactor import start_snapshot_compaction
from scheduler.round_robin import RoundRobin


MAX_OPERATOR_PARALLELISM = int(os.getenv('MAX_OPERATOR_PARALLELISM', 10))


class Coordinator(object):

    def __init__(self, networking: NetworkingManager):
        self.networking = networking
        self.worker_counter: int = 0
        self.workers: dict[int, tuple[str, int, int]] = {}
        self.graph_submitted: bool = False
        self.prev_completed_snapshot_id: int = 0

        self.worker_snapshot_ids: dict[int, int] = {}
        self.worker_heartbeats: dict[int, float] = {}
        self.dead_workers: set[int] = set()

        self.worker_assignments: dict[tuple[str, int, int], list[tuple[BaseOperator, int]]] = ...
        self.operator_partition_locations: dict[str, dict[str, tuple[str, int, int]]] = ...
        self.operator_state_backend: LocalStateBackend = ...

    async def register_worker(self, worker_ip: str, worker_port: int, protocol_port: int) -> tuple[int, bool]:
        # a worker died before a new one registered
        id_not_ready: bool = True
        send_recovery: bool = False
        worker_id: int = -1
        while id_not_ready:
            if self.dead_workers:
                dead_worker_id: int = self.dead_workers.pop()
                self.workers[dead_worker_id] = (worker_ip, worker_port, protocol_port)
                logging.info(f'Assigned: {dead_worker_id} to {worker_ip}')
                if self.graph_submitted:
                    self.change_operator_partition_locations(dead_worker_id, worker_ip, worker_port, protocol_port)
                    send_recovery = True
                worker_id = dead_worker_id
                id_not_ready = False
            # a worker registers without any dead workers in the cluster
            else:
                if self.graph_submitted:
                    # sleep for 10ms waiting for heartbeat to detect
                    await asyncio.sleep(0.01)
                else:
                    self.worker_counter += 1
                    self.workers[self.worker_counter] = (worker_ip, worker_port, protocol_port)
                    self.worker_snapshot_ids[self.worker_counter] = -1
                    worker_id = self.worker_counter
                    id_not_ready = False
        self.worker_heartbeats[worker_id] = 1_000_000.0
        return worker_id, send_recovery

    async def remove_workers(self, workers_to_remove: set[int]):
        self.dead_workers.update(workers_to_remove)
        await self.send_recovery_to_healthy_workers(workers_to_remove)

    def change_operator_partition_locations(self,
                                            dead_worker_id: int,
                                            worker_ip: str,
                                            worker_port: int,
                                            protocol_port: int):
        removed_ip = self.workers[dead_worker_id][0]
        new_operator_partition_locations = {}
        for operator_name, partition_dict in self.operator_partition_locations.items():
            new_operator_partition_locations[operator_name] = {}
            for partition, worker in partition_dict.items():
                if worker[0] == removed_ip:
                    new_operator_partition_locations[operator_name][partition] = (worker_ip, worker_port, protocol_port)
                else:
                    new_operator_partition_locations[operator_name][partition] = worker
        self.operator_partition_locations = new_operator_partition_locations

    def register_worker_heartbeat(self, worker_id: int, heartbeat_time: float):
        self.worker_heartbeats[worker_id] = heartbeat_time

    def register_snapshot(self, worker_id: int, snapshot_id: int, pool: concurrent.futures.ProcessPoolExecutor):
        self.worker_snapshot_ids[worker_id] = snapshot_id
        current_completed_snapshot: int = self.get_current_completed_snapshot_id()
        if current_completed_snapshot != self.prev_completed_snapshot_id:
            # if we reached a complete snapshot we could compact its deltas with the previous one
            self.prev_completed_snapshot_id = current_completed_snapshot
            loop = asyncio.get_running_loop()
            loop.run_in_executor(pool,
                                 start_snapshot_compaction,
                                 current_completed_snapshot)

    def get_current_completed_snapshot_id(self):
        return min(self.worker_snapshot_ids.values())

    async def submit_stateflow_graph(self,
                                     stateflow_graph: StateflowGraph,
                                     ingress_type: IngressTypes = IngressTypes.KAFKA,
                                     scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        if ingress_type == IngressTypes.KAFKA:
            self.create_kafka_ingress_topics(stateflow_graph)
        self.worker_assignments, self.operator_partition_locations, self.operator_state_backend = \
            await scheduler.schedule(self.workers, stateflow_graph, self.networking)
        self.graph_submitted = True

    async def send_operators_snapshot_offsets(self, dead_worker_id: int):
        # wait for 100ms for the resurrected worker to receive its assignment
        await asyncio.sleep(0.1)
        worker = self.workers[dead_worker_id]
        operator_partitions = self.worker_assignments[worker]
        await self.networking.send_message(worker[0], worker[1],
                                           msg=(dead_worker_id,
                                                operator_partitions,
                                                self.operator_partition_locations,
                                                self.workers,
                                                self.operator_state_backend,
                                                self.get_current_completed_snapshot_id()),
                                           msg_type=MessageType.RecoveryOwn)
        logging.info(f'SENT RECOVER TO DEAD WORKER: {worker[0]}:{worker[1]}')

    async def send_recovery_to_healthy_workers(self, workers_to_remove: set[int]):
        # wait till all the workers have been reassigned
        if self.graph_submitted:
            while True:
                if not self.dead_workers:
                    break
                await asyncio.sleep(0.01)

            async with asyncio.TaskGroup() as tg:
                for worker_id, worker in self.workers.items():
                    if worker_id not in workers_to_remove:
                        tg.create_task(self.networking.send_message(worker[0], worker[1],
                                                                    msg=(self.operator_partition_locations,
                                                                         self.workers,
                                                                         self.operator_state_backend,
                                                                         self.get_current_completed_snapshot_id()),
                                                                    msg_type=MessageType.RecoveryOther))
            logging.info('SENT RECOVER TO HEALTHY WORKERS')

    def create_kafka_ingress_topics(self, stateflow_graph: StateflowGraph):
        kafka_url: str = os.getenv('KAFKA_URL', None)
        if kafka_url is None:
            logging.error('Kafka URL not given')
        while True:
            try:
                client = AdminClient({'bootstrap.servers': kafka_url})
                break
            except KafkaException:
                logging.warning(f'Kafka at {kafka_url} not ready yet, sleeping for 1 second')
                time.sleep(1)
        topics = (
                [NewTopic(topic='styx-metadata', num_partitions=1, replication_factor=1)] +
                [NewTopic(topic=operator.name,
                          num_partitions=MAX_OPERATOR_PARALLELISM,
                          replication_factor=1)
                 for operator in stateflow_graph.nodes.values()] +
                [NewTopic(topic=operator.name + "--OUT",
                          num_partitions=MAX_OPERATOR_PARALLELISM,
                          replication_factor=1)
                 for operator in stateflow_graph.nodes.values()])

        futures = client.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
                logging.warning(f"Topic {topic} created")
            except KafkaException as e:
                logging.warning(f"Failed to create topic {topic}: {e}")

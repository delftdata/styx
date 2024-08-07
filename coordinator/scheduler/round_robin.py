import asyncio

from styx.common.tcp_networking import NetworkingManager
from styx.common.operator import BaseOperator
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_worker import StateflowWorker
from styx.common.message_types import MessageType

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    @staticmethod
    async def schedule(workers: dict[int, tuple[str, int, int]],
                       execution_graph: StateflowGraph,
                       network_manager: NetworkingManager):
        operator_partition_locations: dict[str, dict[str, tuple[str, int, int]]] = {}
        worker_locations = [StateflowWorker(host=worker[0], port=worker[1], protocol_port=worker[2])
                            for worker in workers.values()]
        worker_assignments: dict[tuple[str, int, int], list[tuple[BaseOperator, int]]] = {(worker.host,
                                                                                           worker.port,
                                                                                           worker.protocol_port): []
                                                                                          for worker in worker_locations
                                                                                          }
        for operator_name, operator in iter(execution_graph):
            for partition in range(operator.n_partitions):
                current_worker = worker_locations.pop(0)
                worker_assignments[(current_worker.host,
                                    current_worker.port,
                                    current_worker.protocol_port)].append((operator, partition))
                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
                                                                                         current_worker.port,
                                                                                         current_worker.protocol_port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
                                                                                    current_worker.port,
                                                                                    current_worker.protocol_port)}
                worker_locations.append(current_worker)

        tasks = [
            asyncio.ensure_future(
                network_manager.send_message(worker[0], worker[1],
                                             msg=(operator_partitions,
                                                  operator_partition_locations,
                                                  workers,
                                                  execution_graph.operator_state_backend),
                                             msg_type=MessageType.ReceiveExecutionPlan))
            for worker, operator_partitions in worker_assignments.items()]

        await asyncio.gather(*tasks)
        return worker_assignments, operator_partition_locations, execution_graph.operator_state_backend

import asyncio
from collections import defaultdict
from typing import Any

from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition

from coordinator.worker_pool import Worker


class MigrationMetadata(object):

    def __init__(self, n_workers: int):
        self.n_workers: int = n_workers
        self.sync_sum: dict[MessageType, int] = defaultdict(int)
        self.lock: asyncio.Lock = asyncio.Lock()
        self.input_offsets: dict[OperatorPartition, int] = {}
        self.output_offsets: dict[OperatorPartition, int] = {}
        self.remote_keys: dict[tuple[str, int, int], dict[OperatorPartition, dict[Any, tuple[int, int]]]] = {}
        self.epoch_counter: int = -1
        self.t_counter: int = -1

    def check_sum(self, msg_type: MessageType) -> bool:
        return self.sync_sum[msg_type] == self.n_workers

    def add_hashes(self,
                   data: dict[OperatorPartition, dict[Any, tuple[int, int]]],
                   dns: dict[str, dict[int, tuple[str, int, int]]]):
        for operator_partition, new_hashes in data.items():
            op_name, op_part = operator_partition
            worker_tuple = dns[op_name][op_part]
            if worker_tuple not in self.remote_keys:
                self.remote_keys[worker_tuple] = {}
            if not new_hashes:
                continue
            if operator_partition not in self.remote_keys[worker_tuple]:
                self.remote_keys[worker_tuple][operator_partition] = new_hashes
            else:
                self.remote_keys[worker_tuple][operator_partition].update(new_hashes)

    def get_worker_hashes(self, worker: Worker):
        w_t = worker.to_tuple()
        if w_t in self.remote_keys:
            return self.remote_keys[w_t]
        else:
            return {}

    async def repartitioning_done(self,
                                  epoch_counter: int,
                                  t_counter: int,
                                  input_offsets: dict[OperatorPartition, int],
                                  output_offsets: dict[OperatorPartition, int]) -> bool:
        async with self.lock:
            self.sync_sum[MessageType.MigrationRepartitioningDone] += 1
            for operator_partition, pio in input_offsets.items():
                self.input_offsets[operator_partition] = max(pio,
                                                             self.input_offsets.get(
                                                                 operator_partition,
                                                                 pio))
            for operator_partition, poo in output_offsets.items():
                self.output_offsets[operator_partition] = max(poo,
                                                              self.output_offsets.get(operator_partition,
                                                                                      poo))
            self.epoch_counter = max(epoch_counter, self.epoch_counter)
            self.t_counter = max(t_counter, self.t_counter)
            return self.check_sum(MessageType.MigrationRepartitioningDone)

    async def set_empty_sync_done(self, msg_type: MessageType):
        async with self.lock:
            self.sync_sum[msg_type] += 1
            return self.check_sum(msg_type)

    async def cleanup(self, msg_type: MessageType):
        async with self.lock:
            self.sync_sum[msg_type] = 0
            if msg_type == MessageType.MigrationRepartitioningDone:
                self.epoch_counter = -1
                self.t_counter = -1
                self.input_offsets.clear()
                self.output_offsets.clear()
                self.remote_keys.clear()

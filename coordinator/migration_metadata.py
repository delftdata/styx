import asyncio

from styx.common.types import OperatorPartition


class MigrationMetadata(object):

    def __init__(self, n_workers: int):
        self.n_workers: int = n_workers
        self.sync_sum: int = 0
        self.lock: asyncio.Lock = asyncio.Lock()
        self.input_offsets: dict[OperatorPartition, int] = {}
        self.output_offsets: dict[OperatorPartition, int] = {}
        self.epoch_counter: int = -1
        self.t_counter: int = -1

    def check_sum(self) -> bool:
        return self.sync_sum == self.n_workers

    async def repartitioning_done(self,
                                  epoch_counter: int,
                                  t_counter: int,
                                  input_offsets: dict[OperatorPartition, int],
                                  output_offsets: dict[OperatorPartition, int]) -> bool:
        async with self.lock:
            self.sync_sum += 1
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
            return self.check_sum()

    async def set_empty_sync_done(self):
        async with self.lock:
            self.sync_sum += 1
            return self.check_sum()

    async def cleanup(self):
        async with self.lock:
            self.sync_sum: int = 0
            self.epoch_counter = -1
            self.t_counter = -1
            self.input_offsets.clear()
            self.output_offsets.clear()

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING

from styx.common.message_types import MessageType

if TYPE_CHECKING:
    from styx.common.types import OperatorPartition


class MigrationMetadata:
    def __init__(self, n_workers: int) -> None:
        self.n_workers: int = n_workers
        self.sync_sum: dict[MessageType, int] = defaultdict(int)
        self.lock: asyncio.Lock = asyncio.Lock()
        self.input_offsets: dict[OperatorPartition, int] = {}
        self.output_offsets: dict[OperatorPartition, int] = {}
        self.epoch_counter: int = -1
        self.t_counter: int = -1

    def check_sum(self, msg_type: MessageType) -> bool:
        return self.sync_sum[msg_type] == self.n_workers

    async def repartitioning_done(
        self,
        epoch_counter: int,
        t_counter: int,
        input_offsets: dict[OperatorPartition, int],
        output_offsets: dict[OperatorPartition, int],
    ) -> bool:
        async with self.lock:
            self.sync_sum[MessageType.MigrationRepartitioningDone] += 1
            for operator_partition, pio in input_offsets.items():
                self.input_offsets[operator_partition] = max(
                    pio,
                    self.input_offsets.get(operator_partition, pio),
                )
            for operator_partition, poo in output_offsets.items():
                self.output_offsets[operator_partition] = max(
                    poo,
                    self.output_offsets.get(operator_partition, poo),
                )
            self.epoch_counter = max(epoch_counter, self.epoch_counter)
            self.t_counter = max(t_counter, self.t_counter)
            return self.check_sum(MessageType.MigrationRepartitioningDone)

    async def set_empty_sync_done(self, msg_type: MessageType) -> bool:
        async with self.lock:
            self.sync_sum[msg_type] += 1
            return self.check_sum(msg_type)

    async def cleanup(self, msg_type: MessageType) -> None:
        async with self.lock:
            self.sync_sum[msg_type] = 0
            if msg_type == MessageType.MigrationRepartitioningDone:
                self.epoch_counter = -1
                self.t_counter = -1
                self.input_offsets.clear()
                self.output_offsets.clear()

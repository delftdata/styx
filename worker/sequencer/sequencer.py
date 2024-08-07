import asyncio

from styx.common.logging import logging
from styx.common.run_func_payload import RunFuncPayload, SequencedItem


class Sequencer(object):

    def __init__(self, max_size: int = None, t_counter: int = 0, epoch_counter: int = 0):
        self.distributed_log: list[SequencedItem] = []
        self.current_epoch: list[SequencedItem] = []
        self.t_counter: int = t_counter
        self.worker_id: int = -1
        self.n_workers: int = -1
        self.epoch_counter: int = epoch_counter
        self.max_size: int = max_size
        self.lock: asyncio.Lock = asyncio.Lock()

    def set_worker_id(self, worker_id: int):
        self.worker_id = worker_id

    def set_n_workers(self, n_workers: int):
        self.n_workers = n_workers

    def sequence(self, message: RunFuncPayload):
        t_id = self.worker_id + self.t_counter * self.n_workers
        self.t_counter += 1
        logging.info(f'Sequencing message: {message.key} with t_id: {t_id}')
        self.distributed_log.append(SequencedItem(t_id, message))

    def get_epoch(self) -> list[SequencedItem]:
        if len(self.distributed_log) > 0:
            if self.max_size is None:
                self.current_epoch = self.distributed_log
                self.distributed_log = []
            else:
                self.current_epoch = self.distributed_log[:self.max_size]
                self.distributed_log = self.distributed_log[self.max_size:]
            return self.current_epoch
        return []

    def increment_epoch(self,
                        max_t_counter: int,
                        t_ids_to_reschedule: set[int]):
        self.t_counter = max_t_counter
        if t_ids_to_reschedule:
            # add the sorted aborted SequencedItems to the beginning of the sequence
            self.distributed_log = sorted([item for item in self.current_epoch
                                           if item.t_id in t_ids_to_reschedule]) + self.distributed_log
        self.epoch_counter += 1
        self.current_epoch = []

    def get_aborted_sequence(self,
                             t_ids_to_reschedule: set[int]) -> list[SequencedItem]:
        if t_ids_to_reschedule:
            aborted_sequence: list[SequencedItem] = sorted({item for item in self.current_epoch
                                                            if item.t_id in t_ids_to_reschedule})
            return aborted_sequence
        return []

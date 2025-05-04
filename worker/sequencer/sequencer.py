import asyncio

# from styx.common.logging import logging
from styx.common.run_func_payload import RunFuncPayload, SequencedItem


class Sequencer(object):

    def __init__(self, max_size: int = None, t_counter: int = 0, epoch_counter: int = 0):
        self.distributed_log: list[SequencedItem] = []
        self.current_epoch: list[SequencedItem] = []
        self.t_counter: int = t_counter
        self.sequencer_id: int = -1
        self.n_workers: int = -1
        self.epoch_counter: int = epoch_counter
        self.max_size: int = max_size
        self.lock: asyncio.Lock = asyncio.Lock()
        # wal values
        self.request_id_to_t_id_map: dict[bytes, int] = {}
        self.t_ids_in_wal: set[int] = set()

    def set_sequencer_id(self, peer_ids: list[int], worker_id: int):
        sorted_w_ids = sorted(peer_ids + [worker_id])
        s_id_assignment: int = -1
        available_s_ids = set(range(1, len(sorted_w_ids) + 1))
        for w_id in sorted_w_ids:
            if w_id in available_s_ids:
                s_id_assignment = w_id
            else:
                # Find the smallest next available ID. Needed for determinism across workers since the set is unordered
                s_id_assignment = min(available_s_ids)
            available_s_ids.remove(s_id_assignment)
            if w_id == worker_id:
                break
        self.sequencer_id = s_id_assignment
        self.n_workers = len(sorted_w_ids)

    def sequence(self, message: RunFuncPayload):
        if message.request_id in self.request_id_to_t_id_map:
            t_id = self.request_id_to_t_id_map[message.request_id]
        else:
            t_id = self.get_t_id()
            while t_id in self.t_ids_in_wal:
                t_id = self.get_t_id()
        # logging.info(f'Sequencing message: {message.key} with t_id: {t_id}')
        self.distributed_log.append(SequencedItem(t_id, message))

    def get_t_id(self) -> int:
        t_id = self.sequencer_id + self.t_counter * self.n_workers
        self.t_counter += 1
        return t_id

    def set_wal_values_after_recovery(self, request_id_to_t_id_map: dict[bytes, int]):
        if request_id_to_t_id_map is not None:
            self.request_id_to_t_id_map = request_id_to_t_id_map
            self.t_ids_in_wal: set[int] = set(request_id_to_t_id_map.values())

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

from typing import Any


class AriaSyncMetadata(object):

    def __init__(self, n_workers: int):
        self.n_workers: int = n_workers
        self.arrived: set[int] = set()
        self.sent_proceed_msg: bool = False
        self.logic_aborts_everywhere: set[int] = set()
        self.concurrency_aborts_everywhere: set[int] = set()
        self.processed_seq_size: int = 0
        self.max_t_counter: int = -1
        self.global_read_reservations: None | dict = None
        self.global_write_set: None | dict = None
        self.global_read_set: None | dict = None
        self.stop_next_epoch: bool = False
        self.take_snapshot: bool = False

    def check_distributed_barrier(self) -> bool:
        return len(self.arrived) == self.n_workers

    def stop_in_next_epoch(self):
        self.stop_next_epoch = True

    def take_snapshot_at_next_epoch(self):
        self.take_snapshot = True

    def set_aria_processing_done(self, worker_id, workers_logic_aborts: set[int]) -> bool:
        self.arrived.add(worker_id)
        self.logic_aborts_everywhere.update(workers_logic_aborts)
        return self.check_distributed_barrier()

    def set_aria_commit_done(self, worker_id, aborted: set[int], remote_t_counter: int, processed_seq_size: int) -> bool:
        self.arrived.add(worker_id)
        self.concurrency_aborts_everywhere.update(aborted)
        self.processed_seq_size += processed_seq_size
        self.max_t_counter = max(self.max_t_counter, remote_t_counter)
        return self.check_distributed_barrier()

    def set_empty_sync_done(self, worker_id):
        self.arrived.add(worker_id)
        return self.check_distributed_barrier()

    def set_deterministic_reordering_done(self, worker_id, remote_read_reservation, remote_write_set, remote_read_set):
        self.arrived.add(worker_id)
        if self.global_read_reservations is None:
            self.global_read_reservations = remote_read_reservation
            self.global_write_set = remote_write_set
            self.global_read_set = remote_read_set
        else:
            self.global_read_reservations = self.__merge_rw_reservations(remote_read_reservation,
                                                                         self.global_read_reservations)
            self.global_write_set = self.__merge_rw_sets(remote_write_set, self.global_write_set)
            self.global_read_set = self.__merge_rw_sets(remote_read_set, self.global_read_set)
        return self.check_distributed_barrier()

    @staticmethod
    def __merge_rw_sets(d1: dict[str, dict[Any, set[Any] | dict[Any, Any]]],
                        d2: dict[str, dict[Any, set[Any] | dict[Any, Any]]]
                        ) -> dict[str, dict[Any, set[Any] | dict[Any, Any]]]:
        output_dict: dict[str, dict[Any, set[Any] | dict[Any, Any]]] = {}
        namespaces: set[str] = set(d1.keys()) | set(d2.keys())
        for namespace in namespaces:
            output_dict[namespace] = {}
            if namespace in d1 and namespace in d2:
                t_ids = set(d1[namespace].keys()) | set(d2[namespace].keys())
                for t_id in t_ids:
                    if t_id in d1[namespace] and t_id in d2[namespace]:
                        output_dict[namespace][t_id] = d1[namespace][t_id] | d2[namespace][t_id]
                    elif t_id not in d1[namespace]:
                        output_dict[namespace][t_id] = d2[namespace][t_id]
                    else:
                        output_dict[namespace][t_id] = d1[namespace][t_id]
            elif namespace in d1 and namespace not in d2:
                output_dict[namespace] = d1[namespace]
            elif namespace not in d1 and namespace in d2:
                output_dict[namespace] = d2[namespace]
        return output_dict

    @staticmethod
    def __merge_rw_reservations(d1: dict[str, dict[Any, list[int]]],
                                d2: dict[str, dict[Any, list[int]]]
                                ) -> dict[str, dict[Any, list[int]]]:
        output_dict: dict[str, dict[Any, list[int]]] = {}
        namespaces: set[str] = set(d1.keys()) | set(d2.keys())
        for namespace in namespaces:
            output_dict[namespace] = {}
            if namespace in d1 and namespace in d2:
                keys = set(d1[namespace].keys()) | set(d2[namespace].keys())
                for key in keys:
                    output_dict[namespace][key] = d1[namespace].get(key, []) + d2[namespace].get(key, [])
            elif namespace in d1 and namespace not in d2:
                output_dict[namespace] = d1[namespace]
            elif namespace not in d1 and namespace in d2:
                output_dict[namespace] = d2[namespace]
        return output_dict

    def cleanup(self, epoch_end: bool = False, take_snapshot: bool = False) -> None:
        self.arrived.clear()
        self.logic_aborts_everywhere: set[int] = set()
        self.sent_proceed_msg: bool = False
        self.concurrency_aborts_everywhere: set[int] = set()
        self.processed_seq_size: int = 0
        self.max_t_counter: int = -1
        self.global_read_reservations: None | dict = None
        self.global_write_set: None | dict = None
        self.global_read_set: None | dict = None
        if epoch_end:
            self.stop_next_epoch = False
        if take_snapshot:
            self.take_snapshot = False

import asyncio
from abc import abstractmethod
from collections import defaultdict

from styx.common.base_state import BaseOperatorState


class BaseAriaState(BaseOperatorState):
    # read write sets
    # operator_name: {t_id: set(keys)}
    read_sets: dict[str, dict[int, set[any]]]
    global_read_sets: dict[str, dict[int, set[any]]]
    # operator_name: {t_id: {key: value}}
    write_sets: dict[str, dict[int, dict[any, any]]]
    global_write_sets: dict[str, dict[int, dict[any, any]]]
    # the reads and writes with the lowest t_id
    # operator_name: {key: t_id}
    writes: dict[str, dict[any, list[int]]]
    # operator_name: {key: t_id}
    reads: dict[str, dict[any, list[int]]]
    global_reads: dict[str, dict[any, list[int]]]
    # Calvin snapshot things
    # tid: {operator_name: {key, value}}
    fallback_commit_buffer: dict[int, dict[str, dict[any, any]]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.cleanup()

    def put(self, key, value, t_id: int, operator_name: str):
        if t_id in self.write_sets[operator_name]:
            self.write_sets[operator_name][t_id][key] = value
        else:
            self.write_sets[operator_name][t_id] = {key: value}
        if key in self.writes[operator_name]:
            self.writes[operator_name][key].append(t_id)
        else:
            self.writes[operator_name][key] = [t_id]

    def put_immediate(self, key, value, t_id: int, operator_name: str):
        if t_id in self.fallback_commit_buffer:
            if operator_name in self.fallback_commit_buffer[t_id]:
                self.fallback_commit_buffer[t_id][operator_name][key] = value
            else:
                self.fallback_commit_buffer[t_id][operator_name] = {key: value}
        else:
            self.fallback_commit_buffer[t_id] = {operator_name: {key: value}}

    def set_global_read_write_sets(self, global_read_reservations, global_write_set, global_read_set):
        self.global_reads = global_read_reservations
        self.global_write_sets = global_write_set
        self.global_read_sets = global_read_set

    @abstractmethod
    def batch_insert(self, kv_pairs: dict, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    def commit_fallback_transaction(self, t_id: int):
        raise NotImplementedError

    @abstractmethod
    def get(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    def get_immediate(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    def delete(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    def exists(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    def commit(self, aborted_from_remote: set[int]) -> set[int]:
        raise NotImplementedError

    def deal_with_reads(self, key, t_id: int, operator_name: str):
        if key in self.reads[operator_name]:
            self.reads[operator_name][key].append(t_id)
        else:
            self.reads[operator_name][key] = [t_id]
        if t_id in self.read_sets[operator_name]:
            self.read_sets[operator_name][t_id].add(key)
        else:
            self.read_sets[operator_name][t_id] = {key}

    @staticmethod
    def has_conflicts(t_id: int, keys: set[any], reservations: dict[any, int]):
        for key in keys:
            if key in reservations and reservations[key] < t_id:
                return True
        return False

    @staticmethod
    def min_rw_reservations(reservations: dict[str, dict[any, list[int]]]) -> dict[str, dict[any, int]]:
        new__reservations = {}
        for operator_name, reservation in reservations.items():
            new__reservations[operator_name] = {key: min(t_ids) for key, t_ids in reservation.items() if t_ids}
        return new__reservations

    def check_conflicts(self) -> set[int]:
        """Checks for conflicts based on Arias default method

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        aborted_transactions = set()
        minimized_writes = self.min_rw_reservations(self.writes)
        for operator_name, write_set in self.write_sets.items():
            read_set = self.read_sets[operator_name]
            t_ids: set[int] = set(read_set.keys()).union(set(write_set.keys()))
            for t_id in t_ids:
                rs = read_set.get(t_id, set())
                ws = write_set.get(t_id, dict())
                read_write_set = rs.union(ws)
                if self.has_conflicts(t_id, read_write_set, minimized_writes[operator_name]):
                    aborted_transactions.add(t_id)
        return aborted_transactions

    def check_conflicts_deterministic_reordering(self) -> set[int]:
        """Checks for conflicts based on Arias deterministic reordering method

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        aborted_transactions = set()
        merged_reads = self.min_rw_reservations(self.global_reads)
        minimized_writes = self.min_rw_reservations(self.writes)
        for operator_name in self.write_sets.keys():
            write_set = self.global_write_sets[operator_name]
            read_set = self.global_read_sets[operator_name]
            t_ids: set[int] = set(self.write_sets[operator_name].keys()) | set(self.read_sets[operator_name].keys())
            for t_id in t_ids:
                ws = write_set.get(t_id, set())
                waw = self.has_conflicts(t_id, ws, minimized_writes[operator_name])
                if waw:
                    aborted_transactions.add(t_id)
                    continue
                war = self.has_conflicts(t_id, ws, merged_reads[operator_name])
                rs = read_set.get(t_id, set())
                raw = self.has_conflicts(t_id, rs, minimized_writes[operator_name])
                if not war or not raw:
                    continue
                aborted_transactions.add(t_id)
        return aborted_transactions

    def check_conflicts_snapshot_isolation(self) -> set[int]:
        """Checks for conflicts based only on write-after-write dependencies leading to snapshot isolation

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        aborted_transactions = set()
        minimized_writes = self.min_rw_reservations(self.writes)
        for operator_name in self.write_sets.keys():
            t_ids: set[int] = set(self.write_sets[operator_name].keys())
            for t_id in t_ids:
                ws = self.write_sets[operator_name].get(t_id, set())
                waw = self.has_conflicts(t_id, ws, minimized_writes[operator_name])
                if waw:
                    aborted_transactions.add(t_id)
        return aborted_transactions

    def cleanup(self):
        self.write_sets = {operator_name: {} for operator_name in self.operator_names}
        self.writes = {operator_name: {} for operator_name in self.operator_names}
        self.reads = {operator_name: {} for operator_name in self.operator_names}
        self.read_sets = {operator_name: {} for operator_name in self.operator_names}
        self.global_write_sets = {operator_name: {} for operator_name in self.operator_names}
        self.global_reads = {operator_name: {} for operator_name in self.operator_names}
        self.global_read_sets = {operator_name: {} for operator_name in self.operator_names}
        self.fallback_commit_buffer = defaultdict(lambda: defaultdict(dict))
        self.fallback_commit_buffer.clear()

    def get_dep_transactions(self,
                             t_ids_to_reschedule: set[int]) -> tuple[dict[int, set[int]], dict[int, asyncio.Event]]:
        """
        Returns a dict[int, set[int]] where key is the t_id and the value is a set of the transaction ids it depends on.
        """
        tid_locks = {tid: asyncio.Event() for tid in t_ids_to_reschedule}
        t_id_dependencies: dict[int, set[int]] = defaultdict(set)

        # Combine reads and writes for faster processing
        combined_accesses = defaultdict(lambda: defaultdict(list))
        for operator_name, reservations in self.reads.items():
            for key, t_ids in reservations.items():
                combined_accesses[operator_name][key].extend(t_ids)
        for operator_name, reservations in self.writes.items():
            for key, t_ids in reservations.items():
                combined_accesses[operator_name][key].extend(t_ids)

        # Preprocess combined accesses
        for operator_name, access_dict in combined_accesses.items():
            for t_ids in access_dict.values():
                valid_t_ids_accessed_key = t_ids_to_reschedule & set(t_ids)
                for t_id in valid_t_ids_accessed_key:
                    # Ensure smaller t_ids do not depend on larger ones
                    smaller_t_ids = {tid for tid in valid_t_ids_accessed_key if tid < t_id}
                    t_id_dependencies[t_id].update(smaller_t_ids)

        return t_id_dependencies, tid_locks

    def remove_aborted_from_rw_sets(self, global_logic_aborts: set[int]):
        """
        Here we delete the t_ids of the aborted transactions from the rw sets and reservations as if they never existed.
        """
        if not global_logic_aborts:
            return

        # Remove aborted t_ids from read_sets and write_sets
        self.read_sets = {
            operator_name: {tid: value for tid, value in self.read_sets[operator_name].items()
                            if tid not in global_logic_aborts}
            for operator_name in self.operator_names
        }
        self.write_sets = {
            operator_name: {tid: value for tid, value in self.write_sets[operator_name].items()
                            if tid not in global_logic_aborts}
            for operator_name in self.operator_names
        }

        # Update reads and writes dictionaries
        self.reads = {
            operator_name: {key: [tid for tid in t_ids if tid not in global_logic_aborts]
                            for key, t_ids in self.reads[operator_name].items()}
            for operator_name in self.operator_names
        }
        self.writes = {
            operator_name: {key: [tid for tid in t_ids if tid not in global_logic_aborts]
                            for key, t_ids in self.writes[operator_name].items()}
            for operator_name in self.operator_names
        }

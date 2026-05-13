from abc import abstractmethod
import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING

from styx.common.base_state import BaseOperatorState

from worker.operator_state.aria._aria_state import (
    check_conflicts as _cy_check_conflicts,
    check_conflicts_deterministic_reordering as _cy_check_conflicts_dr,
    check_conflicts_snapshot_isolation as _cy_check_conflicts_si,
    deal_with_reads as _cy_deal_with_reads,
    has_conflicts as _cy_has_conflicts,
    min_rw_reservations as _cy_min_rw_reservations,
    remove_aborted_from_rw_sets as _cy_remove_aborted,
    state_put as _cy_state_put,
)

if TYPE_CHECKING:
    from styx.common.types import K, OperatorPartition, V


class BaseAriaState(BaseOperatorState):
    # read write sets
    # operator_name: |t_id: set|keys||
    read_sets: dict[OperatorPartition, dict[int, set[K]]]
    global_read_sets: dict[OperatorPartition, dict[int, set[K]]]
    # operator_name: |t_id: |key: value||
    write_sets: dict[OperatorPartition, dict[int, dict[K, V]]]
    global_write_sets: dict[OperatorPartition, dict[int, dict[K, V]]]
    # the reads and writes with the lowest t_id
    # operator_name: |key: t_id|
    writes: dict[OperatorPartition, dict[K, list[int]]]
    # operator_name: |key: t_id|
    reads: dict[OperatorPartition, dict[K, list[int]]]
    global_reads: dict[OperatorPartition, dict[K, list[int]]]
    # Calvin snapshot things
    # tid: | operator_name: |key, value|
    fallback_commit_buffer: dict[int, dict[OperatorPartition, dict[K, V]]]
    # Fallback read tracking for rw-set change detection (Aria paper §4.2)
    # tid: | operator_partition: set of keys read during fallback
    fallback_read_sets: dict[int, dict[OperatorPartition, set[K]]]

    def __init__(self, operator_partitions: set[OperatorPartition]) -> None:
        super().__init__(operator_partitions)
        self.write_sets = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.writes = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.reads = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.read_sets = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.global_write_sets = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.global_reads = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.global_read_sets = {operator_partition: {} for operator_partition in self.operator_partitions}
        self.fallback_commit_buffer = defaultdict(lambda: defaultdict(dict))
        self.fallback_read_sets: dict[int, dict[OperatorPartition, set[K]]] = {}

    def put(
        self,
        key: K,
        value: V,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> None:
        _cy_state_put(self.write_sets, self.writes, key, value, t_id, operator_name, partition)

    def put_immediate(
        self,
        key: K,
        value: V,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> None:
        operator_partition: OperatorPartition = (operator_name, partition)
        if t_id in self.fallback_commit_buffer:
            if operator_partition in self.fallback_commit_buffer[t_id]:
                self.fallback_commit_buffer[t_id][operator_partition][key] = value
            else:
                self.fallback_commit_buffer[t_id][operator_partition] = {key: value}
        else:
            self.fallback_commit_buffer[t_id] = {operator_partition: {key: value}}

    def set_global_read_write_sets(
        self,
        global_read_reservations: dict[OperatorPartition, dict[K, list[int]]],
        global_write_set: dict[OperatorPartition, dict[int, dict[K, V]]],
        global_read_set: dict[OperatorPartition, dict[int, set[K]]],
    ) -> None:
        self.global_reads = global_read_reservations
        self.global_write_sets = global_write_set
        self.global_read_sets = global_read_set

    @abstractmethod
    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit_fallback_transaction(self, t_id: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        raise NotImplementedError

    @abstractmethod
    def get_immediate(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: K, operator_name: str, partition: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: K, operator_name: str, partition: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def commit(self, aborted_from_remote: set[int]) -> set[int]:
        raise NotImplementedError

    def deal_with_reads(
        self,
        key: K,
        t_id: int,
        operator_partition: OperatorPartition,
    ) -> None:
        _cy_deal_with_reads(self.reads, self.read_sets, key, t_id, operator_partition)

    @staticmethod
    def has_conflicts(t_id: int, keys: set[K], reservations: dict[K, int]) -> bool:
        return _cy_has_conflicts(t_id, keys, reservations)

    @staticmethod
    def min_rw_reservations(
        reservations: dict[OperatorPartition, dict[K, list[int]]],
    ) -> dict[OperatorPartition, dict[K, int]]:
        return _cy_min_rw_reservations(reservations)

    def check_conflicts(self) -> set[int]:
        """Checks for conflicts based on Arias default method

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        return _cy_check_conflicts(self.write_sets, self.read_sets, self.writes)

    def check_conflicts_deterministic_reordering(self) -> set[int]:
        """Checks for conflicts based on Arias deterministic reordering method

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        return _cy_check_conflicts_dr(
            self.write_sets,
            self.read_sets,
            self.writes,
            self.global_reads,
            self.global_write_sets,
            self.global_read_sets,
        )

    def check_conflicts_snapshot_isolation(self) -> set[int]:
        """Checks for conflicts based only on write-after-write dependencies leading to snapshot isolation

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        return _cy_check_conflicts_si(self.write_sets, self.writes)

    def has_fallback_rw_set_changed(self, t_id: int) -> bool:
        """Check if a transaction's read/write set changed during fallback re-execution.

        Per the Aria paper, if a transaction's rw-set differs from its original
        optimistic execution, the fallback dependency graph is invalid and the
        transaction must be rescheduled to the next epoch.
        """
        from styx.common.logging import logging

        fallback_reads = self.fallback_read_sets.get(t_id, {})
        for op_part in self.operator_partitions:
            original_read_keys = self.read_sets[op_part].get(t_id, set())
            fallback_read_keys = fallback_reads.get(op_part, set())
            if original_read_keys != fallback_read_keys:
                logging.warning(
                    f"RW_CHANGED t_id={t_id} READS op={op_part} "
                    f"orig={sorted(original_read_keys)[:8]} "
                    f"fb={sorted(fallback_read_keys)[:8]} "
                    f"orig_n={len(original_read_keys)} fb_n={len(fallback_read_keys)} "
                    f"orig-fb={sorted(original_read_keys - fallback_read_keys)[:5]} "
                    f"fb-orig={sorted(fallback_read_keys - original_read_keys)[:5]}",
                )
                return True

        for op_part in fallback_reads:
            if op_part not in self.operator_partitions:
                logging.warning(
                    f"RW_CHANGED t_id={t_id} READS on extra op={op_part} "
                    f"keys={sorted(fallback_reads[op_part])[:5]}",
                )
                return True

        fallback_writes = self.fallback_commit_buffer.get(t_id, {})
        for op_part in self.operator_partitions:
            original_write_keys = set(self.write_sets[op_part].get(t_id, {}).keys())
            fallback_write_keys = set(fallback_writes.get(op_part, {}).keys())
            if original_write_keys != fallback_write_keys:
                logging.warning(
                    f"RW_CHANGED t_id={t_id} WRITES op={op_part} "
                    f"orig_n={len(original_write_keys)} fb_n={len(fallback_write_keys)} "
                    f"orig-fb={sorted(original_write_keys - fallback_write_keys)[:5]} "
                    f"fb-orig={sorted(fallback_write_keys - original_write_keys)[:5]}",
                )
                return True

        for op_part in fallback_writes:
            if op_part not in self.operator_partitions:
                logging.warning(
                    f"RW_CHANGED t_id={t_id} WRITES on extra op={op_part}",
                )
                return True
        return False

    def cleanup(self) -> None:
        for operator_partition in self.operator_partitions:
            self.write_sets[operator_partition].clear()
            self.writes[operator_partition].clear()
            self.reads[operator_partition].clear()
            self.read_sets[operator_partition].clear()
            self.global_write_sets[operator_partition].clear()
            self.global_reads[operator_partition].clear()
            self.global_read_sets[operator_partition].clear()
        self.fallback_commit_buffer.clear()
        self.fallback_read_sets.clear()

    def get_dep_transactions(
        self,
        t_ids_to_reschedule: set[int],
    ) -> tuple[dict[int, set[int]], dict[int, asyncio.Event]]:
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
        for access_dict in combined_accesses.values():
            for t_ids in access_dict.values():
                valid_t_ids_accessed_key = t_ids_to_reschedule & set(t_ids)
                for t_id in valid_t_ids_accessed_key:
                    # Ensure smaller t_ids do not depend on larger ones
                    smaller_t_ids = {tid for tid in valid_t_ids_accessed_key if tid < t_id}
                    t_id_dependencies[t_id].update(smaller_t_ids)

        return t_id_dependencies, tid_locks

    def remove_aborted_from_rw_sets(self, global_logic_aborts: set[int]) -> None:
        """
        Here we delete the t_ids of the aborted transactions from the rw sets and reservations as if they never existed.
        """
        _cy_remove_aborted(
            self.operator_partitions,
            self.read_sets,
            self.write_sets,
            self.reads,
            self.writes,
            global_logic_aborts,
        )

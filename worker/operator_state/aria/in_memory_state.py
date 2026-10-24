from collections import defaultdict
import traceback
from typing import TYPE_CHECKING

from styx.common.logging import logging

from worker.operator_state.aria._aria_state import (
    commit as _cy_commit,
    state_get as _cy_state_get,
    state_get_immediate as _cy_state_get_immediate,
)
from worker.operator_state.aria.base_aria_state import BaseAriaState
from worker.operator_state.aria.fast_copy import fast_deepcopy

if TYPE_CHECKING:
    from styx.common.types import K, KVPairs, OperatorPartition, V

# Marks a key that is not in a partition at all. A key can hold None as its value,
# so migration cannot use None to mean "already left in a batch".
_ABSENT = object()


class InMemoryOperatorState(BaseAriaState):
    data: dict[OperatorPartition, KVPairs]
    delta_map: dict[OperatorPartition, KVPairs]

    def __init__(self, operator_partitions: set[OperatorPartition]) -> None:
        super().__init__(operator_partitions)
        self.data = {}
        self.delta_map = {}
        for operator_partition in self.operator_partitions:
            self.data[operator_partition] = {}
            self.delta_map[operator_partition] = {}
        # State migration data structures
        # Where do the keys belong (operator_partition: key: (worker_id, old partition))
        self.remote_keys: dict[OperatorPartition, dict[K, tuple[int, int]]] = {}
        # Used to track migration progress, the async migration and whether the operator still owns the specific key
        # (operator_name, old_partition): set of keys with the new partition
        self.keys_to_send: dict[OperatorPartition, set[tuple[K, int]]] = {}

    def add_keys_to_send(self, keys_to_send: dict[OperatorPartition, K]) -> None:
        self.keys_to_send = keys_to_send

    def has_keys_to_send(self) -> bool:
        return bool(self.keys_to_send)

    def get_key_to_migrate(
        self,
        new_operator_partition: OperatorPartition,
        key: K,
        old_partition: int,
    ) -> tuple[bool, V]:
        """Take the key out of old_partition on its way to new_operator_partition.

        Returns (found, value): found is False when the key already left in a
        migration batch. The value itself may be None.
        """
        operator_name, new_partition = new_operator_partition
        operator_partition: OperatorPartition = (operator_name, old_partition)
        data_to_send = self.data[operator_partition].pop(key, _ABSENT)
        if data_to_send is _ABSENT:
            # Key was already transferred via async migration batch
            return False, None
        # Record source-side key removal as tombstone for snapshotting
        self.delta_map[operator_partition][key] = None
        if operator_partition in self.keys_to_send:
            self.keys_to_send[operator_partition].discard((key, new_partition))
        return True, data_to_send

    def set_data_from_migration(
        self,
        operator_partition: OperatorPartition,
        key: K,
        data: V,
    ) -> None:
        operator_partition = tuple(operator_partition)
        self.data[operator_partition][key] = data
        self.delta_map[operator_partition][key] = data
        if operator_partition in self.remote_keys:
            self.remote_keys[operator_partition].pop(key, None)
            if not self.remote_keys[operator_partition]:
                del self.remote_keys[operator_partition]

    def migrate_within_the_same_worker(
        self,
        operator_name: str,
        new_partition: int,
        key: K,
        old_partition: int,
    ) -> bool:
        new_operator_partition: OperatorPartition = (operator_name, new_partition)
        found, data = self.get_key_to_migrate(new_operator_partition, key, old_partition)
        if found:
            self.set_data_from_migration(new_operator_partition, key, data)
        # Not found: the async sender already took the key into a batch to this worker.
        return found

    def keys_remaining_to_remote(self) -> int:
        c = 0
        for keys in self.remote_keys.values():
            c += len(keys)
        return c

    def keys_remaining_to_send(self) -> int:
        c = 0
        for keys in self.keys_to_send.values():
            c += len(keys)
        return c

    def get_async_migrate_batch(
        self,
        batch_size: int,
    ) -> dict[OperatorPartition, KVPairs]:
        batch_to_send: dict[OperatorPartition, KVPairs] = defaultdict(dict)
        c = 0
        operator_partitions_to_clear = []
        for operator_partition, keys in self.keys_to_send.items():
            operator_name, _ = operator_partition
            while keys and c < batch_size:
                key, new_partition = keys.pop()
                value = self.data[operator_partition].pop(key, _ABSENT)
                if value is _ABSENT:
                    # Key already left via get_key_to_migrate — skip it
                    continue
                # A None value is sent too: the destination still expects the key.
                # Record source-side key removal as tombstone for snapshotting
                self.delta_map[operator_partition][key] = None
                batch_to_send[(operator_name, new_partition)][key] = value
                c += 1
            if not keys:
                operator_partitions_to_clear.append(operator_partition)
            if c >= batch_size:
                break
        # Remove emptied partitions
        for operator_partition in operator_partitions_to_clear:
            del self.keys_to_send[operator_partition]
        return batch_to_send

    def set_batch_data_from_migration(
        self,
        operator_partition: OperatorPartition,
        kv_pairs: KVPairs,
    ) -> None:
        operator_partition = tuple(operator_partition)  # new partitioning
        self.data[operator_partition].update(kv_pairs)
        self.delta_map[operator_partition].update(kv_pairs)
        # Guard: remote_keys may not have entries if async batch arrived
        # before hash metadata, or entries were already removed.
        if operator_partition in self.remote_keys:
            for key in kv_pairs:
                self.remote_keys[operator_partition].pop(key, None)
            if not self.remote_keys[operator_partition]:
                del self.remote_keys[operator_partition]

    def get_worker_id_old_partition(
        self,
        operator_name: str,
        partition: int,
        key: K,
    ) -> tuple[int, int] | None:
        """
        Returns the worker ID and worker index for a given key from a previously assigned operator partition.

        This method is getting call only during a migration cycle. It can return None because the key could have been
        already migrated from a previous function call.

        Args:
            operator_name (str): The name of the operator.
            partition (int): The partition index of the operator.
            key (Any): The key whose worker mapping is being queried.

        Returns:
            tuple[int, int] | None: A tuple of (worker_id, worker_index) if the key is found,
            otherwise `None`.
        """
        operator_partition = (operator_name, partition)
        if operator_partition in self.remote_keys and key in self.remote_keys[operator_partition]:
            return self.remote_keys[operator_partition][key]
        return None

    def add_new_operator_partition(self, operator_partition: OperatorPartition) -> None:
        operator_partition = tuple(operator_partition)
        if operator_partition not in self.operator_partitions:
            self.operator_partitions.add(operator_partition)
            self.data[operator_partition] = {}
            self.delta_map[operator_partition] = {}

    def add_remote_keys(
        self,
        operator_partition: OperatorPartition,
        data: dict[K, tuple[int, int]],
    ) -> None:
        operator_partition = tuple(operator_partition)
        if operator_partition in self.remote_keys:
            self.remote_keys[operator_partition].update(data)
        else:
            self.remote_keys[operator_partition] = data

    def set_data_from_snapshot(self, data: dict[OperatorPartition, KVPairs]) -> None:
        for operator_partition, kv_pairs in data.items():
            self.data[operator_partition] = kv_pairs

    def get_operator_partitions_to_repartition(
        self,
    ) -> dict[str, set[OperatorPartition]]:
        res = {operator_name: set() for operator_name, _ in self.operator_partitions}
        for operator_name, partition in self.operator_partitions:
            res[operator_name].add((operator_name, partition))
        return res

    def get_operator_data_for_repartitioning(
        self,
        operator: OperatorPartition,
    ) -> KVPairs:
        return self.data[operator]

    def get_data_for_snapshot(self) -> dict[OperatorPartition, KVPairs]:
        return self.delta_map

    def clear_delta_map(self) -> None:
        for operator_partition in self.operator_partitions:
            self.delta_map[operator_partition].clear()

    def take_delta_map(self) -> dict[OperatorPartition, KVPairs]:
        """Hand over the accumulated deltas and continue on a fresh map.

        Swapping (instead of clearing once the caller is done) matters because the
        caller awaits while shipping the deltas: async-migration writes that land
        during that await must go into the next delta, not be wiped unsent.
        """
        delta = self.delta_map
        self.delta_map = {operator_partition: {} for operator_partition in self.operator_partitions}
        return delta

    def commit_fallback_transaction(self, t_id: int) -> None:
        if t_id in self.fallback_commit_buffer:
            for operator_partition, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    self.data[operator_partition][key] = value
                    self.delta_map[operator_partition][key] = value

    def get_all(
        self,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> dict[OperatorPartition, KVPairs]:
        operator_partition: OperatorPartition = (operator_name, partition)
        for key in self.data[operator_partition]:
            self.deal_with_reads(key, t_id, operator_partition)
        return fast_deepcopy(self.data[operator_partition])

    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int) -> None:
        operator_partition: OperatorPartition = (operator_name, partition)
        self.data[operator_partition].update(kv_pairs)
        self.delta_map[operator_partition].update(kv_pairs)

    def get(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        return _cy_state_get(
            self.data,
            self.write_sets,
            self.reads,
            self.read_sets,
            key,
            t_id,
            operator_name,
            partition,
        )

    def get_immediate(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        return _cy_state_get_immediate(
            self.data,
            self.fallback_commit_buffer,
            key,
            t_id,
            operator_name,
            partition,
            self.fallback_read_sets,
        )

    def delete(self, key: K, operator_name: str, partition: int) -> None:
        # Need to find a way to implement deletes
        pass

    def in_remote_keys(self, key: K, operator_name: str, partition: int) -> bool:
        operator_partition: OperatorPartition = (operator_name, partition)
        return operator_partition in self.remote_keys and key in self.remote_keys[operator_partition]

    def exists(self, key: K, operator_name: str, partition: int) -> bool:
        operator_partition: OperatorPartition = (operator_name, partition)
        if operator_partition not in self.data:
            return False
        if key in self.data[operator_partition]:
            # During migration: a key still in data but pending send to another
            # partition should not be considered as belonging here.
            if operator_partition in self.keys_to_send:
                return not any(k == key for k, _ in self.keys_to_send[operator_partition])
            return True
        # During migration: key is expected to arrive from a remote worker
        return self.in_remote_keys(key, operator_name, partition)

    def commit(self, aborted_from_remote: set[int]) -> set[int]:
        try:
            return _cy_commit(self.write_sets, self.data, self.delta_map, aborted_from_remote)
        except Exception as e:
            logging.warning(traceback.format_exc())
            raise e

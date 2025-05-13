import traceback
from collections import defaultdict
from typing import Any

from msgspec import msgpack

from styx.common.types import OperatorPartition, KVPairs
from styx.common.logging import logging

from worker.operator_state.aria.base_aria_state import BaseAriaState


class InMemoryOperatorState(BaseAriaState):

    data: dict[OperatorPartition, KVPairs]
    delta_map: dict[OperatorPartition, KVPairs]

    def __init__(self, operator_partitions: set[OperatorPartition]):
        super().__init__(operator_partitions)
        self.data = {}
        self.delta_map = {}
        for operator_partition in self.operator_partitions:
            self.data[operator_partition] = {}
            self.delta_map[operator_partition] = {}
        # State migration data structures
        # Where do the keys belong (operator_partition: key: (worker_id, old partition))
        self.remote_keys: dict[OperatorPartition, dict[Any, tuple[int, int]]] = {}
        # Used to track migration progress, the async migration and whether the operator still owns the specific key
        # (operator_name, old_partition): set of keys with the new partition
        self.keys_to_send: dict[OperatorPartition, set[tuple[Any, int]]] = {}

    def add_keys_to_send(self, keys_to_send: dict[OperatorPartition, Any]):
        self.keys_to_send = keys_to_send

    def has_keys_to_send(self) -> bool:
        return bool(self.keys_to_send)

    def get_key_to_migrate(self, new_operator_partition: OperatorPartition, key: Any, old_partition: int):
        operator_name, new_partition = new_operator_partition
        operator_partition: OperatorPartition = (operator_name, old_partition)
        data_to_send = self.data[operator_partition].pop(key)
        self.keys_to_send[operator_partition].remove((key, new_partition))
        # logging.warning(f"Keys left to send in partition: {operator_partition} -> {len(self.keys_to_send[operator_partition])}")
        return data_to_send

    def set_data_from_migration(self, operator_partition: OperatorPartition, key: Any, data: Any):
        operator_partition = tuple(operator_partition)
        self.data[operator_partition][key] = data
        del self.remote_keys[operator_partition][key]
        if not self.remote_keys[operator_partition]:
            del self.remote_keys[operator_partition]

    def keys_remaining_to_remote(self):
        c = 0
        for keys in self.remote_keys.values():
            c += len(keys)
        return c

    def get_async_migrate_batch(self, batch_size: int) -> dict[OperatorPartition, KVPairs]:
        batch_to_send: dict[OperatorPartition, KVPairs] = defaultdict(dict)
        c = 0
        operator_partitions_to_clear = []
        for operator_partition, keys in self.keys_to_send.items():
            operator_name, old_partition = operator_partition
            while keys and c < batch_size:
                key, new_partition = keys.pop()
                value = self.data[operator_partition].pop(key)
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

    def set_batch_data_from_migration(self, operator_partition: OperatorPartition, kv_pairs: KVPairs):
        operator_partition = tuple(operator_partition) # new partitioning
        self.data[operator_partition].update(kv_pairs)
        for key in kv_pairs.keys():
            del self.remote_keys[operator_partition][key]
        if not self.remote_keys[operator_partition]:
            del self.remote_keys[operator_partition]

    def get_worker_id_old_partition(self, operator_name: str, partition: int, key: Any) -> tuple[int, int] | None:
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
        else:
            return None

    def add_new_operator_partition(self, operator_partition: OperatorPartition):
        operator_partition = tuple(operator_partition)
        if operator_partition not in self.operator_partitions:
            self.operator_partitions.add(operator_partition)
            self.data[operator_partition] = {}
            self.delta_map[operator_partition] = {}

    def add_remote_keys(self, operator_partition: OperatorPartition, data: dict[Any, tuple[int, int]]):
        operator_partition = tuple(operator_partition)
        if operator_partition in self.remote_keys:
            self.remote_keys[operator_partition].update(data)
        else:
            self.remote_keys[operator_partition] = data

    def set_data_from_snapshot(self, data: dict[OperatorPartition, KVPairs]):
        for operator_partition, kv_pairs in data.items():
            self.data[operator_partition] = kv_pairs

    def get_operator_partitions_to_repartition(self) -> dict[str, set[OperatorPartition]]:
        res = {operator_name: set() for operator_name, _ in self.operator_partitions}
        for operator_name, partition in self.operator_partitions:
            res[operator_name].add((operator_name, partition))
        return res

    def get_operator_data_for_repartitioning(self, operator: OperatorPartition) -> KVPairs:
        return self.data[operator]

    def get_data_for_snapshot(self) -> dict[OperatorPartition, KVPairs]:
        return self.delta_map

    def clear_delta_map(self):
        for operator_partition in self.operator_partitions:
            self.delta_map[operator_partition].clear()

    def commit_fallback_transaction(self, t_id: int):
        if t_id in self.fallback_commit_buffer:
            for operator_partition, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    self.data[operator_partition][key] = value
                    self.delta_map[operator_partition][key] = value

    def get_all(self, t_id: int, operator_name: str, partition: int):
        operator_partition: OperatorPartition = (operator_name, partition)
        for key in self.data[operator_partition].keys():
            self.deal_with_reads(key, t_id, operator_partition)
        return msgpack.decode(msgpack.encode(self.data[operator_partition]))

    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int):
        operator_partition: OperatorPartition = (operator_name, partition)
        self.data[operator_partition].update(kv_pairs)
        self.delta_map[operator_partition].update(kv_pairs)

    def get(self, key, t_id: int, operator_name: str, partition: int) -> any:
        operator_partition: OperatorPartition = (operator_name, partition)
        self.deal_with_reads(key, t_id, operator_partition)
        # if transaction wrote to this key, read from the write set
        if t_id in self.write_sets[operator_partition] and key in self.write_sets[operator_partition][t_id]:
            return msgpack.decode(msgpack.encode(self.write_sets[operator_partition][t_id][key]))
        return msgpack.decode(msgpack.encode(self.data[operator_partition].get(key)))

    def get_immediate(self, key, t_id: int, operator_name: str, partition: int):
        operator_partition: OperatorPartition = (operator_name, partition)
        if (t_id in self.fallback_commit_buffer and
                operator_partition in self.fallback_commit_buffer[t_id] and
                key in self.fallback_commit_buffer[t_id][operator_partition]):
            return msgpack.decode(msgpack.encode(self.fallback_commit_buffer[t_id][operator_partition][key]))
        return msgpack.decode(msgpack.encode(self.data[operator_partition].get(key)))

    def delete(self, key, operator_name: str, partition: int):
        # Need to find a way to implement deletes
        pass

    def in_remote_keys(self, key, operator_name: str, partition: int) -> bool:
        operator_partition: OperatorPartition = (operator_name, partition)
        return operator_partition in self.remote_keys and key in self.remote_keys[operator_partition]

    def exists(self, key, operator_name: str, partition: int):
        operator_partition: OperatorPartition = (operator_name, partition)
        if operator_partition not in self.data:
            return False
        return (operator_partition in self.keys_to_send and key not in self.keys_to_send[operator_partition] and key in self.data[operator_partition]) or self.in_remote_keys(key, operator_name, partition)

    def commit(self, aborted_from_remote: set[int]) -> set[int]:
        committed_t_ids = set()
        try:
            for operator_name in self.write_sets.keys():
                updates_to_commit = {}
                for t_id, ws in self.write_sets[operator_name].items():
                    if t_id not in aborted_from_remote:
                        updates_to_commit.update(ws)
                        committed_t_ids.add(t_id)
                self.data[operator_name].update(updates_to_commit)
                self.delta_map[operator_name].update(updates_to_commit)
        except Exception as e:
            logging.warning(traceback.format_exc())
            raise e
        return committed_t_ids

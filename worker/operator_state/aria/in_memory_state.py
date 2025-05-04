import traceback

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

    def exists(self, key, operator_name: str, partition: int):
        operator_partition: OperatorPartition = (operator_name, partition)
        if operator_partition not in self.data:
            return False
        return key in self.data[operator_partition]

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

from typing import Any

from msgspec import msgpack

from worker.operator_state.aria.base_aria_state import BaseAriaState


class InMemoryOperatorState(BaseAriaState):

    data: dict[str, dict[Any, Any]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.data = {}
        self.delta_map = {}
        for operator_name in self.operator_names:
            self.data[operator_name] = {}
            self.delta_map[operator_name] = {}

    def set_data_from_snapshot(self, data: dict[str, dict[Any, Any]]):
        if data:
            self.data = data

    def get_data_for_snapshot(self):
        return self.delta_map

    def clear_delta_map(self):
        for operator_name in self.operator_names:
            self.delta_map[operator_name] = {}

    def commit_fallback_transaction(self, t_id: int):
        if t_id in self.fallback_commit_buffer:
            for operator_name, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    self.data[operator_name][key] = value
                    self.delta_map[operator_name][key] = value

    def get_all(self, t_id: int, operator_name: str):
        for key in self.data[operator_name].keys():
            self.deal_with_reads(key, t_id, operator_name)
        return self.data[operator_name]

    def batch_insert(self, kv_pairs: dict, operator_name: str):
        self.data[operator_name].update(kv_pairs)
        self.delta_map[operator_name].update(kv_pairs)

    def get(self, key, t_id: int, operator_name: str) -> Any:
        self.deal_with_reads(key, t_id, operator_name)
        # if transaction wrote to this key, read from the write set
        if t_id in self.write_sets[operator_name] and key in self.write_sets[operator_name][t_id]:
            return self.write_sets[operator_name][t_id][key]
        return msgpack.decode(msgpack.encode(self.data[operator_name].get(key)))

    def get_immediate(self, key, t_id: int, operator_name: str):
        if key in self.fallback_commit_buffer[t_id][operator_name]:
            return self.fallback_commit_buffer[t_id][operator_name][key]
        return msgpack.decode(msgpack.encode(self.data[operator_name].get(key)))

    def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    def exists(self, key, operator_name: str):
        return key in self.data[operator_name]

    def commit(self, aborted_from_remote: set[int]) -> set[int]:
        committed_t_ids = set()
        for operator_name in self.write_sets.keys():
            updates_to_commit = {}
            for t_id, ws in self.write_sets[operator_name].items():
                if t_id not in aborted_from_remote:
                    updates_to_commit.update(ws)
                    committed_t_ids.add(t_id)
            self.data[operator_name].update(updates_to_commit)
            self.delta_map[operator_name].update(updates_to_commit)
        return committed_t_ids

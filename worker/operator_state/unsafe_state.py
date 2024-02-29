from typing import Any

from styx.common.base_state import BaseOperatorState


class UnsafeOperatorState(BaseOperatorState):

    data: dict[str, dict[Any, Any]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.data = {}
        for operator_name in operator_names:
            self.data[operator_name] = {}

    def set_data_from_snapshot(self, data: dict[str, dict[Any, Any]]):
        self.data = data

    async def get(self, key, t_id: int, operator_name: str) -> Any:
        return self.data[operator_name].get(key)

    async def put(self, key, value, t_id: int, operator_name: str):
        self.data[operator_name][key] = value

    async def put_immediate(self, key, value, t_id: int, operator_name: str):
        """Fallback not applicable"""
        pass

    async def get_immediate(self, key, t_id: int, operator_name: str):
        """Fallback not applicable"""
        pass

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if key in self.data[operator_name] else False

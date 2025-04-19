import cityhash


from .base_partitioner import BasePartitioner
from ..exceptions import NonSupportedKeyType


class HashPartitioner(BasePartitioner):

    def __init__(self, partitions: int):
        self._partitions = partitions

    def update_partitions(self, partitions: int):
        self._partitions = partitions

    @property
    def partitions(self):
        return self._partitions

    def get_partition(self, key) -> int | None:
        if key is None:
            return None
        return self.make_key_hashable(key) % self._partitions

    @staticmethod
    def make_key_hashable(key) -> int:
        if isinstance(key, int):
            return key
        else:
            try:
                return cityhash.CityHash64(key)
            except Exception:
                raise NonSupportedKeyType()

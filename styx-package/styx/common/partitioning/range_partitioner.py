from .base_partitioner import BasePartitioner
from ..exceptions import InvalidRangePartitioning


class RangePartitioner(BasePartitioner):

    def __init__(self, ranges: list[tuple[int, int]]):
        # ranges is a sorted list of inclusive ranges where the index is the partition id
        self._ranges = ranges

    def get_partition(self, key: int) -> int:
        low = 0
        high = len(self._ranges) - 1
        while low <= high:
            mid = (high + low) // 2
            mid_lower, mid_upper = self._ranges[mid]
            if mid_lower <= key <= mid_upper:
                return mid
            elif key < mid_lower:
                high = mid - 1
            else:
                low = mid + 1
        raise InvalidRangePartitioning()

    def update_ranges(self, ranges: list[tuple[int, int]]):
        self._ranges = ranges

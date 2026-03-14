"""Unit tests for styx/common/partitioning/range_partitioner.py"""

import pytest
from styx.common.exceptions import InvalidRangePartitioningError
from styx.common.partitioning.range_partitioner import RangePartitioner


class TestGetPartition:
    def test_key_in_first_range(self):
        rp = RangePartitioner([(0, 9), (10, 19), (20, 29)])
        assert rp.get_partition(5) == 0

    def test_key_in_middle_range(self):
        rp = RangePartitioner([(0, 9), (10, 19), (20, 29)])
        assert rp.get_partition(15) == 1

    def test_key_in_last_range(self):
        rp = RangePartitioner([(0, 9), (10, 19), (20, 29)])
        assert rp.get_partition(25) == 2

    def test_key_at_lower_boundary(self):
        rp = RangePartitioner([(0, 9), (10, 19)])
        assert rp.get_partition(0) == 0
        assert rp.get_partition(10) == 1

    def test_key_at_upper_boundary(self):
        rp = RangePartitioner([(0, 9), (10, 19)])
        assert rp.get_partition(9) == 0
        assert rp.get_partition(19) == 1

    def test_key_below_min_raises(self):
        rp = RangePartitioner([(10, 19), (20, 29)])
        with pytest.raises(InvalidRangePartitioningError):
            rp.get_partition(5)

    def test_key_above_max_raises(self):
        rp = RangePartitioner([(0, 9), (10, 19)])
        with pytest.raises(InvalidRangePartitioningError):
            rp.get_partition(20)

    def test_key_in_gap_raises(self):
        rp = RangePartitioner([(0, 4), (10, 14)])
        with pytest.raises(InvalidRangePartitioningError):
            rp.get_partition(7)

    def test_single_partition(self):
        rp = RangePartitioner([(0, 100)])
        assert rp.get_partition(0) == 0
        assert rp.get_partition(50) == 0
        assert rp.get_partition(100) == 0

    def test_single_element_ranges(self):
        rp = RangePartitioner([(5, 5), (10, 10), (15, 15)])
        assert rp.get_partition(5) == 0
        assert rp.get_partition(10) == 1
        assert rp.get_partition(15) == 2


class TestUpdateRanges:
    def test_update_changes_ranges(self):
        rp = RangePartitioner([(0, 9)])
        assert rp.get_partition(5) == 0
        rp.update_ranges([(0, 4), (5, 9)])
        assert rp.get_partition(3) == 0
        assert rp.get_partition(7) == 1

    def test_update_to_single_range(self):
        rp = RangePartitioner([(0, 4), (5, 9)])
        rp.update_ranges([(0, 9)])
        assert rp.get_partition(7) == 0

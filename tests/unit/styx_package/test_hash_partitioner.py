"""Unit tests for styx/common/partitioning/hash_partitioner.py"""

import pytest

from styx.common.exceptions import NonSupportedKeyTypeError
from styx.common.partitioning.hash_partitioner import HashPartitioner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _partitioner(n: int = 4, composite: tuple | None = None) -> HashPartitioner:
    return HashPartitioner(n, composite)


# ---------------------------------------------------------------------------
# make_key_hashable
# ---------------------------------------------------------------------------


class TestMakeKeyHashable:
    def test_int_returns_itself(self):
        assert HashPartitioner.make_key_hashable(5) == 5

    def test_negative_int_returns_itself(self):
        assert HashPartitioner.make_key_hashable(-3) == -3

    def test_str_digit_returns_int(self):
        assert HashPartitioner.make_key_hashable("42") == 42

    def test_str_non_digit_returns_cityhash(self):
        result = HashPartitioner.make_key_hashable("hello")
        assert isinstance(result, int)

    def test_str_non_digit_is_deterministic(self):
        a = HashPartitioner.make_key_hashable("styx")
        b = HashPartitioner.make_key_hashable("styx")
        assert a == b

    def test_unsupported_type_raises(self):
        with pytest.raises(NonSupportedKeyTypeError):
            HashPartitioner.make_key_hashable([1, 2, 3])

    def test_dict_key_raises(self):
        with pytest.raises(NonSupportedKeyTypeError):
            HashPartitioner.make_key_hashable({"a": 1})


# ---------------------------------------------------------------------------
# get_partition
# ---------------------------------------------------------------------------


class TestGetPartition:
    def test_none_key_returns_none(self):
        p = _partitioner(4)
        assert p.get_partition(None) is None

    def test_int_key_is_modulo(self):
        p = _partitioner(4)
        assert p.get_partition(7) == 7 % 4
        assert p.get_partition(0) == 0

    def test_result_in_range(self):
        p = _partitioner(8)
        for key in ["alpha", "beta", "gamma", "delta", "key_1", "key_2"]:
            result = p.get_partition(key)
            assert 0 <= result < 8

    def test_single_partition_always_zero(self):
        p = _partitioner(1)
        for key in [0, 1, "abc", "xyz", 999]:
            assert p.get_partition(key) == 0

    def test_deterministic(self):
        p = _partitioner(16)
        for key in [42, "worker", "user:1", "order:999"]:
            assert p.get_partition(key) == p.get_partition(key)

    def test_different_keys_can_map_to_different_partitions(self):
        p = _partitioner(10)
        results = {p.get_partition(f"key_{i}") for i in range(100)}
        assert len(results) > 1


# ---------------------------------------------------------------------------
# update_partitions
# ---------------------------------------------------------------------------


class TestUpdatePartitions:
    def test_update_changes_partition_count(self):
        p = _partitioner(4)
        p.update_partitions(8)
        assert p.partitions == 8

    def test_int_key_after_update(self):
        p = _partitioner(4)
        assert p.get_partition_no_cache(8) == 8 % 4
        p.update_partitions(3)
        assert p.get_partition_no_cache(8) == 8 % 3

    def test_no_cache_reflects_new_count(self):
        p = _partitioner(10)
        p.update_partitions(2)
        result = p.get_partition_no_cache(5)
        assert result == 5 % 2


# ---------------------------------------------------------------------------
# Composite key
# ---------------------------------------------------------------------------


class TestCompositeKey:
    def test_extracts_second_field(self):
        # field=1, delim=":" → "user:123".split(":")[1] == "123"
        p = _partitioner(8, composite=(1, ":"))
        # "123" is a digit string → int(123) % 8
        assert p.get_partition_no_cache("user:123") == 123 % 8

    def test_extracts_first_field(self):
        p = _partitioner(4, composite=(0, "|"))
        # "42|extra".split("|")[0] == "42" → int(42) % 4
        assert p.get_partition_no_cache("42|extra") == 42 % 4

    def test_same_suffix_same_partition(self):
        p = _partitioner(8, composite=(1, ":"))
        a = p.get_partition_no_cache("alice:7")
        b = p.get_partition_no_cache("bob:7")
        assert a == b

    def test_different_suffix_can_differ(self):
        p = _partitioner(8, composite=(1, ":"))
        a = p.get_partition_no_cache("user:0")
        b = p.get_partition_no_cache("user:5")
        assert a != b

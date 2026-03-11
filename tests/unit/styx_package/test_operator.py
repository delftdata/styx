"""Unit tests for styx/common/operator.py"""

import asyncio

import pytest

from styx.common.exceptions import OperatorDoesNotContainFunctionError
from styx.common.operator import Operator
from styx.common.partitioning.hash_partitioner import HashPartitioner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _op(name: str = "users", n: int = 4) -> Operator:
    return Operator(name, n)


def _dummy_func():
    """A minimal callable used as a stand-in for a registered function."""


_dummy_func.__name__ = "dummy"


def _another_func():
    """A second stand-in function."""


_another_func.__name__ = "another"


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestOperatorInit:
    def test_name_stored(self):
        op = _op("orders", 2)
        assert op.name == "orders"

    def test_n_partitions_stored(self):
        op = _op("items", 6)
        assert op.n_partitions == 6

    def test_default_n_partitions(self):
        op = Operator("solo")
        assert op.n_partitions == 1

    def test_not_shadow_by_default(self):
        assert _op().is_shadow is False

    def test_functions_empty_on_init(self):
        assert _op().functions == {}

    def test_dns_empty_on_init(self):
        assert _op().dns == {}

    def test_partitioner_is_hash_partitioner(self):
        assert isinstance(_op().get_partitioner(), HashPartitioner)

    def test_partitioner_has_correct_count(self):
        op = _op("x", 7)
        assert op.get_partitioner().partitions == 7


# ---------------------------------------------------------------------------
# Shadow
# ---------------------------------------------------------------------------


class TestOperatorShadow:
    def test_make_shadow_sets_flag(self):
        op = _op()
        op.make_shadow()
        assert op.is_shadow is True

    def test_make_shadow_idempotent(self):
        op = _op()
        op.make_shadow()
        op.make_shadow()
        assert op.is_shadow is True


# ---------------------------------------------------------------------------
# register / functions
# ---------------------------------------------------------------------------


class TestOperatorRegister:
    def test_register_adds_by_name(self):
        op = _op()
        op.register(_dummy_func)
        assert "dummy" in op.functions
        assert op.functions["dummy"] is _dummy_func

    def test_register_multiple(self):
        op = _op()
        op.register(_dummy_func)
        op.register(_another_func)
        assert set(op.functions.keys()) == {"dummy", "another"}

    def test_register_overwrites(self):
        op = _op()
        op.register(_dummy_func)

        def dummy():  # noqa: ANN202
            return 99

        dummy.__name__ = "dummy"
        op.register(dummy)
        assert op.functions["dummy"] is dummy


# ---------------------------------------------------------------------------
# which_partition
# ---------------------------------------------------------------------------


class TestWhichPartition:
    def test_returns_int(self):
        op = _op("x", 4)
        assert isinstance(op.which_partition("somekey"), int)

    def test_result_in_range(self):
        op = _op("x", 8)
        for key in ["a", "b", "c", 0, 1, 100]:
            result = op.which_partition(key)
            assert 0 <= result < 8

    def test_consistent_with_partitioner(self):
        op = _op("x", 4)
        for key in ["alice", "bob", "charlie", 42]:
            assert op.which_partition(key) == op.get_partitioner().get_partition(key)

    def test_deterministic(self):
        op = _op("x", 10)
        key = "stable_key"
        assert op.which_partition(key) == op.which_partition(key)


# ---------------------------------------------------------------------------
# set_n_partitions
# ---------------------------------------------------------------------------


class TestSetNPartitions:
    def test_updates_n_partitions_attribute(self):
        op = _op("x", 4)
        op.set_n_partitions(8)
        assert op.n_partitions == 8

    def test_updates_partitioner(self):
        op = _op("x", 4)
        op.set_n_partitions(6)
        assert op.get_partitioner().partitions == 6

    def test_which_partition_reflects_new_count(self):
        op = _op("x", 4)
        op.set_n_partitions(2)
        # int key 3 → 3 % 2 == 1
        assert op.which_partition(3) in {0, 1}


# ---------------------------------------------------------------------------
# run_function — unregistered function raises
# ---------------------------------------------------------------------------


class TestRunFunctionErrors:
    def test_unregistered_function_raises(self):
        op = _op()
        with pytest.raises(OperatorDoesNotContainFunctionError):
            asyncio.run(
                op.run_function(
                    key="k",
                    t_id=1,
                    request_id=b"req",
                    function_name="nonexistent",
                    partition=0,
                    ack_payload=None,
                    fallback_mode=False,
                    use_fallback_cache=False,
                    params=(),
                    protocol=None,
                )
            )

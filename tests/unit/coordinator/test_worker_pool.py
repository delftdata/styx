"""Unit tests for coordinator/worker_pool.py"""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from coordinator.worker_pool import Worker, WorkerPool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(worker_id: int = 1, ip: str = "127.0.0.1", port: int = 5000, proto: int = 6000) -> Worker:
    return Worker(
        worker_id=worker_id,
        worker_ip=ip,
        worker_port=port,
        protocol_port=proto,
        assigned_operators={},
    )


def _mock_operator() -> MagicMock:
    return MagicMock()


def _run_sync_in_loop(fn, *args):
    """Run a sync function inside a running asyncio event loop."""

    async def _inner():
        return fn(*args)

    return asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Worker dataclass
# ---------------------------------------------------------------------------


class TestWorker:
    def test_priority_empty(self):
        w = _make_worker()
        assert w.priority == 0

    def test_priority_with_operators(self):
        w = _make_worker()
        w.assigned_operators[("op", 0)] = _mock_operator()
        w.assigned_operators[("op", 1)] = _mock_operator()
        assert w.priority == 2

    def test_participating_false_when_no_operators(self):
        w = _make_worker()
        assert not w.participating

    def test_participating_true_when_has_operators(self):
        w = _make_worker()
        w.assigned_operators[("op", 0)] = _mock_operator()
        assert w.participating

    def test_to_tuple(self):
        w = _make_worker(ip="10.0.0.1", port=5001, proto=6001)
        assert w.to_tuple() == ("10.0.0.1", 5001, 6001)

    def test_hash_uses_worker_id(self):
        w1 = _make_worker(worker_id=1)
        w2 = _make_worker(worker_id=1)
        assert hash(w1) == hash(w2)
        w3 = _make_worker(worker_id=2)
        assert hash(w1) != hash(w3)


# ---------------------------------------------------------------------------
# WorkerPool — registration
# ---------------------------------------------------------------------------


class TestWorkerPoolRegistration:
    def test_first_worker_gets_id_1(self):
        pool = WorkerPool()
        wid = pool.register_worker("127.0.0.1", 5000, 6000)
        assert wid == 1

    def test_sequential_ids(self):
        pool = WorkerPool()
        ids = [pool.register_worker("127.0.0.1", 5000 + i, 6000 + i) for i in range(4)]
        assert ids == [1, 2, 3, 4]

    def test_dead_id_is_reused(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)  # id=1
        pool.register_worker("127.0.0.1", 5001, 6001)  # id=2
        pool.remove_worker(1)
        pool.dead_worker_ids.append(1)
        new_id = pool.register_worker("127.0.0.1", 5002, 6002)
        assert new_id == 1

    def test_register_worker_is_returned_from_peek(self):
        pool = WorkerPool()
        wid = pool.register_worker("10.0.0.1", 5000, 6000)
        w = pool.peek(wid)
        assert w.worker_ip == "10.0.0.1"
        assert w.worker_port == 5000
        assert w.protocol_port == 6000


# ---------------------------------------------------------------------------
# WorkerPool — heap operations (put / pop / peek / remove)
# ---------------------------------------------------------------------------


class TestWorkerPoolHeap:
    def test_pop_from_empty_returns_none(self):
        pool = WorkerPool()
        assert pool.pop() is None

    def test_put_then_pop_returns_worker(self):
        pool = WorkerPool()
        w = _make_worker()
        pool.put(w)
        result = pool.pop()
        assert result is w

    def test_pop_returns_lowest_priority_first(self):
        pool = WorkerPool()
        w1 = _make_worker(worker_id=1)
        w1.assigned_operators[("op", 0)] = _mock_operator()  # priority 1
        w2 = _make_worker(worker_id=2)  # priority 0
        pool.put(w1)
        pool.put(w2)
        first = pool.pop()
        assert first.worker_id == 2  # lower priority wins

    def test_peek_returns_worker_without_removing(self):
        pool = WorkerPool()
        wid = pool.register_worker("127.0.0.1", 5000, 6000)
        w = pool.peek(wid)
        assert w.worker_id == wid
        # worker is still in the pool
        assert pool.peek(wid).worker_id == wid

    def test_peek_unknown_id_raises(self):
        pool = WorkerPool()
        with pytest.raises(KeyError):
            pool.peek(999)

    def test_remove_worker_marks_tombstone(self):
        pool = WorkerPool()
        w = _make_worker(worker_id=1)
        pool.put(w)
        removed = pool.remove_worker(1)
        assert removed is w
        # popping now should skip the tombstone and return None
        assert pool.pop() is None

    def test_remove_worker_unknown_id_raises(self):
        pool = WorkerPool()
        with pytest.raises(KeyError):
            pool.remove_worker(999)


# ---------------------------------------------------------------------------
# WorkerPool — operator scheduling
# ---------------------------------------------------------------------------


class TestWorkerPoolScheduling:
    def test_schedule_assigns_to_least_loaded_worker(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)  # w1
        pool.register_worker("127.0.0.1", 5001, 6001)  # w2

        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        pool.schedule_operator_partition(("users", 1), op)
        pool.schedule_operator_partition(("users", 2), op)
        pool.schedule_operator_partition(("users", 3), op)

        # With 2 workers and 4 partitions, each should get exactly 2
        w1 = pool.peek(1)
        w2 = pool.peek(2)
        assert len(w1.assigned_operators) == 2
        assert len(w2.assigned_operators) == 2

    def test_schedule_records_operator_partition_to_worker_mapping(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        assert ("users", 0) in pool.operator_partition_to_worker

    def test_remove_operator_partition_updates_worker(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        pool.remove_operator_partition(("users", 0))
        w = pool.peek(1)
        assert ("users", 0) not in w.assigned_operators
        assert ("users", 0) not in pool.operator_partition_to_worker

    def test_update_operator_replaces_operator_in_place(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        op_old = _mock_operator()
        op_new = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op_old)
        pool.update_operator(("users", 0), op_new)
        w = pool.peek(1)
        assert w.assigned_operators[("users", 0)] is op_new


# ---------------------------------------------------------------------------
# WorkerPool — query methods
# ---------------------------------------------------------------------------


class TestWorkerPoolQueries:
    def test_number_of_workers_counts_including_tombstones(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        pool.register_worker("127.0.0.1", 5001, 6001)
        pool.remove_worker(1)
        # raw queue still has the entry; number_of_workers counts the raw list
        assert pool.number_of_workers() == 2

    def test_get_standby_workers(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)  # no operators → standby
        pool.register_worker("127.0.0.1", 5001, 6001)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)  # w1 gets it
        standby = pool.get_standby_workers()
        assert all(not w.participating for w in standby)

    def test_get_participating_workers(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        pool.register_worker("127.0.0.1", 5001, 6001)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        participating = pool.get_participating_workers()
        assert len(participating) == 1
        assert participating[0].participating

    def test_get_workers_returns_dict_of_addresses(self):
        pool = WorkerPool()
        pool.register_worker("10.0.0.1", 5000, 6000)
        pool.register_worker("10.0.0.2", 5001, 6001)
        workers = pool.get_workers()
        assert 1 in workers
        assert 2 in workers
        assert workers[1] == ("10.0.0.1", 5000, 6000)
        assert workers[2] == ("10.0.0.2", 5001, 6001)

    def test_get_workers_excludes_tombstones(self):
        pool = WorkerPool()
        pool.register_worker("10.0.0.1", 5000, 6000)
        pool.register_worker("10.0.0.2", 5001, 6001)
        pool.remove_worker(1)
        workers = pool.get_workers()
        assert 1 not in workers
        assert 2 in workers

    def test_get_worker_assignments(self):
        pool = WorkerPool()
        pool.register_worker("10.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        assignments = pool.get_worker_assignments()
        key = ("10.0.0.1", 5000, 6000)
        assert key in assignments
        assert ("users", 0) in assignments[key]

    def test_get_operator_partition_locations(self):
        pool = WorkerPool()
        pool.register_worker("10.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        locations = pool.get_operator_partition_locations()
        assert "users" in locations
        assert 0 in locations["users"]
        assert locations["users"][0] == ("10.0.0.1", 5000, 6000)


# ---------------------------------------------------------------------------
# WorkerPool — heartbeat detection
# ---------------------------------------------------------------------------


class TestWorkerPoolHeartbeats:
    def test_healthy_worker_not_in_failed_set(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        now = time.time()
        pool.register_worker_heartbeat(1, now)
        failed, _ = _run_sync_in_loop(pool.check_heartbeats, now + 1.0)  # 1 second later → 1000ms < 5000ms
        assert len(failed) == 0

    def test_stale_worker_detected_as_failed(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)  # make it participating
        now = time.time()
        pool.register_worker_heartbeat(1, now)
        failed, _ = _run_sync_in_loop(pool.check_heartbeats, now + 10.0)  # 10 seconds → 10000ms > 5000ms
        assert any(w.worker_id == 1 for w in failed)

    def test_stale_non_participating_worker_not_in_failed_set(self):
        """A worker with no operators that misses heartbeats is removed but not in 'failed'."""
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)  # no operators assigned
        now = time.time()
        pool.register_worker_heartbeat(1, now)
        failed, _ = _run_sync_in_loop(pool.check_heartbeats, now + 10.0)
        assert len(failed) == 0  # not participating, so doesn't trigger recovery

    def test_heartbeats_per_worker_populated(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        now = time.time()
        pool.register_worker_heartbeat(1, now)
        _, heartbeats = _run_sync_in_loop(pool.check_heartbeats, now + 1.0)
        assert 1 in heartbeats

    def test_register_heartbeat_unknown_worker_does_not_raise(self):
        """Should log a warning but not crash."""
        pool = WorkerPool()
        _run_sync_in_loop(pool.register_worker_heartbeat, 999, time.time())  # no KeyError

    def test_dead_worker_id_appended_on_failure(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        now = time.time()
        pool.register_worker_heartbeat(1, now)
        _run_sync_in_loop(pool.check_heartbeats, now + 10.0)
        assert 1 in pool.dead_worker_ids

    def test_orphaned_operators_collected_on_failure(self):
        pool = WorkerPool()
        pool.register_worker("127.0.0.1", 5000, 6000)
        op = _mock_operator()
        pool.schedule_operator_partition(("users", 0), op)
        now = time.time()
        pool.register_worker_heartbeat(1, now)
        _run_sync_in_loop(pool.check_heartbeats, now + 10.0)
        assert ("users", 0) in pool.orphaned_operator_assignments

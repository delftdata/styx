"""Unit tests for worker/sequencer/sequencer.py"""

import asyncio

from styx.common.run_func_payload import RunFuncPayload

from worker.sequencer.sequencer import Sequencer


def _run_sync_in_loop(fn, *args):
    """Execute a sync function inside a running asyncio event loop."""

    async def _inner():
        return fn(*args)

    return asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _payload(request_id: bytes = b"r1") -> RunFuncPayload:
    return RunFuncPayload(
        request_id=request_id,
        key="key",
        operator_name="op",
        partition=0,
        function_name="fn",
        params=(),
    )


def _seq(peer_ids: list[int], worker_id: int, **kwargs) -> Sequencer:
    s = Sequencer(**kwargs)
    s.set_sequencer_id(peer_ids, worker_id=worker_id)
    return s


# ---------------------------------------------------------------------------
# set_sequencer_id
# ---------------------------------------------------------------------------


class TestSetSequencerId:
    def test_single_worker_gets_id_one(self):
        s = _seq([], worker_id=1)
        assert s.sequencer_id == 1
        assert s.n_workers == 1

    def test_three_consecutive_workers_map_to_one_two_three(self):
        s1 = _seq([2, 3], worker_id=1)
        s2 = _seq([1, 3], worker_id=2)
        s3 = _seq([1, 2], worker_id=3)
        assert s1.sequencer_id == 1
        assert s2.sequencer_id == 2
        assert s3.sequencer_id == 3

    def test_non_consecutive_ids_get_dense_assignments(self):
        # Workers [3, 5, 7]: available IDs are {1,2,3}
        # Worker 3 is in {1,2,3} → gets 3; worker 5 is not → gets min({1,2})=1; worker 7 → 2
        s3 = _seq([5, 7], worker_id=3)
        s5 = _seq([3, 7], worker_id=5)
        s7 = _seq([3, 5], worker_id=7)
        assert s3.sequencer_id == 3
        assert s5.sequencer_id == 1
        assert s7.sequencer_id == 2

    def test_assignments_cover_range_one_to_n(self):
        ids = [10, 20, 30]
        seq_ids = {_seq([x for x in ids if x != w], worker_id=w).sequencer_id for w in ids}
        assert seq_ids == {1, 2, 3}

    def test_n_workers_matches_cluster_size(self):
        s = _seq([2, 3, 4], worker_id=1)
        assert s.n_workers == 4

    def test_after_crash_remaining_workers_remap(self):
        # Workers [1, 3] after worker 2 crashed: 1 → seq_id 1, 3 → seq_id 2
        s1 = _seq([3], worker_id=1)
        s3 = _seq([1], worker_id=3)
        assert s1.sequencer_id == 1
        assert s3.sequencer_id == 2

    def test_determinism_same_inputs_same_output(self):
        s_a = _seq([2, 3], worker_id=1)
        s_b = _seq([2, 3], worker_id=1)
        assert s_a.sequencer_id == s_b.sequencer_id


# ---------------------------------------------------------------------------
# get_t_id
# ---------------------------------------------------------------------------


class TestGetTId:
    def test_first_t_id_equals_sequencer_id(self):
        s = _seq([2], worker_id=1)  # seq_id=1, n_workers=2
        assert s.get_t_id() == 1  # 1 + 0 * 2

    def test_second_t_id_advances_by_n_workers(self):
        s = _seq([2], worker_id=1)
        s.get_t_id()
        assert s.get_t_id() == 3  # 1 + 1 * 2

    def test_t_counter_increments_after_each_call(self):
        s = _seq([], worker_id=1)
        s.get_t_id()
        assert s.t_counter == 1

    def test_two_workers_produce_disjoint_t_ids(self):
        s1 = _seq([2], worker_id=1)
        s2 = _seq([1], worker_id=2)
        ids1 = {s1.get_t_id() for _ in range(5)}
        ids2 = {s2.get_t_id() for _ in range(5)}
        assert ids1.isdisjoint(ids2)

    def test_initial_t_counter_used_in_formula(self):
        # t_counter starts at 5: first t_id = seq_id + 5 * n_workers
        s = _seq([], worker_id=1, t_counter=5)
        assert s.get_t_id() == 1 + 5 * 1


# ---------------------------------------------------------------------------
# sequence
# ---------------------------------------------------------------------------


class TestSequence:
    def test_new_request_appended_to_log(self):
        s = _seq([], worker_id=1)
        _run_sync_in_loop(s.sequence, _payload(b"req"))
        assert len(s.distributed_log) == 1

    def test_new_request_assigned_fresh_t_id(self):
        s = _seq([], worker_id=1)
        _run_sync_in_loop(s.sequence, _payload(b"req"))
        assert s.distributed_log[0].t_id == 1

    def test_duplicate_request_id_reuses_t_id(self):
        s = _seq([], worker_id=1)
        s.request_id_to_t_id_map[b"req"] = 42
        _run_sync_in_loop(s.sequence, _payload(b"req"))
        assert s.distributed_log[0].t_id == 42

    def test_wal_collision_skips_to_next_t_id(self):
        s = _seq([], worker_id=1)
        # First t_id would be 1; mark it as used in WAL
        s.t_ids_in_wal.add(1)
        _run_sync_in_loop(s.sequence, _payload(b"req"))
        assert s.distributed_log[0].t_id == 2  # skipped 1, used 2

    def test_multiple_requests_get_distinct_t_ids(self):
        s = _seq([], worker_id=1)
        for i in range(4):
            _run_sync_in_loop(s.sequence, _payload(f"r{i}".encode()))
        t_ids = [item.t_id for item in s.distributed_log]
        assert len(set(t_ids)) == 4


# ---------------------------------------------------------------------------
# get_epoch
# ---------------------------------------------------------------------------


class TestGetEpoch:
    def test_returns_all_when_max_size_none(self):
        s = _seq([], worker_id=1, max_size=None)
        for i in range(5):
            _run_sync_in_loop(s.sequence, _payload(f"r{i}".encode()))
        epoch = s.get_epoch()
        assert len(epoch) == 5
        assert len(s.distributed_log) == 0

    def test_bounded_epoch_returns_at_most_max_size(self):
        s = _seq([], worker_id=1, max_size=3)
        for i in range(5):
            _run_sync_in_loop(s.sequence, _payload(f"r{i}".encode()))
        epoch = s.get_epoch()
        assert len(epoch) == 3
        assert len(s.distributed_log) == 2

    def test_empty_log_returns_empty_list(self):
        s = _seq([], worker_id=1)
        assert s.get_epoch() == []

    def test_get_epoch_populates_current_epoch(self):
        s = _seq([], worker_id=1)
        _run_sync_in_loop(s.sequence, _payload(b"r"))
        s.get_epoch()
        assert len(s.current_epoch) == 1


# ---------------------------------------------------------------------------
# increment_epoch
# ---------------------------------------------------------------------------


class TestIncrementEpoch:
    def test_t_counter_updated_to_max(self):
        s = _seq([], worker_id=1)
        s.increment_epoch(max_t_counter=99, t_ids_to_reschedule=set())
        assert s.t_counter == 99

    def test_epoch_counter_incremented(self):
        s = _seq([], worker_id=1)
        s.increment_epoch(max_t_counter=0, t_ids_to_reschedule=set())
        assert s.epoch_counter == 1

    def test_current_epoch_cleared(self):
        s = _seq([], worker_id=1)
        _run_sync_in_loop(s.sequence, _payload(b"r"))
        s.get_epoch()
        s.increment_epoch(max_t_counter=0, t_ids_to_reschedule=set())
        assert s.current_epoch == []

    def test_aborted_items_prepended_sorted_ascending(self):
        s = _seq([2], worker_id=1)  # seq_id=1, n_workers=2 → t_ids 1,3,5
        for i in range(3):
            _run_sync_in_loop(s.sequence, _payload(f"r{i}".encode()))
        s.get_epoch()
        aborted = {item.t_id for item in s.current_epoch}  # {1, 3, 5}
        s.increment_epoch(max_t_counter=10, t_ids_to_reschedule=aborted)
        front = s.distributed_log[:3]
        ids = [item.t_id for item in front]
        assert ids == sorted(ids)

    def test_non_aborted_items_not_prepended(self):
        s = _seq([], worker_id=1)
        _run_sync_in_loop(s.sequence, _payload(b"r1"))
        s.get_epoch()  # current_epoch has 1 item with t_id=1
        # Also queue a new item
        _run_sync_in_loop(s.sequence, _payload(b"r2"))
        s.increment_epoch(max_t_counter=0, t_ids_to_reschedule=set())
        assert len(s.distributed_log) == 1  # only the queued item, no prepend


# ---------------------------------------------------------------------------
# get_aborted_sequence
# ---------------------------------------------------------------------------


class TestGetAbortedSequence:
    def test_returns_sorted_items_matching_reschedule_set(self):
        s = _seq([2], worker_id=1)  # n_workers=2 → t_ids 1,3,5
        for i in range(3):
            _run_sync_in_loop(s.sequence, _payload(f"r{i}".encode()))
        s.get_epoch()
        all_t_ids = {item.t_id for item in s.current_epoch}
        aborted = s.get_aborted_sequence(all_t_ids)
        ids = [item.t_id for item in aborted]
        assert ids == sorted(ids)
        assert set(ids) == all_t_ids

    def test_empty_reschedule_returns_empty(self):
        s = _seq([], worker_id=1)
        _run_sync_in_loop(s.sequence, _payload(b"r"))
        s.get_epoch()
        assert s.get_aborted_sequence(set()) == []

    def test_only_matching_t_ids_returned(self):
        s = _seq([], worker_id=1)
        for i in range(3):
            _run_sync_in_loop(s.sequence, _payload(f"r{i}".encode()))
        epoch = s.get_epoch()
        first_t_id = epoch[0].t_id
        aborted = s.get_aborted_sequence({first_t_id})
        assert len(aborted) == 1
        assert aborted[0].t_id == first_t_id


# ---------------------------------------------------------------------------
# set_wal_values_after_recovery
# ---------------------------------------------------------------------------


class TestSetWalValuesAfterRecovery:
    def test_populates_request_map_and_t_ids_set(self):
        s = Sequencer()
        mapping = {b"req1": 10, b"req2": 20}
        s.set_wal_values_after_recovery(mapping)
        assert s.request_id_to_t_id_map == mapping
        assert s.t_ids_in_wal == {10, 20}

    def test_none_input_is_no_op(self):
        s = Sequencer()
        s.set_wal_values_after_recovery(None)
        assert s.request_id_to_t_id_map == {}
        assert s.t_ids_in_wal == set()

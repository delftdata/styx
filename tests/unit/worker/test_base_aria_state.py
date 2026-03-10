"""Unit tests for worker/operator_state/aria/base_aria_state.py

BaseAriaState is abstract; tests use InMemoryOperatorState as the concrete
subclass so the shared logic (conflict detection, rw-set tracking, etc.)
can be exercised without mocking anything.
"""

from worker.operator_state.aria.base_aria_state import BaseAriaState
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

OP = "op"
PART = 0
OP_PART = (OP, PART)


def _state(*extra_partitions) -> InMemoryOperatorState:
    parts = {OP_PART, *extra_partitions}
    return InMemoryOperatorState(parts)


def _put(s, key, value, t_id, op=OP, part=PART):
    s.put(key, value, t_id, op, part)


def _read(s, key, t_id, op=OP, part=PART):
    s.deal_with_reads(key, t_id, (op, part))


# ---------------------------------------------------------------------------
# put — write_sets and writes tracking
# ---------------------------------------------------------------------------


class TestPut:
    def test_write_set_entry_created(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        assert s.write_sets[OP_PART][1]["k"] == "v"

    def test_writes_reservation_list_populated(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        assert 1 in s.writes[OP_PART]["k"]

    def test_multiple_t_ids_write_same_key(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=3)
        assert s.writes[OP_PART]["k"] == [1, 3]

    def test_same_t_id_writes_multiple_keys(self):
        s = _state()
        _put(s, "k1", "v1", t_id=1)
        _put(s, "k2", "v2", t_id=1)
        assert set(s.write_sets[OP_PART][1].keys()) == {"k1", "k2"}

    def test_write_overwrites_previous_value_for_same_t_id(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=1)
        assert s.write_sets[OP_PART][1]["k"] == "v2"


# ---------------------------------------------------------------------------
# put_immediate — fallback commit buffer
# ---------------------------------------------------------------------------


class TestPutImmediate:
    def test_buffer_entry_created(self):
        s = _state()
        s.put_immediate("k", "v", t_id=1, operator_name=OP, partition=PART)
        assert s.fallback_commit_buffer[1][OP_PART]["k"] == "v"

    def test_multiple_keys_same_t_id(self):
        s = _state()
        s.put_immediate("k1", "v1", 1, OP, PART)
        s.put_immediate("k2", "v2", 1, OP, PART)
        assert set(s.fallback_commit_buffer[1][OP_PART].keys()) == {"k1", "k2"}

    def test_multiple_t_ids_stored_independently(self):
        s = _state()
        s.put_immediate("k", "v1", 1, OP, PART)
        s.put_immediate("k", "v2", 3, OP, PART)
        assert s.fallback_commit_buffer[1][OP_PART]["k"] == "v1"
        assert s.fallback_commit_buffer[3][OP_PART]["k"] == "v2"


# ---------------------------------------------------------------------------
# deal_with_reads — reads and read_sets tracking
# ---------------------------------------------------------------------------


class TestDealWithReads:
    def test_reads_reservation_populated(self):
        s = _state()
        _read(s, "k", t_id=1)
        assert 1 in s.reads[OP_PART]["k"]

    def test_read_set_for_t_id_populated(self):
        s = _state()
        _read(s, "k", t_id=1)
        assert "k" in s.read_sets[OP_PART][1]

    def test_multiple_t_ids_read_same_key(self):
        s = _state()
        _read(s, "k", t_id=1)
        _read(s, "k", t_id=3)
        assert s.reads[OP_PART]["k"] == [1, 3]

    def test_same_t_id_reads_multiple_keys(self):
        s = _state()
        _read(s, "k1", t_id=1)
        _read(s, "k2", t_id=1)
        assert {"k1", "k2"}.issubset(s.read_sets[OP_PART][1])


# ---------------------------------------------------------------------------
# Static helpers: has_conflicts, min_rw_reservations
# ---------------------------------------------------------------------------


class TestHasConflicts:
    def test_conflict_when_reservation_has_lower_t_id(self):
        assert BaseAriaState.has_conflicts(5, {"k"}, {"k": 3}) is True

    def test_no_conflict_when_reservation_equals_t_id(self):
        assert BaseAriaState.has_conflicts(5, {"k"}, {"k": 5}) is False

    def test_no_conflict_when_reservation_higher(self):
        assert BaseAriaState.has_conflicts(5, {"k"}, {"k": 7}) is False

    def test_no_conflict_when_key_absent(self):
        assert BaseAriaState.has_conflicts(5, {"k"}, {"other": 1}) is False

    def test_no_conflict_with_empty_keys(self):
        assert BaseAriaState.has_conflicts(5, set(), {"k": 1}) is False


class TestMinRwReservations:
    def test_returns_minimum_t_id_per_key(self):
        res = BaseAriaState.min_rw_reservations({OP_PART: {"k": [3, 1, 2]}})
        assert res[OP_PART]["k"] == 1

    def test_empty_list_skipped(self):
        res = BaseAriaState.min_rw_reservations({OP_PART: {"k": [], "j": [5]}})
        assert "k" not in res[OP_PART]
        assert res[OP_PART]["j"] == 5

    def test_single_t_id_returned_as_is(self):
        res = BaseAriaState.min_rw_reservations({OP_PART: {"k": [7]}})
        assert res[OP_PART]["k"] == 7

    def test_multiple_partitions(self):
        res = BaseAriaState.min_rw_reservations(
            {
                ("op", 0): {"k": [2, 4]},
                ("op", 1): {"k": [10, 1]},
            }
        )
        assert res[("op", 0)]["k"] == 2
        assert res[("op", 1)]["k"] == 1


# ---------------------------------------------------------------------------
# set_global_read_write_sets
# ---------------------------------------------------------------------------


class TestSetGlobalReadWriteSets:
    def test_sets_all_three_global_structures(self):
        s = _state()
        gr = {OP_PART: {"k": [1]}}
        gws = {OP_PART: {1: {"k": "v"}}}
        grs = {OP_PART: {1: {"k"}}}
        s.set_global_read_write_sets(gr, gws, grs)
        assert s.global_reads is gr
        assert s.global_write_sets is gws
        assert s.global_read_sets is grs


# ---------------------------------------------------------------------------
# check_conflicts DEFAULT_SERIALIZABLE
# ---------------------------------------------------------------------------


class TestCheckConflicts:
    def test_no_writes_no_conflict(self):
        s = _state()
        assert s.check_conflicts() == set()

    def test_single_writer_no_conflict(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        assert s.check_conflicts() == set()

    def test_raw_conflict_higher_t_id_aborted(self):
        # t1 writes "k", t5 reads "k" → t5 has RAW with t1
        s = _state()
        _put(s, "k", "v", t_id=1)
        _read(s, "k", t_id=5)
        aborted = s.check_conflicts()
        assert 5 in aborted
        assert 1 not in aborted

    def test_waw_conflict_higher_t_id_aborted(self):
        # t1 writes "k", t5 writes "k" → t5 has WAW with t1
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=5)
        aborted = s.check_conflicts()
        assert 5 in aborted
        assert 1 not in aborted

    def test_disjoint_keys_no_conflict(self):
        s = _state()
        _put(s, "k1", "v", t_id=1)
        _read(s, "k2", t_id=5)
        assert s.check_conflicts() == set()

    def test_multiple_conflicts_all_detected(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        _read(s, "k", t_id=3)
        _read(s, "k", t_id=5)
        aborted = s.check_conflicts()
        assert 3 in aborted
        assert 5 in aborted


# ---------------------------------------------------------------------------
# check_conflicts_snapshot_isolation
# ---------------------------------------------------------------------------


class TestCheckConflictsSnapshotIsolation:
    def test_waw_conflict_aborts_higher_t_id(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=5)
        aborted = s.check_conflicts_snapshot_isolation()
        assert 5 in aborted
        assert 1 not in aborted

    def test_raw_not_aborted(self):
        # Snapshot isolation only catches WAW
        s = _state()
        _put(s, "k", "v", t_id=1)
        _read(s, "k", t_id=5)
        aborted = s.check_conflicts_snapshot_isolation()
        assert 5 not in aborted

    def test_disjoint_writes_no_conflict(self):
        s = _state()
        _put(s, "k1", "v", t_id=1)
        _put(s, "k2", "v", t_id=5)
        assert s.check_conflicts_snapshot_isolation() == set()

    def test_single_writer_no_conflict(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        assert s.check_conflicts_snapshot_isolation() == set()


# ---------------------------------------------------------------------------
# check_conflicts_deterministic_reordering
# ---------------------------------------------------------------------------


class TestCheckConflictsDeterministicReordering:
    def _setup_global(self, s, global_reads, global_write_set, global_read_set):
        s.set_global_read_write_sets(global_reads, global_write_set, global_read_set)

    def test_waw_aborts_higher_t_id(self):
        # t1 and t5 both write "k" locally → WAW → t5 aborted
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=5)
        gws = {OP_PART: {1: {"k": "v1"}, 5: {"k": "v2"}}}
        self._setup_global(s, {OP_PART: {}}, gws, {OP_PART: {}})
        aborted = s.check_conflicts_deterministic_reordering()
        assert 5 in aborted
        assert 1 not in aborted

    def test_war_and_raw_cycle_aborts_higher_t_id(self):
        # t1 reads "a", writes "b"; t5 reads "b", writes "a" → WAR+RAW cycle → t5 aborted
        s = _state()
        _put(s, "b", "vb", t_id=1)
        _read(s, "a", t_id=1)
        _put(s, "a", "va", t_id=5)
        _read(s, "b", t_id=5)
        gws = {OP_PART: {1: {"b": "vb"}, 5: {"a": "va"}}}
        grs = {OP_PART: {1: {"a"}, 5: {"b"}}}
        gr = {OP_PART: {"a": [1], "b": [5]}}
        self._setup_global(s, gr, gws, grs)
        aborted = s.check_conflicts_deterministic_reordering()
        assert 5 in aborted
        assert 1 not in aborted

    def test_only_war_not_aborted(self):
        # t1 reads "a"; t5 writes "a" (WAR only, no RAW for t5) → t5 not aborted
        s = _state()
        _read(s, "a", t_id=1)
        _put(s, "a", "v", t_id=5)
        gws = {OP_PART: {5: {"a": "v"}}}
        grs = {OP_PART: {1: {"a"}, 5: set()}}
        gr = {OP_PART: {"a": [1]}}
        self._setup_global(s, gr, gws, grs)
        aborted = s.check_conflicts_deterministic_reordering()
        assert 5 not in aborted

    def test_no_conflicts_empty_state(self):
        s = _state()
        self._setup_global(s, {OP_PART: {}}, {OP_PART: {}}, {OP_PART: {}})
        assert s.check_conflicts_deterministic_reordering() == set()


# ---------------------------------------------------------------------------
# get_dep_transactions
# ---------------------------------------------------------------------------


class TestGetDepTransactions:
    def test_no_shared_keys_no_dependencies(self):
        s = _state()
        _put(s, "k1", "v", t_id=1)
        _put(s, "k2", "v", t_id=3)
        deps, _ = s.get_dep_transactions({1, 3})
        assert not deps.get(1)
        assert not deps.get(3)

    def test_shared_write_key_creates_dependency(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=3)
        deps, _ = s.get_dep_transactions({1, 3})
        assert 1 in deps[3]

    def test_smaller_does_not_depend_on_larger(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=3)
        deps, _ = s.get_dep_transactions({1, 3})
        assert 3 not in deps.get(1, set())

    def test_locks_created_for_all_t_ids(self):
        s = _state()
        _, locks = s.get_dep_transactions({1, 3, 5})
        assert set(locks.keys()) == {1, 3, 5}

    def test_transitive_chain_all_dependencies_present(self):
        # 1 → 3 → 5 all access "k"
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=3)
        _put(s, "k", "v3", t_id=5)
        deps, _ = s.get_dep_transactions({1, 3, 5})
        assert 1 in deps[3]
        assert 1 in deps[5]
        assert 3 in deps[5]

    def test_read_write_shared_key_also_creates_dependency(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        _read(s, "k", t_id=3)
        deps, _ = s.get_dep_transactions({1, 3})
        assert 1 in deps[3]


# ---------------------------------------------------------------------------
# remove_aborted_from_rw_sets
# ---------------------------------------------------------------------------


class TestRemoveAbortedFromRwSets:
    def test_aborted_t_id_removed_from_write_set(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        _put(s, "k", "v", t_id=3)
        s.remove_aborted_from_rw_sets({1})
        assert 1 not in s.write_sets[OP_PART]
        assert 3 in s.write_sets[OP_PART]

    def test_aborted_t_id_removed_from_read_set(self):
        s = _state()
        _read(s, "k", t_id=1)
        _read(s, "k", t_id=3)
        s.remove_aborted_from_rw_sets({1})
        assert 1 not in s.read_sets[OP_PART]
        assert 3 in s.read_sets[OP_PART]

    def test_aborted_t_id_removed_from_writes_reservation(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=3)
        s.remove_aborted_from_rw_sets({1})
        assert 1 not in s.writes[OP_PART].get("k", [])
        assert 3 in s.writes[OP_PART].get("k", [])

    def test_empty_abort_set_is_no_op(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        s.remove_aborted_from_rw_sets(set())
        assert 1 in s.write_sets[OP_PART]


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_all_local_rw_sets_cleared(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        _read(s, "k", t_id=1)
        s.cleanup()
        assert s.write_sets[OP_PART] == {}
        assert s.writes[OP_PART] == {}
        assert s.reads[OP_PART] == {}
        assert s.read_sets[OP_PART] == {}

    def test_fallback_commit_buffer_cleared(self):
        s = _state()
        s.put_immediate("k", "v", t_id=1, operator_name=OP, partition=PART)
        s.cleanup()
        assert len(s.fallback_commit_buffer) == 0

    def test_global_sets_cleared(self):
        s = _state()
        s.global_write_sets[OP_PART] = {1: {"k": "v"}}
        s.global_read_sets[OP_PART] = {1: {"k"}}
        s.global_reads[OP_PART] = {"k": [1]}
        s.cleanup()
        assert s.global_write_sets[OP_PART] == {}
        assert s.global_read_sets[OP_PART] == {}
        assert s.global_reads[OP_PART] == {}

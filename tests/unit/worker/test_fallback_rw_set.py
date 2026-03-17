"""Tests for fallback rw-set change detection (Aria paper §4.2).

When transactions are re-executed in the fallback phase, if their read/write
set changes compared to the original optimistic execution, the dependency
graph is no longer valid and the transaction must be rescheduled.
"""

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
    """Record an optimistic-phase write."""
    s.put(key, value, t_id, op, part)


def _read(s, key, t_id, op=OP, part=PART):
    """Record an optimistic-phase read."""
    s.deal_with_reads(key, t_id, (op, part))


def _fallback_read(s, key, t_id, op=OP, part=PART):
    """Simulate a fallback-phase read (get_immediate)."""
    s.get_immediate(key, t_id, op, part)


def _fallback_write(s, key, value, t_id, op=OP, part=PART):
    """Simulate a fallback-phase write (put_immediate)."""
    s.put_immediate(key, value, t_id, op, part)


# ---------------------------------------------------------------------------
# has_fallback_rw_set_changed — read set changes
# ---------------------------------------------------------------------------


class TestFallbackReadSetChange:
    def test_same_reads_no_change(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        # Original read
        _read(s, "k1", t_id=1)
        # Fallback read of the same key
        _fallback_read(s, "k1", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is False

    def test_extra_fallback_read_detected(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        s.data[OP_PART]["k2"] = "v2"
        # Original read: only k1
        _read(s, "k1", t_id=1)
        # Fallback reads: k1 and k2
        _fallback_read(s, "k1", t_id=1)
        _fallback_read(s, "k2", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_missing_fallback_read_detected(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        s.data[OP_PART]["k2"] = "v2"
        # Original read: k1 and k2
        _read(s, "k1", t_id=1)
        _read(s, "k2", t_id=1)
        # Fallback read: only k1
        _fallback_read(s, "k1", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_different_key_in_fallback_read(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        s.data[OP_PART]["k2"] = "v2"
        # Original read: k1
        _read(s, "k1", t_id=1)
        # Fallback read: k2 (different key)
        _fallback_read(s, "k2", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_no_original_reads_but_fallback_reads(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        # No original reads for t_id=1
        # Fallback reads k1
        _fallback_read(s, "k1", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_original_reads_but_no_fallback_reads(self):
        s = _state()
        # Original read: k1
        _read(s, "k1", t_id=1)
        # No fallback reads
        assert s.has_fallback_rw_set_changed(1) is True


# ---------------------------------------------------------------------------
# has_fallback_rw_set_changed — write set changes
# ---------------------------------------------------------------------------


class TestFallbackWriteSetChange:
    def test_same_writes_no_change(self):
        s = _state()
        # Original write
        _put(s, "k1", "v1", t_id=1)
        # Fallback write of the same key
        _fallback_write(s, "k1", "v1_new", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is False

    def test_extra_fallback_write_detected(self):
        s = _state()
        # Original write: k1
        _put(s, "k1", "v1", t_id=1)
        # Fallback writes: k1 and k2
        _fallback_write(s, "k1", "v1", t_id=1)
        _fallback_write(s, "k2", "v2", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_missing_fallback_write_detected(self):
        s = _state()
        # Original writes: k1 and k2
        _put(s, "k1", "v1", t_id=1)
        _put(s, "k2", "v2", t_id=1)
        # Fallback write: only k1
        _fallback_write(s, "k1", "v1", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_different_value_same_keys_no_change(self):
        """Only key sets matter, not values."""
        s = _state()
        _put(s, "k1", "v1", t_id=1)
        _fallback_write(s, "k1", "v1_different", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is False


# ---------------------------------------------------------------------------
# has_fallback_rw_set_changed — combined read + write changes
# ---------------------------------------------------------------------------


class TestFallbackCombinedRwSetChange:
    def test_reads_same_writes_changed(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        _read(s, "k1", t_id=1)
        _put(s, "k2", "v2", t_id=1)
        # Fallback: same read, but extra write
        _fallback_read(s, "k1", t_id=1)
        _fallback_write(s, "k2", "v2", t_id=1)
        _fallback_write(s, "k3", "v3", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_writes_same_reads_changed(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        s.data[OP_PART]["k2"] = "v2"
        _read(s, "k1", t_id=1)
        _put(s, "k2", "v2", t_id=1)
        # Fallback: different read, same write
        _fallback_read(s, "k2", t_id=1)
        _fallback_write(s, "k2", "v2", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_both_same_no_change(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        _read(s, "k1", t_id=1)
        _put(s, "k2", "v2", t_id=1)
        _fallback_read(s, "k1", t_id=1)
        _fallback_write(s, "k2", "v2", t_id=1)
        assert s.has_fallback_rw_set_changed(1) is False


# ---------------------------------------------------------------------------
# has_fallback_rw_set_changed — no original activity
# ---------------------------------------------------------------------------


class TestFallbackNoOriginalActivity:
    def test_no_original_no_fallback_no_change(self):
        s = _state()
        assert s.has_fallback_rw_set_changed(99) is False


# ---------------------------------------------------------------------------
# Multi-partition scenarios
# ---------------------------------------------------------------------------


class TestFallbackMultiPartition:
    def test_change_on_second_partition_detected(self):
        op_part_2 = (OP, 1)
        s = _state(op_part_2)
        s.data[OP_PART]["k1"] = "v1"
        s.data[op_part_2]["k2"] = "v2"
        # Original reads on both partitions
        _read(s, "k1", t_id=1, part=0)
        _read(s, "k2", t_id=1, part=1)
        # Fallback: same read on part 0, extra read on part 1
        _fallback_read(s, "k1", t_id=1, part=0)
        _fallback_read(s, "k2", t_id=1, part=1)
        s.data[op_part_2]["k3"] = "v3"
        _fallback_read(s, "k3", t_id=1, part=1)
        assert s.has_fallback_rw_set_changed(1) is True

    def test_same_on_both_partitions_no_change(self):
        op_part_2 = (OP, 1)
        s = _state(op_part_2)
        s.data[OP_PART]["k1"] = "v1"
        s.data[op_part_2]["k2"] = "v2"
        _read(s, "k1", t_id=1, part=0)
        _read(s, "k2", t_id=1, part=1)
        _fallback_read(s, "k1", t_id=1, part=0)
        _fallback_read(s, "k2", t_id=1, part=1)
        assert s.has_fallback_rw_set_changed(1) is False


# ---------------------------------------------------------------------------
# Cleanup clears fallback_read_sets
# ---------------------------------------------------------------------------


class TestFallbackReadSetsCleanup:
    def test_cleanup_clears_fallback_read_sets(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        _fallback_read(s, "k1", t_id=1)
        assert len(s.fallback_read_sets) > 0
        s.cleanup()
        assert len(s.fallback_read_sets) == 0

    def test_cleanup_clears_fallback_commit_buffer(self):
        s = _state()
        _fallback_write(s, "k1", "v1", t_id=1)
        assert len(s.fallback_commit_buffer) > 0
        s.cleanup()
        assert len(s.fallback_commit_buffer) == 0

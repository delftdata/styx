"""Unit tests for coordinator/snapshot_compactor.py

Focuses on get_snapshots_per_worker(), which is a pure function and
therefore straightforward to test without S3 mocking.
"""

from snapshot_compactor import get_snapshots_per_worker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seq(snap_id: int) -> str:
    return f"sequencer/{snap_id}.bin"


def _data(operator: str, partition: int, snap_id: int) -> str:
    return f"data/{operator}/{partition}/{snap_id}.bin"


def _migration(snap_id: int) -> str:
    return f"migration/{snap_id}.bin"


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------


class TestGetSnapshotsPerWorkerEmpty:
    def test_empty_list_returns_empty_results(self):
        partitions, sequencer = get_snapshots_per_worker([], max_snap_id=10)
        assert partitions == {}
        assert sequencer == []


# ---------------------------------------------------------------------------
# Sequencer snapshots
# ---------------------------------------------------------------------------


class TestSequencerSnapshots:
    def test_single_sequencer_snapshot_returned(self):
        files = [_seq(1)]
        _, sequencer = get_snapshots_per_worker(files, max_snap_id=5)
        assert len(sequencer) == 1
        assert sequencer[0] == (1, _seq(1))

    def test_sequencer_snapshot_filtered_above_max_snap_id(self):
        files = [_seq(3), _seq(7)]
        _, sequencer = get_snapshots_per_worker(files, max_snap_id=5)
        assert len(sequencer) == 1
        assert sequencer[0][0] == 3

    def test_sequencer_snapshot_at_exact_max_snap_id_included(self):
        files = [_seq(5)]
        _, sequencer = get_snapshots_per_worker(files, max_snap_id=5)
        assert len(sequencer) == 1

    def test_sequencer_snapshots_sorted_ascending(self):
        files = [_seq(3), _seq(1), _seq(2)]
        _, sequencer = get_snapshots_per_worker(files, max_snap_id=10)
        ids = [s[0] for s in sequencer]
        assert ids == sorted(ids)

    def test_multiple_sequencer_snapshots_all_within_max(self):
        files = [_seq(1), _seq(2), _seq(3)]
        _, sequencer = get_snapshots_per_worker(files, max_snap_id=10)
        assert len(sequencer) == 3


# ---------------------------------------------------------------------------
# Data snapshots
# ---------------------------------------------------------------------------


class TestDataSnapshots:
    def test_single_snapshot_per_partition_excluded(self):
        """A partition with only one snapshot doesn't need compaction → excluded."""
        files = [_data("users", 0, 1)]
        partitions, _ = get_snapshots_per_worker(files, max_snap_id=10)
        assert ("users", 0) not in partitions

    def test_two_snapshots_per_partition_included(self):
        files = [_data("users", 0, 1), _data("users", 0, 2)]
        partitions, _ = get_snapshots_per_worker(files, max_snap_id=10)
        assert ("users", 0) in partitions
        assert len(partitions[("users", 0)]) == 2

    def test_data_snapshot_filtered_above_max_snap_id(self):
        files = [_data("users", 0, 1), _data("users", 0, 5), _data("users", 0, 10)]
        partitions, _ = get_snapshots_per_worker(files, max_snap_id=5)
        entries = partitions.get(("users", 0), [])
        snap_ids = [e[0] for e in entries]
        assert all(sid <= 5 for sid in snap_ids)

    def test_data_snapshot_at_exact_max_snap_id_included(self):
        files = [_data("users", 0, 1), _data("users", 0, 5)]
        partitions, _ = get_snapshots_per_worker(files, max_snap_id=5)
        assert ("users", 0) in partitions

    def test_multiple_partitions_tracked_independently(self):
        files = [
            _data("users", 0, 1),
            _data("users", 0, 2),
            _data("orders", 0, 1),
            _data("orders", 0, 2),
        ]
        partitions, _ = get_snapshots_per_worker(files, max_snap_id=10)
        assert ("users", 0) in partitions
        assert ("orders", 0) in partitions

    def test_multiple_partitions_of_same_operator(self):
        files = [
            _data("users", 0, 1),
            _data("users", 0, 2),
            _data("users", 1, 1),
            _data("users", 1, 2),
        ]
        partitions, _ = get_snapshots_per_worker(files, max_snap_id=10)
        assert ("users", 0) in partitions
        assert ("users", 1) in partitions


# ---------------------------------------------------------------------------
# Migration snapshots (should be ignored)
# ---------------------------------------------------------------------------


class TestMigrationIgnored:
    def test_migration_paths_are_ignored(self):
        files = [_migration(1), _migration(2), _seq(1)]
        partitions, sequencer = get_snapshots_per_worker(files, max_snap_id=10)
        assert partitions == {}
        assert len(sequencer) == 1  # only the sequencer entry


# ---------------------------------------------------------------------------
# Mixed input
# ---------------------------------------------------------------------------


class TestMixedInput:
    def test_mixed_sequencer_and_data(self):
        files = [
            _seq(1),
            _seq(2),
            _data("users", 0, 1),
            _data("users", 0, 2),
            _migration(1),
        ]
        partitions, sequencer = get_snapshots_per_worker(files, max_snap_id=10)
        assert len(sequencer) == 2
        assert ("users", 0) in partitions

    def test_max_snap_id_zero_filters_all(self):
        files = [_seq(1), _data("users", 0, 1), _data("users", 0, 2)]
        partitions, sequencer = get_snapshots_per_worker(files, max_snap_id=0)
        assert partitions == {}
        assert sequencer == []

    def test_snapshot_id_zero_included_with_max_zero(self):
        files = [_seq(0), _data("users", 0, 0), _data("users", 0, 1)]
        partitions, sequencer = get_snapshots_per_worker(files, max_snap_id=0)
        # snap_id=0 ≤ max_snap_id=0 → included; snap_id=1 > 0 → excluded
        assert len(sequencer) == 1
        # only one data snapshot survives filtering → not enough for compaction
        assert ("users", 0) not in partitions

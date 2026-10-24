"""Unit tests for migration fault tolerance features.

Covers:
- delta_map writes during migration (set_data_from_migration, set_batch_data_from_migration)
- tombstone writes on source-side key removal (get_key_to_migrate, get_async_migrate_batch)
- tombstone handling during snapshot recovery (_load_operator_state)
- migration_reassign_delta_maps preserves existing deltas
"""

from unittest.mock import MagicMock, patch

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState

OP = "op"
PART_0 = 0
PART_1 = 1
OP_PART_0 = (OP, PART_0)
OP_PART_1 = (OP, PART_1)


def _state(*partitions):
    parts = set(partitions) if partitions else {OP_PART_0}
    return InMemoryOperatorState(parts)


# ---------------------------------------------------------------------------
# Step 1: set_data_from_migration writes to delta_map
# ---------------------------------------------------------------------------


class TestSetDataFromMigrationDeltaMap:
    def test_single_key_written_to_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.set_data_from_migration(OP_PART_1, "k1", "v1")
        assert s.delta_map[OP_PART_1]["k1"] == "v1"

    def test_none_data_not_written_to_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.set_data_from_migration(OP_PART_1, "k1", None)
        assert "k1" not in s.delta_map[OP_PART_1]

    def test_data_written_to_both_data_and_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.set_data_from_migration(OP_PART_1, "k1", {"field": 42})
        assert s.data[OP_PART_1]["k1"] == {"field": 42}
        assert s.delta_map[OP_PART_1]["k1"] == {"field": 42}


class TestSetBatchDataFromMigrationDeltaMap:
    def test_batch_written_to_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        batch = {"k1": "v1", "k2": "v2", "k3": "v3"}
        s.set_batch_data_from_migration(OP_PART_1, batch)
        assert s.delta_map[OP_PART_1] == batch

    def test_batch_merges_with_existing_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.delta_map[OP_PART_1]["existing"] = "val"
        s.set_batch_data_from_migration(OP_PART_1, {"k1": "v1"})
        assert s.delta_map[OP_PART_1] == {"existing": "val", "k1": "v1"}


class TestMigrateWithinSameWorkerDeltaMap:
    def test_migrate_within_same_worker_writes_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.data[OP_PART_0]["k1"] = "v1"
        s.migrate_within_the_same_worker(OP, PART_1, "k1", PART_0)
        # Destination gets the value in delta_map
        assert s.delta_map[OP_PART_1]["k1"] == "v1"
        # Source gets a tombstone in delta_map
        assert s.delta_map[OP_PART_0]["k1"] is None


# ---------------------------------------------------------------------------
# Step 2: Source-side key removal writes tombstones
# ---------------------------------------------------------------------------


class TestGetKeyToMigrateTombstone:
    def test_tombstone_written_to_delta_map(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.data[OP_PART_0]["k1"] = "v1"
        result = s.get_key_to_migrate(OP_PART_1, "k1", PART_0)
        assert result == "v1"
        assert s.delta_map[OP_PART_0]["k1"] is None

    def test_no_tombstone_when_key_already_gone(self):
        s = _state(OP_PART_0, OP_PART_1)
        # key not in data
        result = s.get_key_to_migrate(OP_PART_1, "k1", PART_0)
        assert result is None
        assert "k1" not in s.delta_map[OP_PART_0]


class TestGetAsyncMigrateBatchTombstone:
    def test_tombstones_written_for_migrated_keys(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.data[OP_PART_0] = {"k1": "v1", "k2": "v2", "k3": "v3"}
        s.keys_to_send = {OP_PART_0: {("k1", PART_1), ("k2", PART_1), ("k3", PART_1)}}
        batch = s.get_async_migrate_batch(batch_size=10)
        # All three keys should have tombstones in source delta_map
        for key in ["k1", "k2", "k3"]:
            assert s.delta_map[OP_PART_0][key] is None
        # The batch should contain the values for the destination
        assert len(batch[(OP, PART_1)]) == 3

    def test_no_tombstone_for_already_deleted_key(self):
        s = _state(OP_PART_0, OP_PART_1)
        s.data[OP_PART_0] = {}  # key not present
        s.keys_to_send = {OP_PART_0: {("k1", PART_1)}}
        s.get_async_migrate_batch(batch_size=10)
        # No tombstone because key was already gone
        assert "k1" not in s.delta_map[OP_PART_0]


# ---------------------------------------------------------------------------
# Step 3: Tombstone handling in snapshot recovery
# ---------------------------------------------------------------------------


class TestLoadOperatorStateTombstones:
    def test_tombstone_removes_key_from_accumulated_state(self):
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        snap = AsyncSnapshotsS3.__new__(AsyncSnapshotsS3)
        snap.worker_id = 0

        # Simulate S3 with two deltas: first adds key, second tombstones it
        delta_1 = {"k1": "v1", "k2": "v2"}
        delta_2 = {"k1": None}  # tombstone

        mock_s3 = MagicMock()

        with (
            patch.object(snap, "_iter_snapshot_files", return_value=[(1, "d/op/0/1.bin"), (2, "d/op/0/2.bin")]),
            patch.object(snap, "_get_zstd_msgpack", side_effect=[delta_1, delta_2]),
        ):
            data = snap._load_operator_state(mock_s3, 2, [("op", 0)])

        assert ("op", 0) in data
        assert "k1" not in data[("op", 0)]  # tombstoned
        assert data[("op", 0)]["k2"] == "v2"  # preserved

    def test_tombstone_in_first_delta_filtered_out(self):
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        snap = AsyncSnapshotsS3.__new__(AsyncSnapshotsS3)
        snap.worker_id = 0

        delta_1 = {"k1": None, "k2": "v2"}  # tombstone in first delta

        mock_s3 = MagicMock()

        with (
            patch.object(snap, "_iter_snapshot_files", return_value=[(1, "d/op/0/1.bin")]),
            patch.object(snap, "_get_zstd_msgpack", return_value=delta_1),
        ):
            data = snap._load_operator_state(mock_s3, 1, [("op", 0)])

        assert "k1" not in data[("op", 0)]
        assert data[("op", 0)]["k2"] == "v2"


# ---------------------------------------------------------------------------
# Step 5: migration_reassign_delta_maps preserves deltas
# ---------------------------------------------------------------------------


class TestMigrationReassignDeltaMaps:
    def test_preserves_existing_deltas(self):
        from worker.async_snapshotting import AsyncSnapshottingProcess

        proc = AsyncSnapshottingProcess.__new__(AsyncSnapshottingProcess)
        proc.delta_maps = {
            ("op", 0): {"k1": "v1"},
            ("op", 1): {"k2": "v2"},
        }

        # Reassign: keep partition 0, add partition 2, drop partition 1
        proc.migration_reassign_delta_maps([("op", 0), ("op", 2)])

        assert proc.delta_maps[("op", 0)] == {"k1": "v1"}  # preserved
        assert proc.delta_maps[("op", 2)] == {}  # new, empty
        assert ("op", 1) not in proc.delta_maps  # removed

    def test_no_change_when_same_partitions(self):
        from worker.async_snapshotting import AsyncSnapshottingProcess

        proc = AsyncSnapshottingProcess.__new__(AsyncSnapshottingProcess)
        proc.delta_maps = {
            ("op", 0): {"k1": "v1"},
            ("op", 1): {"k2": "v2"},
        }

        proc.migration_reassign_delta_maps([("op", 0), ("op", 1)])

        assert proc.delta_maps[("op", 0)] == {"k1": "v1"}
        assert proc.delta_maps[("op", 1)] == {"k2": "v2"}

    def test_all_new_partitions(self):
        from worker.async_snapshotting import AsyncSnapshottingProcess

        proc = AsyncSnapshottingProcess.__new__(AsyncSnapshottingProcess)
        proc.delta_maps = {("op", 0): {"k1": "v1"}}

        proc.migration_reassign_delta_maps([("op", 2), ("op", 3)])

        assert ("op", 0) not in proc.delta_maps
        assert proc.delta_maps[("op", 2)] == {}
        assert proc.delta_maps[("op", 3)] == {}

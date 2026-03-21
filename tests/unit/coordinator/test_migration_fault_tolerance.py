"""Unit tests for migration fault tolerance features in coordinator.

Covers:
- Pre-migration snapshot tracking (register_snapshot)
- Deferred graph update (finalize_graph_update)
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from coordinator.coordinator_metadata import Coordinator


def _mock_coordinator():
    return Coordinator(MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Step 8: Pre-migration snapshot tracking
# ---------------------------------------------------------------------------


class TestPreMigrationSnapshotTracking:
    def test_pre_migration_snapshot_recorded(self):
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 3}
        c.prev_completed_snapshot_id = 2
        c.pre_migration_snapshot_pending = True
        c.pre_migration_snapshot_id = -1

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        assert c.pre_migration_snapshot_id == 3
        assert c.pre_migration_snapshot_pending is False

    def test_pre_migration_snapshot_not_recorded_when_not_pending(self):
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 3}
        c.prev_completed_snapshot_id = 2
        c.pre_migration_snapshot_pending = False
        c.pre_migration_snapshot_id = -1

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        assert c.pre_migration_snapshot_id == -1

    def test_pre_migration_snapshot_not_recorded_when_no_advance(self):
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 2}  # min is 2, same as prev
        c.prev_completed_snapshot_id = 2
        c.pre_migration_snapshot_pending = True
        c.pre_migration_snapshot_id = -1

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        # Snapshot didn't advance, so pre-migration not recorded yet
        assert c.pre_migration_snapshot_pending is True
        assert c.pre_migration_snapshot_id == -1


# ---------------------------------------------------------------------------
# Step 9: Deferred graph update
# ---------------------------------------------------------------------------


class TestDeferredGraphUpdate:
    @pytest.mark.asyncio
    async def test_finalize_graph_update_commits_pending(self):
        c = _mock_coordinator()
        c.submitted_graph = MagicMock(name="old_graph")
        new_graph = MagicMock(name="new_graph")
        new_graph.name = "test-app"
        c._pending_graph = new_graph
        c.kafka_metadata_producer = AsyncMock()

        c.finalize_graph_update()

        assert c.submitted_graph is new_graph
        assert c._pending_graph is None

    def test_finalize_graph_update_noop_when_no_pending(self):
        c = _mock_coordinator()
        old_graph = MagicMock(name="old_graph")
        c.submitted_graph = old_graph
        c._pending_graph = None

        c.finalize_graph_update()

        assert c.submitted_graph is old_graph

    def test_pending_graph_initialized_to_none(self):
        c = _mock_coordinator()
        assert c._pending_graph is None
        assert c.pre_migration_snapshot_id == -1
        assert c.pre_migration_snapshot_pending is False

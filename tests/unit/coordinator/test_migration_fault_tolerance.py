"""Unit tests for migration fault tolerance features in coordinator.

Covers:
- Pre-migration snapshot tracking (register_snapshot)
- Deferred graph update (finalize_graph_update)
- Worker pool revert on recovery during migration
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

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


# ---------------------------------------------------------------------------
# Worker pool revert on recovery during migration
# ---------------------------------------------------------------------------


class TestRevertWorkerPoolToSubmittedGraph:
    def _setup_coordinator_with_graph(self, n_partitions, max_parallelism=8):
        """Create a coordinator with a submitted graph and worker pool."""
        c = _mock_coordinator()
        c.max_operator_parallelism = max_parallelism

        g = StateflowGraph("test-app", operator_state_backend=LocalStateBackend.DICT, max_operator_parallelism=max_parallelism)
        op = Operator("ycsb", n_partitions=n_partitions)
        g.add_operators(op)
        c.submitted_graph = g

        # Simulate worker pool with update_operator tracking
        c.worker_pool = MagicMock()
        return c

    def test_revert_restores_shadow_partitions(self):
        c = self._setup_coordinator_with_graph(n_partitions=4, max_parallelism=8)

        c.revert_worker_pool_to_submitted_graph()

        # Should have called update_operator for all 8 partitions
        assert c.worker_pool.update_operator.call_count == 8

        # Partitions 0-3 should be active (not shadow)
        for call_args in c.worker_pool.update_operator.call_args_list[:4]:
            op_part, operator = call_args[0]
            assert op_part[0] == "ycsb"
            assert not operator.is_shadow

        # Partitions 4-7 should be shadow
        for call_args in c.worker_pool.update_operator.call_args_list[4:]:
            op_part, operator = call_args[0]
            assert op_part[0] == "ycsb"
            assert operator.is_shadow

    def test_revert_noop_when_no_graph(self):
        c = _mock_coordinator()
        c.submitted_graph = None
        c.max_operator_parallelism = None
        c.worker_pool = MagicMock()

        c.revert_worker_pool_to_submitted_graph()

        c.worker_pool.update_operator.assert_not_called()

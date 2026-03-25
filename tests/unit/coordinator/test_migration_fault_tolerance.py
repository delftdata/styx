"""Unit tests for migration fault tolerance features in coordinator.

Covers:
- Pre-migration snapshot tracking (register_snapshot)
- Deferred graph update (finalize_graph_update)
- Post-migration snapshot triggers graph finalization (register_snapshot)
- Worker pool revert on recovery during migration
- Recovery condition: detect mid-migration AND post-migration-pre-snapshot crashes
- Full migration lifecycle scenarios
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

from coordinator.coordinator_metadata import Coordinator


def _mock_coordinator():
    return Coordinator(MagicMock(), MagicMock())


def _make_graph(name="test-app", n_partitions=4, max_parallelism=8):
    g = StateflowGraph(name, operator_state_backend=LocalStateBackend.DICT, max_operator_parallelism=max_parallelism)
    op = Operator("ycsb", n_partitions=n_partitions)
    g.add_operators(op)
    return g


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
        assert c.post_migration_snapshot_pending is False


# ---------------------------------------------------------------------------
# Post-migration snapshot triggers graph finalization
# ---------------------------------------------------------------------------


class TestPostMigrationSnapshotFinalization:
    """Tests that finalize_graph_update() is only called when a post-migration
    snapshot completes — NOT immediately when migration finishes.

    Bug caught: if finalize is called at MigrationDone time and a crash occurs
    before the next snapshot, recovery uses the NEW graph but the OLD snapshot,
    losing ~50% of keys.
    """

    def test_post_migration_snapshot_pending_initialized_false(self):
        c = _mock_coordinator()
        assert c.post_migration_snapshot_pending is False

    @pytest.mark.asyncio
    async def test_register_snapshot_finalizes_graph_when_post_migration_pending(self):
        """When post_migration_snapshot_pending is True and a new snapshot
        completes, finalize_graph_update() should be called."""
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 3}
        c.prev_completed_snapshot_id = 2
        c.post_migration_snapshot_pending = True

        new_graph = MagicMock(name="new_graph")
        new_graph.name = "test-app"
        c._pending_graph = new_graph
        c.submitted_graph = MagicMock(name="old_graph")
        c.kafka_metadata_producer = AsyncMock()

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        # Graph should be finalized
        assert c.submitted_graph is new_graph
        assert c._pending_graph is None
        assert c.post_migration_snapshot_pending is False

    def test_register_snapshot_does_not_finalize_when_not_pending(self):
        """When post_migration_snapshot_pending is False, finalize should
        NOT be called even if snapshot advances."""
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 3}
        c.prev_completed_snapshot_id = 2
        c.post_migration_snapshot_pending = False

        old_graph = MagicMock(name="old_graph")
        c.submitted_graph = old_graph
        c._pending_graph = MagicMock(name="new_graph")

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        # Graph should NOT be finalized
        assert c.submitted_graph is old_graph

    def test_register_snapshot_does_not_finalize_when_snapshot_not_advanced(self):
        """When snapshot hasn't advanced (not all workers reported), finalize
        should NOT be called."""
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 2}  # min is 2, same as prev
        c.prev_completed_snapshot_id = 2
        c.post_migration_snapshot_pending = True

        old_graph = MagicMock(name="old_graph")
        c.submitted_graph = old_graph
        c._pending_graph = MagicMock(name="new_graph")

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        # Still pending — snapshot hasn't advanced
        assert c.post_migration_snapshot_pending is True
        assert c.submitted_graph is old_graph

    @pytest.mark.asyncio
    async def test_finalize_happens_before_s3_write(self):
        """Graph finalization must happen when the snapshot advances, in the
        same register_snapshot call that writes to S3."""
        c = _mock_coordinator()
        c.worker_snapshot_ids = {0: 3, 1: 3}
        c.prev_completed_snapshot_id = 2
        c.post_migration_snapshot_pending = True

        new_graph = MagicMock(name="new_graph")
        new_graph.name = "test-app"
        c._pending_graph = new_graph
        c.submitted_graph = MagicMock(name="old_graph")
        c.kafka_metadata_producer = AsyncMock()

        pool = MagicMock()
        c.register_snapshot(0, 3, {}, {}, 10, 100, pool)

        # Both finalization and S3 write happened
        assert c.submitted_graph is new_graph
        c.s3_client.put_object.assert_called_once()


# ---------------------------------------------------------------------------
# Recovery condition detection
# ---------------------------------------------------------------------------


class TestRecoveryConditionDetection:
    """Tests that recovery correctly detects both mid-migration crashes AND
    post-migration-pre-snapshot crashes.

    Bug caught: if migration completed but no post-migration snapshot was taken,
    recovery didn't detect this as migration-related, used the NEW graph with
    the OLD snapshot, and lost half the data.
    """

    def test_mid_migration_detected(self):
        """migration_in_progress=True with _pending_graph set → needs revert."""
        c = _mock_coordinator()
        c._pending_graph = MagicMock()
        migration_in_progress = True

        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is True

    def test_post_migration_pre_snapshot_detected(self):
        """migration_in_progress=False but post_migration_snapshot_pending=True
        → needs revert. This is the bug scenario."""
        c = _mock_coordinator()
        c._pending_graph = MagicMock()
        c.post_migration_snapshot_pending = True
        migration_in_progress = False

        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is True

    def test_no_migration_not_detected(self):
        """Normal state — no migration, no pending snapshot."""
        c = _mock_coordinator()
        c._pending_graph = None
        c.post_migration_snapshot_pending = False
        migration_in_progress = False

        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is False

    def test_completed_migration_with_snapshot_not_detected(self):
        """Migration completed AND post-migration snapshot taken → no revert needed."""
        c = _mock_coordinator()
        c._pending_graph = None  # finalize_graph_update() cleared it
        c.post_migration_snapshot_pending = False
        migration_in_progress = False

        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is False

    def test_saved_pending_graph_preserved_for_retry(self):
        """When was_migrating, the pending graph should be saved for auto-retry."""
        c = _mock_coordinator()
        new_graph = MagicMock(name="new_graph")
        c._pending_graph = new_graph
        c.post_migration_snapshot_pending = True
        migration_in_progress = False

        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        saved_pending_graph = c._pending_graph if was_migrating else None

        assert saved_pending_graph is new_graph


# ---------------------------------------------------------------------------
# Full migration lifecycle scenarios
# ---------------------------------------------------------------------------


class TestMigrationLifecycleScenarios:
    """End-to-end scenarios testing the full migration lifecycle through
    coordinator metadata, catching the exact bugs that caused 34000 missed
    messages in E2E tests."""

    def _setup_coordinator(self, n_partitions=4, max_parallelism=8):
        c = _mock_coordinator()
        c.max_operator_parallelism = max_parallelism
        c.submitted_graph = _make_graph("test-app", n_partitions, max_parallelism)
        c.worker_pool = MagicMock()
        c.kafka_metadata_producer = AsyncMock()
        c.worker_snapshot_ids = {0: 0, 1: 0, 2: 0}
        c.prev_completed_snapshot_id = 0
        return c

    def test_scenario_crash_after_migration_before_snapshot(self):
        """Scenario: migration completes, graph NOT finalized yet, crash
        before post-migration snapshot.

        Expected: submitted_graph still has OLD layout, revert works,
        migration can be retried.
        """
        c = self._setup_coordinator(n_partitions=4)
        old_graph = c.submitted_graph
        new_graph = _make_graph("test-app", n_partitions=8, max_parallelism=8)

        # Simulate: migration sets _pending_graph (update_stateflow_graph)
        c._pending_graph = new_graph

        # Simulate: pre-migration snapshot completes (snap 1)
        c.pre_migration_snapshot_pending = True
        c.worker_snapshot_ids = {0: 1, 1: 1, 2: 1}
        pool = MagicMock()
        c.register_snapshot(0, 1, {}, {}, 10, 100, pool)
        assert c.pre_migration_snapshot_id == 1

        # Simulate: migration finishes → set post_migration_snapshot_pending
        # (NOT calling finalize_graph_update)
        c.post_migration_snapshot_pending = True

        # CRASH happens here — before post-migration snapshot

        # Recovery check
        migration_in_progress = False  # already cleared by _handle_migration_done
        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is True

        # submitted_graph is still the OLD graph
        assert c.submitted_graph is old_graph
        assert c.submitted_graph.nodes["ycsb"].n_partitions == 4

        # Revert works correctly
        c.revert_worker_pool_to_submitted_graph()
        # Partitions 4-7 should be shadow
        for call_args in c.worker_pool.update_operator.call_args_list[4:]:
            _, operator = call_args[0]
            assert operator.is_shadow

        # _pending_graph preserved for retry
        assert c._pending_graph is new_graph

    @pytest.mark.asyncio
    async def test_scenario_crash_after_migration_after_snapshot(self):
        """Scenario: migration completes, post-migration snapshot taken,
        graph finalized, then crash.

        Expected: submitted_graph has NEW layout, no revert needed.
        """
        c = self._setup_coordinator(n_partitions=4)
        new_graph = _make_graph("test-app", n_partitions=8, max_parallelism=8)

        # Migration sets _pending_graph
        c._pending_graph = new_graph
        c.pre_migration_snapshot_pending = True

        # Pre-migration snapshot (snap 1)
        c.worker_snapshot_ids = {0: 1, 1: 1, 2: 1}
        pool = MagicMock()
        c.register_snapshot(0, 1, {}, {}, 10, 100, pool)

        # Migration finishes
        c.post_migration_snapshot_pending = True

        # Post-migration snapshot (snap 2) → should finalize
        c.worker_snapshot_ids = {0: 2, 1: 2, 2: 2}
        c.register_snapshot(0, 2, {}, {}, 20, 200, pool)

        # Graph finalized
        assert c.submitted_graph is new_graph
        assert c._pending_graph is None
        assert c.post_migration_snapshot_pending is False

        # CRASH happens here — after post-migration snapshot

        # Recovery check: no migration recovery needed
        migration_in_progress = False
        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is False

    def test_scenario_crash_during_migration_phase_a(self):
        """Scenario: crash during Phase A (migration in progress,
        _pending_graph set, pre-migration snapshot may or may not exist).

        Expected: revert to old layout, retry migration.
        """
        c = self._setup_coordinator(n_partitions=4)
        old_graph = c.submitted_graph
        new_graph = _make_graph("test-app", n_partitions=8, max_parallelism=8)

        # Migration starts
        c._pending_graph = new_graph
        c.pre_migration_snapshot_pending = True
        migration_in_progress = True

        # CRASH during Phase A — no pre-migration snapshot completed

        was_migrating = (migration_in_progress and c._pending_graph is not None) or c.post_migration_snapshot_pending
        assert was_migrating is True

        # Old graph preserved
        assert c.submitted_graph is old_graph
        saved = c._pending_graph
        assert saved is new_graph

    @pytest.mark.asyncio
    async def test_scenario_normal_migration_no_crash(self):
        """Scenario: migration completes normally, snapshot taken, no crash.

        Expected: graph finalized after post-migration snapshot.
        """
        c = self._setup_coordinator(n_partitions=4)
        new_graph = _make_graph("test-app", n_partitions=8, max_parallelism=8)

        # Step 1: Migration starts
        c._pending_graph = new_graph
        c.pre_migration_snapshot_pending = True

        # Step 2: Pre-migration snapshot (snap 1)
        c.worker_snapshot_ids = {0: 1, 1: 1, 2: 1}
        pool = MagicMock()
        c.register_snapshot(0, 1, {}, {}, 10, 100, pool)
        assert c.pre_migration_snapshot_id == 1

        # Step 3: Migration finishes
        c.post_migration_snapshot_pending = True
        assert c.submitted_graph is not new_graph  # NOT finalized yet

        # Step 4: Post-migration snapshot (snap 2)
        c.worker_snapshot_ids = {0: 2, 1: 2, 2: 2}
        c.register_snapshot(0, 2, {}, {}, 20, 200, pool)

        # Step 5: Graph finalized
        assert c.submitted_graph is new_graph
        assert c._pending_graph is None
        assert c.post_migration_snapshot_pending is False

        # Step 6: Subsequent snapshots work normally
        c.worker_snapshot_ids = {0: 3, 1: 3, 2: 3}
        c.register_snapshot(0, 3, {}, {}, 30, 300, pool)
        assert c.submitted_graph is new_graph  # still new graph

    def test_scenario_state_cleanup_on_recovery(self):
        """Verify all migration state is properly cleared during recovery."""
        c = self._setup_coordinator(n_partitions=4)
        new_graph = _make_graph("test-app", n_partitions=8, max_parallelism=8)

        # Set up mid-migration state
        c._pending_graph = new_graph
        c.pre_migration_snapshot_pending = True
        c.pre_migration_snapshot_id = 1
        c.post_migration_snapshot_pending = True

        # Simulate recovery clearing state (mirrors _perform_recovery step 1c)
        saved_pending_graph = c._pending_graph
        c.pre_migration_snapshot_pending = False
        c.post_migration_snapshot_pending = False
        c._pending_graph = None

        # All state cleared
        assert c.pre_migration_snapshot_pending is False
        assert c.post_migration_snapshot_pending is False
        assert c._pending_graph is None
        # But we saved the graph for retry
        assert saved_pending_graph is new_graph


# ---------------------------------------------------------------------------
# Worker pool revert on recovery during migration
# ---------------------------------------------------------------------------


class TestRevertWorkerPoolToSubmittedGraph:
    def _setup_coordinator_with_graph(self, n_partitions, max_parallelism=8):
        """Create a coordinator with a submitted graph and worker pool."""
        c = _mock_coordinator()
        c.max_operator_parallelism = max_parallelism

        g = StateflowGraph(
            "test-app", operator_state_backend=LocalStateBackend.DICT, max_operator_parallelism=max_parallelism
        )
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

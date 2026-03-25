"""Unit tests for coordinator/coordinator_metadata.py"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from styx.common.exceptions import NotAStateflowGraphError
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

from coordinator.coordinator_metadata import Coordinator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_networking():
    n = MagicMock()
    n.send_message = AsyncMock()
    return n


def _mock_s3():
    return MagicMock()


def _coordinator():
    return Coordinator(_mock_networking(), _mock_s3())


def _graph(name="test-app", n_partitions=2, max_parallelism=4):
    g = StateflowGraph(name, operator_state_backend=LocalStateBackend.DICT, max_operator_parallelism=max_parallelism)
    op = Operator("users", n_partitions)
    op.register(lambda: None)  # dummy function
    g.add_operator(op)
    return g


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestCoordinatorInit:
    def test_defaults(self):
        c = _coordinator()
        assert c.graph_submitted is False
        assert c.prev_completed_snapshot_id == -1
        assert c.submitted_graph is None
        assert c.max_operator_parallelism is None
        assert c.completed_epoch_counter == 0
        assert c.completed_t_counter == 0

    def test_empty_worker_state(self):
        c = _coordinator()
        assert c.worker_snapshot_ids == {}
        assert c.worker_is_healthy == {}
        assert c.worker_ip_to_id == {}


# ---------------------------------------------------------------------------
# register_worker
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


class TestRegisterWorker:
    def test_new_worker_registered(self):
        async def _test():
            c = _coordinator()
            worker_id, init_recovery = c.register_worker("10.0.0.1", 5000, 6000)
            assert isinstance(worker_id, int)
            assert init_recovery is False

        _run(_test())

    def test_same_worker_returns_same_id(self):
        async def _test():
            c = _coordinator()
            w1, r1 = c.register_worker("10.0.0.1", 5000, 6000)
            w2, r2 = c.register_worker("10.0.0.1", 5000, 6000)
            assert w1 == w2
            assert r1 is False
            assert r2 is True  # second registration = recovery

        _run(_test())

    def test_different_workers_different_ids(self):
        async def _test():
            c = _coordinator()
            w1, _ = c.register_worker("10.0.0.1", 5000, 6000)
            w2, _ = c.register_worker("10.0.0.2", 5001, 6001)
            assert w1 != w2

        _run(_test())

    def test_snapshot_id_tracked(self):
        async def _test():
            c = _coordinator()
            w_id, _ = c.register_worker("10.0.0.1", 5000, 6000)
            assert w_id in c.worker_snapshot_ids

        _run(_test())


# ---------------------------------------------------------------------------
# get_current_completed_snapshot_id
# ---------------------------------------------------------------------------


class TestCompletedSnapshotId:
    def test_no_workers(self):
        c = _coordinator()
        assert c.get_current_completed_snapshot_id() == -1

    def test_single_worker(self):
        c = _coordinator()
        c.worker_snapshot_ids[0] = 5
        assert c.get_current_completed_snapshot_id() == 5

    def test_multiple_workers_min(self):
        c = _coordinator()
        c.worker_snapshot_ids[0] = 5
        c.worker_snapshot_ids[1] = 3
        c.worker_snapshot_ids[2] = 7
        assert c.get_current_completed_snapshot_id() == 3


# ---------------------------------------------------------------------------
# init_data_complete
# ---------------------------------------------------------------------------


class TestInitDataComplete:
    def test_resets_snapshot_ids(self):
        c = _coordinator()
        c.worker_snapshot_ids = {0: 5, 1: 3}
        c.init_data_complete()
        assert c.worker_snapshot_ids == {0: 0, 1: 0}
        assert c.prev_completed_snapshot_id == 0


# ---------------------------------------------------------------------------
# register_worker_heartbeat / check_heartbeats
# ---------------------------------------------------------------------------


class TestHeartbeat:
    def test_register_heartbeat(self):
        c = _coordinator()
        # Manually set up worker pool state to avoid event loop requirement
        c.worker_pool.register_worker_heartbeat = MagicMock()
        c.register_worker_heartbeat(0, 1000.0)
        c.worker_pool.register_worker_heartbeat.assert_called_once_with(0, 1000.0)

    def test_check_heartbeats_no_graph(self):
        c = _coordinator()
        failed, times = c.check_heartbeats(1000.0)
        assert failed == set()
        assert times == {}


# ---------------------------------------------------------------------------
# register_snapshot
# ---------------------------------------------------------------------------


class TestRegisterSnapshot:
    def test_updates_snapshot_id(self):
        c = _coordinator()
        c.worker_snapshot_ids = {0: 0, 1: 0}
        c.prev_completed_snapshot_id = 0
        pool = MagicMock()

        c.register_snapshot(
            worker_id=0,
            snapshot_id=1,
            partial_input_offsets={("users", 0): 10},
            partial_output_offsets={("users", 0): 5},
            epoch_counter=1,
            t_counter=100,
            pool=pool,
        )
        assert c.worker_snapshot_ids[0] == 1
        assert c.completed_input_offsets[("users", 0)] == 10
        assert c.completed_out_offsets[("users", 0)] == 5
        assert c.completed_epoch_counter == 1
        assert c.completed_t_counter == 100

    def test_partial_offsets_take_max(self):
        c = _coordinator()
        c.worker_snapshot_ids = {0: 0, 1: 0}
        c.prev_completed_snapshot_id = 0
        c.completed_input_offsets = {("users", 0): 20}
        pool = MagicMock()

        c.register_snapshot(
            worker_id=0,
            snapshot_id=1,
            partial_input_offsets={("users", 0): 10},
            partial_output_offsets={},
            epoch_counter=1,
            t_counter=100,
            pool=pool,
        )
        # Should keep the max (20 > 10)
        assert c.completed_input_offsets[("users", 0)] == 20

    def test_completed_snapshot_writes_to_s3(self):
        async def _test():
            c = _coordinator()
            c.worker_snapshot_ids = {0: 0}
            c.prev_completed_snapshot_id = 0
            pool = MagicMock()

            c.register_snapshot(
                worker_id=0,
                snapshot_id=1,
                partial_input_offsets={},
                partial_output_offsets={},
                epoch_counter=1,
                t_counter=100,
                pool=pool,
            )
            # When all workers have snapshot_id=1, completed becomes 1 (different from prev 0)
            c.s3_client.put_object.assert_called_once()
            call_kwargs = c.s3_client.put_object.call_args[1]
            assert call_kwargs["Key"] == "sequencer/1.bin"

        _run(_test())


# ---------------------------------------------------------------------------
# get_worker_with_id
# ---------------------------------------------------------------------------


class TestGetWorkerWithId:
    def test_returns_worker(self):
        async def _test():
            c = _coordinator()
            w_id, _ = c.register_worker("10.0.0.1", 5000, 6000)
            worker = c.get_worker_with_id(w_id)
            assert worker.worker_ip == "10.0.0.1"
            assert worker.worker_port == 5000

        _run(_test())


# ---------------------------------------------------------------------------
# worker_is_ready_after_recovery / wait_cluster_healthy
# ---------------------------------------------------------------------------


class TestRecoveryReady:
    @pytest.mark.asyncio
    async def test_worker_ready_sets_event(self):
        c = _coordinator()
        c.worker_is_healthy = {0: asyncio.Event(), 1: asyncio.Event()}
        c.worker_is_ready_after_recovery(0)
        assert c.worker_is_healthy[0].is_set()
        assert not c.worker_is_healthy[1].is_set()

    @pytest.mark.asyncio
    async def test_wait_cluster_healthy(self):
        c = _coordinator()
        c.worker_is_healthy = {0: asyncio.Event(), 1: asyncio.Event()}
        c.worker_is_healthy[0].set()
        c.worker_is_healthy[1].set()
        await c.wait_cluster_healthy()  # should not block


# ---------------------------------------------------------------------------
# update_stateflow_graph validation
# ---------------------------------------------------------------------------


class TestUpdateStafeflowGraphValidation:
    @pytest.mark.asyncio
    async def test_rejects_non_graph(self):
        c = _coordinator()
        with pytest.raises(NotAStateflowGraphError):
            await c.update_stateflow_graph("not a graph")

    @pytest.mark.asyncio
    async def test_pending_graph_set_before_gather(self):
        """_pending_graph must be set BEFORE sending InitMigration to workers."""
        from copy import deepcopy

        c = _coordinator()
        c.max_operator_parallelism = 4
        c.register_worker("10.0.0.1", 5000, 6000)

        # Manually schedule operator partitions (avoids Kafka in submit_stateflow_graph)
        old_g = _graph(n_partitions=1, max_parallelism=4)
        for _, operator in iter(old_g):
            for p in range(4):
                op_copy = deepcopy(operator)
                if p >= operator.n_partitions:
                    op_copy.make_shadow()
                c.worker_pool.schedule_operator_partition((op_copy.name, p), op_copy)
        c.submitted_graph = old_g

        new_g = _graph(n_partitions=2, max_parallelism=4)

        # Track when _pending_graph was set relative to send_message calls
        set_before_send = None
        original_send = c.networking.send_message

        async def tracking_send(*args, **kwargs):
            nonlocal set_before_send
            if set_before_send is None:
                set_before_send = c._pending_graph is not None
            return await original_send(*args, **kwargs)

        c.networking.send_message = tracking_send

        await c.update_stateflow_graph(new_g)
        assert set_before_send is True, "_pending_graph must be set before send_message"
        assert c._pending_graph is new_g


# ---------------------------------------------------------------------------
# finalize_graph_update
# ---------------------------------------------------------------------------


class TestFinalizeGraphUpdate:
    def test_noop_when_no_pending(self):
        c = _coordinator()
        c._pending_graph = None
        c.finalize_graph_update()  # should not raise
        assert c.submitted_graph is None

    @pytest.mark.asyncio
    async def test_commits_pending_graph(self):
        c = _coordinator()
        c.kafka_metadata_producer = AsyncMock()
        old_graph = _graph(n_partitions=1)
        new_graph = _graph(n_partitions=2)
        c.submitted_graph = old_graph
        c._pending_graph = new_graph
        c.finalize_graph_update()
        assert c.submitted_graph is new_graph
        assert c._pending_graph is None


# ---------------------------------------------------------------------------
# set_migration_checkpoint / clear_migration_checkpoint
# ---------------------------------------------------------------------------


class TestMigrationCheckpoint:
    def test_set_and_clear(self):
        c = _coordinator()
        g = _graph()
        locations = {("users", 0): 1}
        c.set_migration_checkpoint(g, locations)
        assert c._migration_checkpoint_blob is not None
        c.clear_migration_checkpoint()
        assert c._migration_checkpoint_blob is None


# ---------------------------------------------------------------------------
# register_snapshot — migration checkpoint detection
# ---------------------------------------------------------------------------


class TestRegisterSnapshotCheckpointDetection:
    def test_checkpoint_event_fires_when_snapshot_exceeds_baseline(self):
        c = _coordinator()
        pool = MagicMock()
        c.worker_snapshot_ids = {0: 0}
        c.prev_completed_snapshot_id = 0
        # Simulate checkpoint blob set with baseline 0
        c._migration_checkpoint_baseline_snap_id = 0
        c._migration_checkpoint_blob = b"fake"

        c.register_snapshot(0, 1, {}, {}, 1, 1, pool)

        assert c._migration_checkpoint_snapshot_complete.is_set()
        assert c._migration_checkpoint_baseline_snap_id == -1

    def test_checkpoint_event_does_not_fire_when_below_baseline(self):
        c = _coordinator()
        pool = MagicMock()
        c.worker_snapshot_ids = {0: 0, 1: 0}
        c.prev_completed_snapshot_id = 0
        c._migration_checkpoint_baseline_snap_id = 1  # baseline is 1

        c.register_snapshot(0, 1, {}, {}, 1, 1, pool)
        # Worker 1 still at 0, so cluster min is 0, not > baseline 1
        assert not c._migration_checkpoint_snapshot_complete.is_set()

    def test_pre_migration_snapshot_recorded(self):
        c = _coordinator()
        pool = MagicMock()
        c.worker_snapshot_ids = {0: 0}
        c.prev_completed_snapshot_id = 0
        c.pre_migration_snapshot_pending = True

        c.register_snapshot(0, 1, {}, {}, 1, 1, pool)

        assert c.pre_migration_snapshot_id == 1
        assert c.pre_migration_snapshot_pending is False

    @pytest.mark.asyncio
    async def test_post_migration_finalize_fires(self):
        c = _coordinator()
        c.kafka_metadata_producer = AsyncMock()
        pool = MagicMock()
        c.worker_snapshot_ids = {0: 0}
        c.prev_completed_snapshot_id = 0
        c.post_migration_snapshot_pending = True
        c._pending_graph = _graph()
        c.submitted_graph = _graph(n_partitions=1)

        c.register_snapshot(0, 1, {}, {}, 1, 1, pool)

        assert c.post_migration_snapshot_pending is False


# ---------------------------------------------------------------------------
# revert_worker_pool_to_submitted_graph
# ---------------------------------------------------------------------------


class TestRevertWorkerPool:
    def test_revert_restores_old_layout(self):
        from copy import deepcopy

        c = _coordinator()
        c.max_operator_parallelism = 4
        wid, _ = c.register_worker("10.0.0.1", 5000, 6000)

        # Schedule initial partitions (like submit_stateflow_graph would)
        old_graph = _graph(n_partitions=2, max_parallelism=4)
        for _, operator in iter(old_graph):
            for p in range(2):
                c.worker_pool.schedule_operator_partition(
                    (operator.name, p),
                    deepcopy(operator),
                )
            for p in range(2, 4):
                op_copy = deepcopy(operator)
                op_copy.make_shadow()
                c.worker_pool.schedule_operator_partition(
                    (operator.name, p),
                    op_copy,
                )
        c.submitted_graph = old_graph

        # Simulate update_stateflow_graph promoting shadow -> active
        new_graph = _graph(n_partitions=4, max_parallelism=4)
        for _, operator in iter(new_graph):
            for partition in range(4):
                op_copy = deepcopy(operator)
                if partition >= operator.n_partitions:
                    op_copy.make_shadow()
                c.worker_pool.update_operator((op_copy.name, partition), op_copy)

        # Partition 2 and 3 should now be active (4 partitions)
        w = c.worker_pool.peek(wid)
        assert not w.assigned_operators[("users", 2)].is_shadow
        assert not w.assigned_operators[("users", 3)].is_shadow

        # Revert
        c.revert_worker_pool_to_submitted_graph()

        # Partitions 2 and 3 should be shadow again (old graph had 2 partitions)
        assert w.assigned_operators[("users", 2)].is_shadow
        assert w.assigned_operators[("users", 3)].is_shadow
        assert not w.assigned_operators[("users", 0)].is_shadow

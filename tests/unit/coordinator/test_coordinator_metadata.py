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

"""Additional coverage tests for coordinator/coordinator_metadata.py

Covers: check_heartbeats with graph_submitted, start_recovery_process,
register_snapshot with COMPACT_SNAPSHOTS, create_kafka_ingress_topics.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
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
    op.register(lambda: None)
    g.add_operator(op)
    return g


# ---------------------------------------------------------------------------
# check_heartbeats with graph_submitted=True
# ---------------------------------------------------------------------------


class TestCheckHeartbeatsWithGraph:
    def test_delegates_to_worker_pool(self):
        c = _coordinator()
        c.graph_submitted = True
        c.worker_pool.check_heartbeats = MagicMock(return_value=(set(), {0: 1.0}))
        _failed, times = c.check_heartbeats(1000.0)
        c.worker_pool.check_heartbeats.assert_called_once_with(1000.0)
        assert times == {0: 1.0}


# ---------------------------------------------------------------------------
# start_recovery_process
# ---------------------------------------------------------------------------


class TestStartRecoveryProcess:
    @pytest.mark.asyncio
    async def test_recovery_removes_workers(self):
        c = _coordinator()
        # Register two workers
        w1_id, _ = c.register_worker("10.0.0.1", 5000, 6000)
        w2_id, _ = c.register_worker("10.0.0.2", 5001, 6001)
        c.submitted_graph = _graph()

        worker_to_remove = c.get_worker_with_id(w1_id)

        # Mock the pool methods needed for recovery
        c.worker_pool.initiate_recovery = AsyncMock()
        c.worker_pool.get_operator_partition_locations = MagicMock(return_value={})
        c.worker_pool.get_worker_assignments = MagicMock(return_value={("10.0.0.2", 5001, 6001): []})
        c.worker_pool.get_participating_workers = MagicMock(return_value=[c.get_worker_with_id(w2_id)])
        c.worker_pool.get_workers = MagicMock(return_value={})

        await c.start_recovery_process({worker_to_remove})
        assert w1_id not in c.worker_snapshot_ids
        c.worker_pool.initiate_recovery.assert_called_once()


# ---------------------------------------------------------------------------
# register_snapshot with COMPACT_SNAPSHOTS=True
# ---------------------------------------------------------------------------


class TestRegisterSnapshotCompaction:
    @pytest.mark.asyncio
    async def test_compaction_triggered_on_new_snapshot(self):
        c = _coordinator()
        c.worker_snapshot_ids = {0: 0}
        c.prev_completed_snapshot_id = 0
        pool = MagicMock()

        with patch("coordinator.coordinator_metadata.COMPACT_SNAPSHOTS", True):
            loop = asyncio.get_running_loop()
            with patch.object(loop, "run_in_executor") as mock_exec:
                c.register_snapshot(
                    worker_id=0,
                    snapshot_id=1,
                    partial_input_offsets={},
                    partial_output_offsets={},
                    epoch_counter=1,
                    t_counter=100,
                    pool=pool,
                )
                mock_exec.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_compaction_when_snapshot_unchanged(self):
        c = _coordinator()
        c.worker_snapshot_ids = {0: 1, 1: 0}
        c.prev_completed_snapshot_id = 0
        pool = MagicMock()

        with patch("coordinator.coordinator_metadata.COMPACT_SNAPSHOTS", True):
            c.register_snapshot(
                worker_id=0,
                snapshot_id=2,
                partial_input_offsets={},
                partial_output_offsets={},
                epoch_counter=2,
                t_counter=200,
                pool=pool,
            )
            # Min is still 0 (worker 1), so no new completed snapshot
            assert c.prev_completed_snapshot_id == 0


# ---------------------------------------------------------------------------
# create_kafka_ingress_topics
# ---------------------------------------------------------------------------


class TestCreateKafkaIngressTopics:
    @pytest.mark.asyncio
    async def test_creates_topics(self):
        c = _coordinator()
        c.max_operator_parallelism = 4
        # Pre-set a producer so it doesn't try to start one
        c.kafka_metadata_producer = MagicMock()
        graph = _graph()

        with patch("coordinator.coordinator_metadata.AdminClient") as MockAdmin:
            mock_admin = MagicMock()
            MockAdmin.return_value = mock_admin

            # Simulate topic creation futures
            topic_futures = {}
            for topic_name in ["styx-metadata", "sequencer-wal", "users", "users--OUT"]:
                f = MagicMock()
                f.result.return_value = None
                topic_futures[topic_name] = f
            mock_admin.create_topics.return_value = topic_futures

            await c.create_kafka_ingress_topics(graph)
            mock_admin.create_topics.assert_called_once()

    @pytest.mark.asyncio
    async def test_topic_creation_error_handled(self):
        c = _coordinator()
        c.max_operator_parallelism = 4
        c.kafka_metadata_producer = MagicMock()
        graph = _graph()

        with patch("coordinator.coordinator_metadata.AdminClient") as MockAdmin:
            mock_admin = MagicMock()
            MockAdmin.return_value = mock_admin

            from confluent_kafka.admin import KafkaException

            f = MagicMock()
            f.result.side_effect = KafkaException(MagicMock())
            mock_admin.create_topics.return_value = {"users": f}

            # Should not raise, just log
            await c.create_kafka_ingress_topics(graph)

    @pytest.mark.asyncio
    async def test_starts_metadata_producer_if_none(self):
        c = _coordinator()
        c.max_operator_parallelism = 4
        c.kafka_metadata_producer = None
        graph = _graph()

        with patch("coordinator.coordinator_metadata.AdminClient") as MockAdmin:
            mock_admin = MagicMock()
            MockAdmin.return_value = mock_admin
            mock_admin.create_topics.return_value = {}

            c.start_kafka_metadata_producer = AsyncMock()
            await c.create_kafka_ingress_topics(graph)
            c.start_kafka_metadata_producer.assert_called_once()


# ---------------------------------------------------------------------------
# submit_stateflow_graph
# ---------------------------------------------------------------------------


class TestSubmitStateflowGraph:
    @pytest.mark.asyncio
    async def test_submit_graph_sets_state(self):
        c = _coordinator()
        # Register workers first
        c.register_worker("10.0.0.1", 5000, 6000)
        c.register_worker("10.0.0.2", 5001, 6001)
        graph = _graph()
        c.kafka_metadata_producer = MagicMock()
        c.kafka_metadata_producer.send_and_wait = AsyncMock()

        with patch("coordinator.coordinator_metadata.AdminClient") as MockAdmin:
            mock_admin = MagicMock()
            MockAdmin.return_value = mock_admin
            mock_admin.create_topics.return_value = {}

            await c.submit_stateflow_graph(graph)

        assert c.graph_submitted is True
        assert c.submitted_graph is graph
        assert c.max_operator_parallelism == graph.max_operator_parallelism


# ---------------------------------------------------------------------------
# notify_cluster_healthy
# ---------------------------------------------------------------------------


class TestNotifyClusterHealthy:
    @pytest.mark.asyncio
    async def test_sends_ready_to_all_workers(self):
        from coordinator.worker_pool import Worker

        networking = _mock_networking()
        c = Coordinator(networking, _mock_s3())
        # Mock the worker pool to return participating workers
        w1 = Worker(
            worker_id=0,
            worker_ip="10.0.0.1",
            worker_port=5000,
            protocol_port=6000,
            assigned_operators={("op", 0): MagicMock()},
        )
        w2 = Worker(
            worker_id=1,
            worker_ip="10.0.0.2",
            worker_port=5001,
            protocol_port=6001,
            assigned_operators={("op", 1): MagicMock()},
        )
        c.worker_pool.get_participating_workers = MagicMock(return_value=[w1, w2])

        await c.notify_cluster_healthy()
        assert networking.send_message.call_count == 2

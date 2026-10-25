"""How long a migration checkpoint stays attached to snapshots.

A snapshot cut before the last migrated key landed still has keys at their old
partitions, so its sequencer file must carry the checkpoint even if it completes
after MigrationDone. Recovery from it resumes the transfer, and the coordinator
must keep tracking that resumed transfer.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from styx.common.local_state_backends import LocalStateBackend
from styx.common.message_types import MessageType
from styx.common.operator import Operator
from styx.common.serialization import Serializer, cloudpickle_serialization, zstd_msgpack_deserialization
from styx.common.stateflow_graph import StateflowGraph
from styx.common.tcp_networking import NetworkingManager

from coordinator import coordinator_service
from coordinator.aria_sync_metadata import AriaSyncMetadata
from coordinator.coordinator_metadata import Coordinator
from coordinator.migration_metadata import MigrationMetadata
from coordinator.worker_pool import HEARTBEAT_LIMIT
from tests.unit.coordinator.service_helpers import stub_service


def _graph(n_partitions: int) -> StateflowGraph:
    g = StateflowGraph("app", operator_state_backend=LocalStateBackend.DICT, max_operator_parallelism=8)
    g.add_operators(Operator("ycsb", n_partitions=n_partitions))
    return g


def _complete_snapshot(c: Coordinator, snapshot_id: int, epoch: int, *, checkpoint: bool = False):
    """All (two) workers register snapshot_id; returns the sequencer tuple written to S3."""
    c.s3_client.put_object.reset_mock()
    for worker_id in (0, 1):
        c.register_snapshot(worker_id, snapshot_id, {}, {}, epoch, 0, MagicMock(), is_migration_checkpoint=checkpoint)
    body = c.s3_client.put_object.call_args.kwargs["Body"]
    return zstd_msgpack_deserialization(body)


def _coordinator_with_checkpoint() -> Coordinator:
    c = Coordinator(MagicMock(), MagicMock())
    c.s3_client = MagicMock()
    c.worker_snapshot_ids = {0: 2, 1: 2}
    c.prev_completed_snapshot_id = 2
    c.set_migration_checkpoint(_graph(8), {}, from_snapshot_id=3)
    return c


class TestCheckpointLifetime:
    def test_snapshot_cut_before_last_key_landed_keeps_checkpoint(self):
        c = _coordinator_with_checkpoint()
        c.end_migration_checkpoint_after(transfer_done_epoch=50)

        # Cut at the end of epoch 49 (carries 50), completes after MigrationDone.
        assert _complete_snapshot(c, 3, epoch=50)[4] is not None
        # Cut at the end of epoch 50 (carries 51): every key had landed.
        assert _complete_snapshot(c, 4, epoch=51)[4] is None
        assert _complete_snapshot(c, 5, epoch=60)[4] is None

    def test_checkpoint_kept_while_transfer_runs(self):
        c = _coordinator_with_checkpoint()
        assert _complete_snapshot(c, 3, epoch=500)[4] is not None

    def test_new_checkpoint_resets_the_end(self):
        c = _coordinator_with_checkpoint()
        c.end_migration_checkpoint_after(transfer_done_epoch=50)
        c.set_migration_checkpoint(_graph(16), {}, from_snapshot_id=3)
        assert _complete_snapshot(c, 3, epoch=60)[4] is not None


class TestCheckpointTagging:
    def test_snapshot_cut_before_phase_b_gets_no_checkpoint(self):
        c = Coordinator(MagicMock(), MagicMock())
        c.s3_client = MagicMock()
        c.worker_snapshot_ids = {0: 2, 1: 2}
        c.prev_completed_snapshot_id = 2
        c.set_migration_checkpoint(_graph(8), {})
        c._migration_checkpoint_baseline_snap_id = 2  # waiting for the checkpoint

        # Started before Phase B, completes after the checkpoint metadata was set.
        assert _complete_snapshot(c, 3, epoch=40)[4] is None
        assert not c._migration_checkpoint_snapshot_complete.is_set()

        # The checkpoint itself, flagged by the workers.
        assert _complete_snapshot(c, 4, epoch=41, checkpoint=True)[4] is not None
        assert c._migration_checkpoint_snapshot_complete.is_set()
        # Snapshots after it keep the checkpoint until the transfer ends.
        assert _complete_snapshot(c, 5, epoch=45)[4] is not None

    def test_checkpoint_not_complete_until_every_worker_has_it(self):
        c = Coordinator(MagicMock(), MagicMock())
        c.s3_client = MagicMock()
        c.worker_snapshot_ids = {0: 2, 1: 2}
        c.prev_completed_snapshot_id = 2
        c.set_migration_checkpoint(_graph(8), {})
        c._migration_checkpoint_baseline_snap_id = 2
        c.register_snapshot(0, 3, {}, {}, 41, 0, MagicMock(), is_migration_checkpoint=True)
        assert not c.migration_checkpoint_completed()
        c.register_snapshot(1, 3, {}, {}, 41, 0, MagicMock(), is_migration_checkpoint=True)
        assert c.migration_checkpoint_completed()


class TestMigrationDoneEpochs:
    async def test_transfer_done_epoch_is_the_latest_reported(self):
        m = MigrationMetadata(2)
        assert await m.migration_done(12) is False
        assert await m.migration_done(10) is True
        assert m.transfer_done_epoch == 12
        await m.cleanup(MessageType.MigrationDone)
        assert m.transfer_done_epoch == -1


def _migration_done_msg(epoch: int) -> bytes:
    return NetworkingManager.encode_message(
        msg=(epoch,),
        msg_type=MessageType.MigrationDone,
        serializer=Serializer.MSGPACK,
    )[8:]


class TestMigrationDoneHandler:
    async def test_all_workers_done_ends_checkpoint_after_latest_epoch(self):
        c = _coordinator_with_checkpoint()
        svc = stub_service(c)
        svc.migration_in_progress = True

        await svc._handle_migration_done(_migration_done_msg(41))
        assert svc.migration_in_progress is True
        await svc._handle_migration_done(_migration_done_msg(43))

        assert svc.migration_in_progress is False
        assert c._migration_checkpoint_blob is not None  # kept for snapshots cut up to 43
        assert c._migration_checkpoint_until_epoch == 43


def _blob(graph: StateflowGraph) -> bytes:
    return cloudpickle_serialization({"new_graph": graph, "operator_partition_locations": {}})


class TestRecoveryFromCheckpoint:
    async def test_resume_after_migration_done_keeps_tracking_the_transfer(self):
        c = Coordinator(MagicMock(), MagicMock())
        c.worker_snapshot_ids = {0: 7, 1: 7}
        svc = stub_service(c)
        # MigrationDone already processed; the latest snapshot still has the checkpoint.
        svc._read_migration_blob_from_s3 = MagicMock(return_value=_blob(_graph(8)))
        state_when_workers_released = {}

        async def notify() -> None:
            state_when_workers_released["in_progress"] = svc.migration_in_progress
            state_when_workers_released["blob"] = c._migration_checkpoint_blob

        c.notify_cluster_healthy = AsyncMock(side_effect=notify)
        await svc._perform_recovery({MagicMock(name="dead_worker")})

        svc._read_migration_blob_from_s3.assert_called_once_with(7)
        assert c.submitted_graph.nodes["ycsb"].n_partitions == 8
        assert state_when_workers_released["in_progress"] is True
        assert state_when_workers_released["blob"] is not None

    async def test_newer_migration_without_checkpoint_is_retried_after_resume(self):
        c = Coordinator(MagicMock(), MagicMock())
        c.worker_snapshot_ids = {0: 7, 1: 7}
        newer = _graph(16)
        c._pending_graph = newer
        svc = stub_service(c)
        svc.migration_in_progress = True
        svc._read_migration_blob_from_s3 = MagicMock(return_value=_blob(_graph(8)))

        await svc._perform_recovery({MagicMock(name="dead_worker")})

        assert svc._deferred_migration_graph is newer
        assert c.submitted_graph.nodes["ycsb"].n_partitions == 8
        c.revert_worker_pool_to_submitted_graph.assert_called_once()

    async def test_crash_before_checkpoint_reverts_and_retries(self):
        c = Coordinator(MagicMock(), MagicMock())
        c.worker_snapshot_ids = {0: 7, 1: 7}
        pending = _graph(8)
        c._pending_graph = pending
        svc = stub_service(c)
        svc.migration_in_progress = True
        svc._read_migration_blob_from_s3 = MagicMock(return_value=None)

        await svc._perform_recovery({MagicMock(name="dead_worker")})

        assert svc.migration_in_progress is False
        assert svc._deferred_migration_graph is pending
        assert c._migration_checkpoint_blob is None
        c.revert_worker_pool_to_submitted_graph.assert_called_once()

    async def test_ordinary_recovery_stays_ordinary(self):
        c = Coordinator(MagicMock(), MagicMock())
        c.worker_snapshot_ids = {0: 7, 1: 7}
        svc = stub_service(c)
        svc._read_migration_blob_from_s3 = MagicMock(return_value=None)

        await svc._perform_recovery({MagicMock(name="dead_worker")})

        assert svc.migration_in_progress is False
        assert svc._deferred_migration_graph is None
        c.revert_worker_pool_to_submitted_graph.assert_not_called()


class TestUnreachableWorkers:
    def _service_with_workers(self, *results):
        c = Coordinator(MagicMock(), MagicMock())
        svc = stub_service(c)
        svc.unreachable_workers = []
        workers = [MagicMock(worker_id=i, worker_ip=f"w{i}", protocol_port=6000 + i) for i in range(len(results))]
        c.worker_pool = MagicMock(get_participating_workers=MagicMock(return_value=workers))
        svc.protocol_networking.send_message = AsyncMock(side_effect=list(results))
        return svc, workers

    async def test_worker_a_barrier_reply_cannot_reach_is_handed_to_recovery(self):
        svc, workers = self._service_with_workers(True, False)
        await svc.finalize_worker_sync(MessageType.SyncCleanup, (True,))
        assert svc.unreachable_workers == [workers[1]]

    async def test_connection_error_counts_as_undelivered(self):
        svc, workers = self._service_with_workers(ConnectionRefusedError(), True)
        await svc.finalize_worker_sync(MessageType.SyncCleanup, (True,))
        assert svc.unreachable_workers == [workers[0]]

    async def test_all_delivered(self):
        svc, _ = self._service_with_workers(True, True)
        await svc.finalize_worker_sync(MessageType.SyncCleanup, (False,))
        assert svc.unreachable_workers == []

    def test_dead_unreachable_worker_is_left_to_the_heartbeat_check(self):
        # Recovered as a participant, a dead worker would get InitRecovery and
        # recovery would wait on it forever; the heartbeat check removes it instead.
        svc, workers = self._service_with_workers(True, True)
        svc.unreachable_workers = [workers[1]]
        svc.coordinator.worker_pool.get_participating_workers.return_value = [workers[0]]

        assert svc._unreachable_to_recover(100.0) == []
        assert svc.unreachable_workers == []
        assert svc._unreachable_since == {}

    def test_live_unreachable_worker_is_recovered_after_the_heartbeat_limit(self):
        svc, workers = self._service_with_workers(True, True)
        svc.unreachable_workers = [workers[1]]
        limit_s = HEARTBEAT_LIMIT / 1000

        assert svc._unreachable_to_recover(100.0) == []
        assert svc._unreachable_to_recover(100.0 + limit_s / 2) == []
        assert svc._unreachable_to_recover(100.0 + limit_s) == [workers[1]]


class TestBarrierWatchdog:
    async def test_names_the_workers_a_stalled_barrier_waits_on(self, monkeypatch):
        c = Coordinator(MagicMock(), MagicMock())
        svc = stub_service(c)
        workers = [MagicMock(worker_id=i) for i in (1, 2, 3)]
        c.worker_pool = MagicMock(get_participating_workers=MagicMock(return_value=workers))
        svc.aria_metadata = AriaSyncMetadata(3)
        svc.aria_metadata.arrived.update({1, 3})
        warnings: list[str] = []
        monkeypatch.setattr(coordinator_service, "BARRIER_STALL_WARN_S", 0.01)
        monkeypatch.setattr(coordinator_service.logging, "warning", warnings.append)

        watchdog = asyncio.create_task(svc.barrier_watchdog())
        await asyncio.sleep(0.05)
        stalled = list(warnings)
        # Barriers completing again: nothing more is reported.
        for _ in range(10):
            svc.aria_metadata.cleanup()
            svc.aria_metadata.arrived.add(1)
            await asyncio.sleep(0.01)
        watchdog.cancel()

        assert stalled
        assert "arrived [1, 3], waiting on [2]" in stalled[0]
        assert len(warnings) <= len(stalled) + 1

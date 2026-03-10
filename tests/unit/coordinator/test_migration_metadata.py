"""Unit tests for coordinator/migration_metadata.py"""

import asyncio

from migration_metadata import MigrationMetadata
from styx.common.message_types import MessageType


def run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# check_sum
# ---------------------------------------------------------------------------


class TestCheckSum:
    def test_false_initially(self):
        meta = MigrationMetadata(n_workers=3)
        assert meta.check_sum(MessageType.MigrationRepartitioningDone) is False

    def test_false_with_partial_arrivals(self):
        meta = MigrationMetadata(n_workers=3)
        meta.sync_sum[MessageType.MigrationRepartitioningDone] = 2
        assert meta.check_sum(MessageType.MigrationRepartitioningDone) is False

    def test_true_when_all_workers_counted(self):
        meta = MigrationMetadata(n_workers=3)
        meta.sync_sum[MessageType.MigrationRepartitioningDone] = 3
        assert meta.check_sum(MessageType.MigrationRepartitioningDone) is True

    def test_independent_per_message_type(self):
        meta = MigrationMetadata(n_workers=2)
        meta.sync_sum[MessageType.MigrationRepartitioningDone] = 2
        # A different message type should still be 0
        assert meta.check_sum(MessageType.MigrationDone) is False


# ---------------------------------------------------------------------------
# repartitioning_done
# ---------------------------------------------------------------------------


class TestRepartitioningDone:
    def test_returns_false_before_all_workers(self):
        meta = MigrationMetadata(n_workers=3)
        result = run(meta.repartitioning_done(1, 100, {("op", 0): 5}, {("op", 0): 3}))
        assert result is False

    def test_returns_true_when_all_workers_reported(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.repartitioning_done(1, 10, {}, {}))
        result = run(meta.repartitioning_done(5, 20, {}, {}))
        assert result is True

    def test_input_offsets_takes_max(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.repartitioning_done(1, 0, {("op", 0): 10}, {}))
        run(meta.repartitioning_done(5, 0, {("op", 0): 5}, {}))
        assert meta.input_offsets[("op", 0)] == 10

    def test_output_offsets_takes_max(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.repartitioning_done(1, 0, {}, {("op", 0): 3}))
        run(meta.repartitioning_done(5, 0, {}, {("op", 0): 7}))
        assert meta.output_offsets[("op", 0)] == 7

    def test_epoch_counter_takes_max(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.repartitioning_done(100, 0, {}, {}))
        run(meta.repartitioning_done(50, 0, {}, {}))
        assert meta.epoch_counter == 100

    def test_t_counter_takes_max(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.repartitioning_done(1, 0, {}, {}))
        run(meta.repartitioning_done(5, 0, {}, {}))
        # t_counter passed as second arg; both calls pass 0 → max stays 0
        # (t_counter starts at -1, max(0, -1) = 0)
        assert meta.t_counter == 0

    def test_multiple_partitions_merged(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.repartitioning_done(1, 0, {("op", 0): 10, ("op", 1): 20}, {}))
        run(meta.repartitioning_done(5, 0, {("op", 2): 30}, {}))
        assert ("op", 0) in meta.input_offsets
        assert ("op", 1) in meta.input_offsets
        assert ("op", 2) in meta.input_offsets

    def test_increments_sync_sum(self):
        meta = MigrationMetadata(n_workers=3)
        run(meta.repartitioning_done(1, 0, {}, {}))
        run(meta.repartitioning_done(2, 0, {}, {}))
        assert meta.sync_sum[MessageType.MigrationRepartitioningDone] == 2


# ---------------------------------------------------------------------------
# set_empty_sync_done
# ---------------------------------------------------------------------------


class TestSetEmptySyncDone:
    def test_increments_counter(self):
        meta = MigrationMetadata(n_workers=3)
        run(meta.set_empty_sync_done(MessageType.MigrationDone))
        assert meta.sync_sum[MessageType.MigrationDone] == 1

    def test_returns_false_before_all_workers(self):
        meta = MigrationMetadata(n_workers=3)
        result = run(meta.set_empty_sync_done(MessageType.MigrationDone))
        assert result is False

    def test_returns_true_when_all_workers_arrived(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.set_empty_sync_done(MessageType.MigrationDone))
        result = run(meta.set_empty_sync_done(MessageType.MigrationDone))
        assert result is True

    def test_independent_per_message_type(self):
        meta = MigrationMetadata(n_workers=2)
        run(meta.set_empty_sync_done(MessageType.MigrationDone))
        run(meta.set_empty_sync_done(MessageType.MigrationDone))
        # MigrationRepartitioningDone not touched
        assert meta.sync_sum[MessageType.MigrationRepartitioningDone] == 0


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_cleanup_resets_sync_sum_for_msg_type(self):
        meta = MigrationMetadata(n_workers=2)
        meta.sync_sum[MessageType.MigrationDone] = 2
        run(meta.cleanup(MessageType.MigrationDone))
        assert meta.sync_sum[MessageType.MigrationDone] == 0

    def test_cleanup_does_not_affect_other_msg_types(self):
        meta = MigrationMetadata(n_workers=2)
        meta.sync_sum[MessageType.MigrationDone] = 2
        meta.sync_sum[MessageType.MigrationRepartitioningDone] = 1
        run(meta.cleanup(MessageType.MigrationDone))
        assert meta.sync_sum[MessageType.MigrationRepartitioningDone] == 1

    def test_cleanup_repartitioning_clears_offsets(self):
        meta = MigrationMetadata(n_workers=2)
        meta.input_offsets[("op", 0)] = 10
        meta.output_offsets[("op", 0)] = 5
        meta.epoch_counter = 99
        meta.t_counter = 42
        run(meta.cleanup(MessageType.MigrationRepartitioningDone))
        assert len(meta.input_offsets) == 0
        assert len(meta.output_offsets) == 0
        assert meta.epoch_counter == -1
        assert meta.t_counter == -1

    def test_cleanup_other_msg_type_does_not_clear_offsets(self):
        meta = MigrationMetadata(n_workers=2)
        meta.input_offsets[("op", 0)] = 10
        run(meta.cleanup(MessageType.MigrationDone))
        assert meta.input_offsets[("op", 0)] == 10

    def test_cleanup_allows_reuse(self):
        """After cleanup the barrier can be used again."""
        meta = MigrationMetadata(n_workers=2)
        run(meta.set_empty_sync_done(MessageType.MigrationDone))
        run(meta.set_empty_sync_done(MessageType.MigrationDone))
        assert meta.check_sum(MessageType.MigrationDone) is True
        run(meta.cleanup(MessageType.MigrationDone))
        assert meta.check_sum(MessageType.MigrationDone) is False

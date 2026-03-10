"""Unit tests for coordinator/aria_sync_metadata.py"""

from aria_sync_metadata import AriaSyncMetadata

# ---------------------------------------------------------------------------
# check_distributed_barrier
# ---------------------------------------------------------------------------


class TestCheckDistributedBarrier:
    def test_false_when_no_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=3)
        assert meta.check_distributed_barrier() is False

    def test_false_when_partial_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=3)
        meta.arrived.add(1)
        meta.arrived.add(2)
        assert meta.check_distributed_barrier() is False

    def test_true_when_all_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=3)
        meta.arrived.update({1, 2, 3})
        assert meta.check_distributed_barrier() is True

    def test_single_worker_cluster(self):
        meta = AriaSyncMetadata(n_workers=1)
        meta.arrived.add(1)
        assert meta.check_distributed_barrier() is True


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------


class TestFlags:
    def test_stop_in_next_epoch(self):
        meta = AriaSyncMetadata(n_workers=2)
        assert meta.stop_next_epoch is False
        meta.stop_in_next_epoch()
        assert meta.stop_next_epoch is True

    def test_take_snapshot_at_next_epoch(self):
        meta = AriaSyncMetadata(n_workers=2)
        assert meta.take_snapshot is False
        meta.take_snapshot_at_next_epoch()
        assert meta.take_snapshot is True


# ---------------------------------------------------------------------------
# set_aria_processing_done
# ---------------------------------------------------------------------------


class TestSetAriaProcessingDone:
    def test_returns_false_before_all_workers(self):
        meta = AriaSyncMetadata(n_workers=3)
        result = meta.set_aria_processing_done(1, {10, 11})
        assert result is False

    def test_returns_true_when_all_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_processing_done(1, set())
        result = meta.set_aria_processing_done(2, set())
        assert result is True

    def test_aggregates_logic_aborts_union(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_processing_done(1, {10, 11})
        meta.set_aria_processing_done(2, {11, 12})
        assert meta.logic_aborts_everywhere == {10, 11, 12}

    def test_duplicate_worker_id_does_not_double_count(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_processing_done(1, {1})
        meta.set_aria_processing_done(1, {2})  # same worker again
        # arrived is a set, so still size 1
        assert len(meta.arrived) == 1
        assert meta.check_distributed_barrier() is False


# ---------------------------------------------------------------------------
# set_aria_commit_done
# ---------------------------------------------------------------------------


class TestSetAriaCommitDone:
    def test_returns_false_before_all_workers(self):
        meta = AriaSyncMetadata(n_workers=3)
        result = meta.set_aria_commit_done(1, {5}, remote_t_counter=10, processed_seq_size=100)
        assert result is False

    def test_returns_true_when_all_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_commit_done(1, set(), 10, 50)
        result = meta.set_aria_commit_done(2, set(), 20, 50)
        assert result is True

    def test_aggregates_concurrency_aborts(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_commit_done(1, {1, 2}, 0, 0)
        meta.set_aria_commit_done(2, {2, 3}, 0, 0)
        assert meta.concurrency_aborts_everywhere == {1, 2, 3}

    def test_tracks_max_t_counter(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_commit_done(1, set(), remote_t_counter=50, processed_seq_size=0)
        meta.set_aria_commit_done(2, set(), remote_t_counter=30, processed_seq_size=0)
        assert meta.max_t_counter == 50

    def test_sums_processed_seq_size(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_aria_commit_done(1, set(), 0, processed_seq_size=200)
        meta.set_aria_commit_done(2, set(), 0, processed_seq_size=300)
        assert meta.processed_seq_size == 500

    def test_initial_max_t_counter_is_negative_one(self):
        meta = AriaSyncMetadata(n_workers=1)
        assert meta.max_t_counter == -1


# ---------------------------------------------------------------------------
# set_empty_sync_done
# ---------------------------------------------------------------------------


class TestSetEmptySyncDone:
    def test_returns_false_before_all_workers(self):
        meta = AriaSyncMetadata(n_workers=3)
        assert meta.set_empty_sync_done(1) is False

    def test_returns_true_when_all_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.set_empty_sync_done(1)
        result = meta.set_empty_sync_done(2)
        assert result is True


# ---------------------------------------------------------------------------
# set_deterministic_reordering_done
# ---------------------------------------------------------------------------


class TestSetDeterministicReorderingDone:
    def _rr(self, ns, t_id, write_set, read_set=None):
        """Build a minimal read_reservation / write_set / read_set triple."""
        if read_set is None:
            read_set = write_set
        return (
            {ns: {t_id: [1]}},  # read_reservation
            {ns: {t_id: write_set}},  # write_set
            {ns: {t_id: read_set}},  # read_set
        )

    def test_first_worker_sets_initial_values(self):
        meta = AriaSyncMetadata(n_workers=2)
        rr, ws, rs = self._rr("users", 0, {10, 11})
        meta.set_deterministic_reordering_done(1, rr, ws, rs)
        assert meta.global_write_set == ws
        assert meta.global_read_set == rs
        assert meta.global_read_reservations == rr

    def test_second_worker_merges_disjoint_t_ids(self):
        meta = AriaSyncMetadata(n_workers=2)
        rr1, ws1, rs1 = self._rr("users", 0, {10})
        rr2, ws2, rs2 = self._rr("users", 1, {20})
        meta.set_deterministic_reordering_done(1, rr1, ws1, rs1)
        meta.set_deterministic_reordering_done(2, rr2, ws2, rs2)
        assert 0 in meta.global_write_set["users"]
        assert 1 in meta.global_write_set["users"]

    def test_second_worker_merges_shared_t_id_union(self):
        meta = AriaSyncMetadata(n_workers=2)
        rr1, ws1, rs1 = self._rr("users", 0, {10, 11})
        rr2, ws2, rs2 = self._rr("users", 0, {11, 12})
        meta.set_deterministic_reordering_done(1, rr1, ws1, rs1)
        meta.set_deterministic_reordering_done(2, rr2, ws2, rs2)
        assert meta.global_write_set["users"][0] == {10, 11, 12}

    def test_returns_true_when_all_workers_arrived(self):
        meta = AriaSyncMetadata(n_workers=2)
        rr, ws, rs = self._rr("users", 0, {1})
        meta.set_deterministic_reordering_done(1, rr, ws, rs)
        result = meta.set_deterministic_reordering_done(2, rr, ws, rs)
        assert result is True


# ---------------------------------------------------------------------------
# __merge_rw_sets (tested via set_deterministic_reordering_done)
# ---------------------------------------------------------------------------


class TestMergeRwSets:
    """
    Tests for the static merge logic, exercised via set_deterministic_reordering_done.
    """

    def _apply_two(self, ws1, ws2, n_workers=2):
        meta = AriaSyncMetadata(n_workers=n_workers)
        empty_rr = {}
        meta.set_deterministic_reordering_done(1, empty_rr, ws1, ws1)
        meta.set_deterministic_reordering_done(2, empty_rr, ws2, ws2)
        return meta.global_write_set

    def test_disjoint_namespaces_both_present(self):
        ws1 = {"ns_a": {0: {1, 2}}}
        ws2 = {"ns_b": {0: {3, 4}}}
        merged = self._apply_two(ws1, ws2)
        assert "ns_a" in merged
        assert "ns_b" in merged

    def test_shared_namespace_shared_t_id_union(self):
        ws1 = {"ns": {0: {1, 2}}}
        ws2 = {"ns": {0: {2, 3}}}
        merged = self._apply_two(ws1, ws2)
        assert merged["ns"][0] == {1, 2, 3}

    def test_shared_namespace_disjoint_t_ids_both_present(self):
        ws1 = {"ns": {0: {10}}}
        ws2 = {"ns": {1: {20}}}
        merged = self._apply_two(ws1, ws2)
        assert 0 in merged["ns"]
        assert 1 in merged["ns"]

    def test_only_in_first_preserved(self):
        ws1 = {"ns": {0: {10}}}
        ws2 = {}
        merged = self._apply_two(ws1, ws2)
        assert merged["ns"][0] == {10}

    def test_only_in_second_preserved(self):
        ws1 = {}
        ws2 = {"ns": {0: {99}}}
        merged = self._apply_two(ws1, ws2)
        assert merged["ns"][0] == {99}


# ---------------------------------------------------------------------------
# __merge_rw_reservations (tested via set_deterministic_reordering_done)
# ---------------------------------------------------------------------------


class TestMergeRwReservations:
    def _apply_two_reservations(self, rr1, rr2):
        meta = AriaSyncMetadata(n_workers=2)
        empty_ws = {}
        meta.set_deterministic_reordering_done(1, rr1, empty_ws, empty_ws)
        meta.set_deterministic_reordering_done(2, rr2, empty_ws, empty_ws)
        return meta.global_read_reservations

    def test_shared_key_lists_concatenated(self):
        rr1 = {"ns": {"key1": [1, 2]}}
        rr2 = {"ns": {"key1": [3, 4]}}
        merged = self._apply_two_reservations(rr1, rr2)
        assert sorted(merged["ns"]["key1"]) == [1, 2, 3, 4]

    def test_disjoint_keys_both_present(self):
        rr1 = {"ns": {"key1": [1]}}
        rr2 = {"ns": {"key2": [2]}}
        merged = self._apply_two_reservations(rr1, rr2)
        assert "key1" in merged["ns"]
        assert "key2" in merged["ns"]

    def test_only_in_first_preserved(self):
        rr1 = {"ns": {"key1": [1]}}
        rr2 = {}
        merged = self._apply_two_reservations(rr1, rr2)
        assert merged["ns"]["key1"] == [1]

    def test_only_in_second_preserved(self):
        rr1 = {}
        rr2 = {"ns": {"key1": [5]}}
        merged = self._apply_two_reservations(rr1, rr2)
        assert merged["ns"]["key1"] == [5]


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_cleanup_resets_arrived(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.arrived.update({1, 2})
        meta.cleanup()
        assert len(meta.arrived) == 0

    def test_cleanup_resets_aborts(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.logic_aborts_everywhere = {1, 2}
        meta.concurrency_aborts_everywhere = {3}
        meta.cleanup()
        assert len(meta.logic_aborts_everywhere) == 0
        assert len(meta.concurrency_aborts_everywhere) == 0

    def test_cleanup_resets_counters(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.processed_seq_size = 500
        meta.max_t_counter = 42
        meta.cleanup()
        assert meta.processed_seq_size == 0
        assert meta.max_t_counter == -1

    def test_cleanup_resets_rw_sets(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.global_write_set = {"ns": {0: {1}}}
        meta.global_read_set = {"ns": {0: {1}}}
        meta.global_read_reservations = {"ns": {"k": [1]}}
        meta.cleanup()
        assert meta.global_write_set is None
        assert meta.global_read_set is None
        assert meta.global_read_reservations is None

    def test_cleanup_resets_sent_proceed_msg(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.sent_proceed_msg = True
        meta.cleanup()
        assert meta.sent_proceed_msg is False

    def test_cleanup_epoch_end_resets_stop_next_epoch(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.stop_next_epoch = True
        meta.cleanup(epoch_end=True)
        assert meta.stop_next_epoch is False

    def test_cleanup_without_epoch_end_preserves_stop_next_epoch(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.stop_next_epoch = True
        meta.cleanup(epoch_end=False)
        assert meta.stop_next_epoch is True

    def test_cleanup_take_snapshot_resets_flag(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.take_snapshot = True
        meta.cleanup(take_snapshot=True)
        assert meta.take_snapshot is False

    def test_cleanup_without_take_snapshot_preserves_flag(self):
        meta = AriaSyncMetadata(n_workers=2)
        meta.take_snapshot = True
        meta.cleanup(take_snapshot=False)
        assert meta.take_snapshot is True

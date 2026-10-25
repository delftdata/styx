"""Unit tests for worker/fault_tolerance/async_snapshots.py"""

from unittest.mock import MagicMock, patch

from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snap(worker_id=0, n_partitions=4, snapshot_id=1):
    return AsyncSnapshotsS3(worker_id, n_partitions, snapshot_id)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestAsyncSnapshotsInit:
    def test_defaults(self):
        s = _snap()
        assert s.worker_id == 0
        assert s.n_assigned_partitions == 4
        assert s.snapshot_id == 1
        assert s._in_progress == {}

    def test_zero_partitions(self):
        s = AsyncSnapshotsS3(worker_id=1)
        assert s.n_assigned_partitions == 0
        assert s.snapshot_id == 1


# ---------------------------------------------------------------------------
# update_n_assigned_partitions
# ---------------------------------------------------------------------------


class TestUpdateNAssignedPartitions:
    def test_updates(self):
        s = _snap()
        s.update_n_assigned_partitions(8)
        assert s.n_assigned_partitions == 8


# ---------------------------------------------------------------------------
# set_snapshot_id
# ---------------------------------------------------------------------------


class TestSetSnapshotId:
    def test_sets_plus_one(self):
        s = _snap()
        s.set_snapshot_id(5)
        assert s.snapshot_id == 6


# ---------------------------------------------------------------------------
# Snapshot lifecycle
# ---------------------------------------------------------------------------


def _done(exc=None):
    f = MagicMock()
    f.cancelled.return_value = False
    f.exception.return_value = exc
    return f


def _sent_snapshot_ids(mock_socket):
    """Snapshot ids in the SnapID messages sent to the coordinator, in order."""
    from styx.common.tcp_networking import NetworkingManager

    sent = [c.args[0] for c in mock_socket.socket.return_value.send.call_args_list]
    return [NetworkingManager.decode_message(m[8:])[1] for m in sent]


@patch("worker.fault_tolerance.async_snapshots.socket")
class TestSnapshotLifecycle:
    def test_reports_once_every_file_is_stored(self, mock_socket):
        s = _snap(snapshot_id=5)
        sid = s.start_snapshotting({("op", 0): 1}, {("op", 0): 0}, 10, 100, n_files=2)
        assert sid == 5
        assert s.snapshot_id == 6
        s.register_size(sid, 100)
        s.file_stored_callback(sid)(_done())
        assert _sent_snapshot_ids(mock_socket) == []
        s.file_stored_callback(sid)(_done())
        assert _sent_snapshot_ids(mock_socket) == [5]
        msg = mock_socket.socket.return_value.send.call_args.args[0]
        from styx.common.tcp_networking import NetworkingManager

        (_w, _sid, _start, _end, _in, _out, epoch, t_counter, size, checkpoint, pending) = (
            NetworkingManager.decode_message(msg[8:])
        )
        assert (epoch, t_counter, size, checkpoint) == (10, 100, 100, False)
        assert list(pending) == []

    def test_reports_pending_requests(self, mock_socket):
        from styx.common.tcp_networking import NetworkingManager

        s = _snap(snapshot_id=5)
        pending = [[b"rq", "k", "op", 1, "f", [], 40, 0]]
        s.start_snapshotting({("op", 0): 41}, {}, 10, 100, n_files=0, pending_requests=pending)
        msg = mock_socket.socket.return_value.send.call_args.args[0]
        assert [list(p) for p in NetworkingManager.decode_message(msg[8:])[10]] == pending

    def test_overlapping_snapshots_keep_their_own_ids_and_metadata(self, mock_socket):
        s = _snap(snapshot_id=1)
        first = s.start_snapshotting({}, {}, 10, 100, n_files=1)
        second = s.start_snapshotting({}, {}, 11, 110, n_files=1)
        assert (first, second) == (1, 2)
        assert s._in_progress[first].epoch_counter == 10
        assert s._in_progress[second].epoch_counter == 11

    def test_later_snapshot_waits_for_earlier_one(self, mock_socket):
        s = _snap(snapshot_id=1)
        first = s.start_snapshotting({}, {}, 10, 100, n_files=1)
        second = s.start_snapshotting({}, {}, 11, 110, n_files=1)
        s.file_stored_callback(second)(_done())  # the later, smaller one finishes first
        assert _sent_snapshot_ids(mock_socket) == []
        s.file_stored_callback(first)(_done())
        assert _sent_snapshot_ids(mock_socket) == [1, 2]

    def test_failed_upload_blocks_it_and_later_snapshots(self, mock_socket):
        s = _snap(snapshot_id=1)
        first = s.start_snapshotting({}, {}, 10, 100, n_files=1)
        second = s.start_snapshotting({}, {}, 11, 110, n_files=1)
        s.file_stored_callback(first)(_done(RuntimeError("S3 down")))
        s.file_stored_callback(second)(_done())
        assert _sent_snapshot_ids(mock_socket) == []

    def test_snapshot_without_files_is_reported_immediately(self, mock_socket):
        s = _snap(snapshot_id=3)
        s.start_snapshotting({}, {}, 1, 1, n_files=0)
        assert _sent_snapshot_ids(mock_socket) == [3]


# ---------------------------------------------------------------------------
# store_snapshot
# ---------------------------------------------------------------------------


class TestStoreSnapshot:
    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_puts_object(self, mock_mk):
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        result = AsyncSnapshotsS3.store_snapshot("data/op/0/1.bin", b"data")
        assert result is True
        mock_s3.put_object.assert_called_once_with(
            Bucket="styx-snapshots",
            Key="data/op/0/1.bin",
            Body=b"data",
        )


# ---------------------------------------------------------------------------
# retrieve_snapshot
# ---------------------------------------------------------------------------


class TestRetrieveSnapshot:
    def test_snapshot_id_negative_one_returns_empty(self):
        s = _snap()
        data, tp_off, tp_out_off, epoch, t_counter, migration_blob, pending = s.retrieve_snapshot(-1, [])
        assert pending == []
        assert data == {}
        assert tp_off == {}
        assert tp_out_off == {}
        assert epoch == 0
        assert t_counter == 0
        assert migration_blob is None
        assert s.snapshot_id == 0  # -1 + 1

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_retrieve_with_no_files(self, mock_mk):
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3
        # Empty paginator
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = paginator

        s = _snap()
        data, _tp_off, _tp_out_off, _epoch, _t_counter, _migration_blob, _pending = s.retrieve_snapshot(
            0,
            [("users", 0)],
        )
        assert data == {}
        assert s.snapshot_id == 1


# ---------------------------------------------------------------------------
# _iter_snapshot_files
# ---------------------------------------------------------------------------


class TestIterSnapshotFiles:
    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_filters_by_max_id(self, mock_mk):
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/op/0/1.bin"},
                    {"Key": "data/op/0/3.bin"},
                    {"Key": "data/op/0/5.bin"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = paginator

        s = _snap()
        result = s._iter_snapshot_files(mock_s3, "data/op/0/", max_snapshot_id=3)
        assert len(result) == 2
        assert result[0] == (1, "data/op/0/1.bin")
        assert result[1] == (3, "data/op/0/3.bin")

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_sorted_by_id(self, mock_mk):
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/op/0/3.bin"},
                    {"Key": "data/op/0/1.bin"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = paginator

        s = _snap()
        result = s._iter_snapshot_files(mock_s3, "data/op/0/", max_snapshot_id=10)
        assert result[0][0] < result[1][0]


# ---------------------------------------------------------------------------
# _list_bin_keys
# ---------------------------------------------------------------------------


class TestListBinKeys:
    def test_filters_bin_files(self):
        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/op/0/1.bin"},
                    {"Key": "data/op/0/1.txt"},
                    {"Key": "data/op/0/2.bin"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = paginator

        s = _snap()
        result = s._list_bin_keys(mock_s3, "data/op/0/")
        assert result == ["data/op/0/1.bin", "data/op/0/2.bin"]

    def test_empty_contents(self):
        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{}]
        mock_s3.get_paginator.return_value = paginator

        s = _snap()
        result = s._list_bin_keys(mock_s3, "data/op/0/")
        assert result == []


# ---------------------------------------------------------------------------
# _load_operator_state — tombstone handling
# ---------------------------------------------------------------------------


class TestLoadOperatorStateTombstones:
    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_tombstone_removes_key_from_previous_delta(self, mock_mk):
        """A None value in a subsequent delta should delete the key."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        # Simulate two snapshot files for the same partition
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/users/0/1.bin"},  # base snapshot
                    {"Key": "data/users/0/2.bin"},  # delta with tombstone
                ]
            }
        ]
        mock_s3.get_paginator.return_value = paginator

        # First call returns base data, second returns delta with tombstone
        call_count = [0]

        def fake_get_zstd_msgpack(_s3_client, _key):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"key_a": "val_a", "key_b": "val_b"}
            return {"key_b": None, "key_c": "val_c"}  # tombstone key_b, add key_c

        s._get_zstd_msgpack = fake_get_zstd_msgpack

        data = s._load_operator_state(mock_s3, 2, [("users", 0)])
        assert "key_a" in data[("users", 0)]
        assert "key_b" not in data[("users", 0)]  # tombstoned
        assert data[("users", 0)]["key_c"] == "val_c"

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_first_delta_filters_tombstones(self, mock_mk):
        """First delta for a partition should filter out None values."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "data/users/0/1.bin"}]}]
        mock_s3.get_paginator.return_value = paginator

        s._get_zstd_msgpack = lambda _s3_client, _key: {"key_a": "val_a", "key_b": None}

        data = s._load_operator_state(mock_s3, 1, [("users", 0)])
        assert "key_a" in data[("users", 0)]
        assert "key_b" not in data[("users", 0)]


# ---------------------------------------------------------------------------
# _load_sequencer_state — 5-element and 4-element tuple formats
# ---------------------------------------------------------------------------


class TestLoadSequencerStateTupleFormats:
    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_5_element_tuple_with_migration_blob(self, mock_mk):
        """Sequencer file with migration checkpoint blob (5 elements)."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "sequencer/1.bin"}]}]
        mock_s3.get_paginator.return_value = paginator

        blob = b"migration_data"
        s._get_zstd_msgpack = lambda _s3_client, _key: (
            {("users", 0): 10},
            {("users", 0): 5},
            3,
            42,
            blob,
        )

        tp_off, tp_out, epoch, t_counter, migration_blob, pending = s._load_sequencer_state(mock_s3, 1)
        assert tp_off == {("users", 0): 10}
        assert tp_out == {("users", 0): 5}
        assert epoch == 3
        assert t_counter == 42
        assert migration_blob == blob
        assert pending == []

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_6_element_tuple_with_pending_requests(self, mock_mk):
        """Sequencer file with the requests behind its offsets that had not committed."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "sequencer/1.bin"}, {"Key": "sequencer/2.bin"}]}]
        mock_s3.get_paginator.return_value = paginator

        files = {
            "sequencer/1.bin": ({("users", 0): 9}, {}, 2, 40, None, [[b"old", "k", "users", 0, "f", [], 8, 0]]),
            "sequencer/2.bin": ({("users", 0): 10}, {}, 3, 42, None, [[b"rq", "k", "users", 0, "f", [], 9, 0]]),
        }
        s._get_zstd_msgpack = lambda _s3_client, key: files[key]

        tp_off, _tp_out, epoch, _t, migration_blob, pending = s._load_sequencer_state(mock_s3, 2)
        assert (tp_off, epoch, migration_blob) == ({("users", 0): 10}, 3, None)
        # Only the recovered snapshot's own pending requests.
        assert pending == [[b"rq", "k", "users", 0, "f", [], 9, 0]]

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_4_element_tuple_legacy_format(self, mock_mk):
        """Legacy sequencer file without migration blob (4 elements)."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "sequencer/1.bin"}]}]
        mock_s3.get_paginator.return_value = paginator

        s._get_zstd_msgpack = lambda _s3_client, _key: (
            {("users", 0): 10},
            {("users", 0): 5},
            3,
            42,
        )

        tp_off, tp_out, epoch, t_counter, migration_blob, pending = s._load_sequencer_state(mock_s3, 1)
        assert tp_off == {("users", 0): 10}
        assert tp_out == {("users", 0): 5}
        assert epoch == 3
        assert t_counter == 42
        assert migration_blob is None
        assert pending == []

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_5_element_with_none_migration_blob(self, mock_mk):
        """5-element tuple with None migration blob (normal snapshot after migration cleared)."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "sequencer/1.bin"}]}]
        mock_s3.get_paginator.return_value = paginator

        s._get_zstd_msgpack = lambda _s3_client, _key: (
            {("users", 0): 10},
            {("users", 0): 5},
            3,
            42,
            None,
        )

        *_, migration_blob, _pending = s._load_sequencer_state(mock_s3, 1)
        assert migration_blob is None

    @patch("worker.fault_tolerance.async_snapshots._get_s3_client")
    def test_no_sequencer_files_returns_defaults(self, mock_mk):
        """When no sequencer files exist, defaults are returned."""
        mock_s3 = MagicMock()
        mock_mk.return_value = mock_s3

        s = _snap()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = paginator

        tp_off, tp_out, epoch, t_counter, migration_blob, pending = s._load_sequencer_state(mock_s3, 1)
        assert tp_off == {}
        assert tp_out == {}
        assert epoch == 0
        assert t_counter == 0
        assert migration_blob is None
        assert pending == []


@patch("worker.fault_tolerance.async_snapshots.socket")
def test_checkpoint_flag_is_reported(mock_socket):
    from styx.common.tcp_networking import NetworkingManager

    s = _snap(snapshot_id=7)
    s.start_snapshotting({}, {}, 1, 1, n_files=0, is_migration_checkpoint=True)
    msg = mock_socket.socket.return_value.send.call_args.args[0]
    assert NetworkingManager.decode_message(msg[8:])[9] is True

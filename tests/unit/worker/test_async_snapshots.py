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
        assert s.completed_snapshots == 0
        assert s.snapshot_in_progress is False
        assert s.snapshot_start == 0.0
        assert s.current_input_offsets is None
        assert s.current_output_offsets is None
        assert s.current_epoch_counter == -1
        assert s.current_t_counter == -1
        assert s.total_snapshot_size == 0

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
# register_size
# ---------------------------------------------------------------------------


class TestRegisterSize:
    def test_accumulates(self):
        s = _snap()
        s.register_size(100)
        s.register_size(200)
        assert s.total_snapshot_size == 300


# ---------------------------------------------------------------------------
# start_snapshotting
# ---------------------------------------------------------------------------


class TestStartSnapshotting:
    async def test_sets_fields(self):
        s = _snap()
        offsets_in = {("op", 0): 10}
        offsets_out = {("op", 0): 5}
        s.start_snapshotting(offsets_in, offsets_out, current_epoch_counter=3, current_t_counter=42)
        assert s.snapshot_in_progress is True
        assert s.snapshot_start > 0
        assert s.current_input_offsets == offsets_in
        assert s.current_output_offsets == offsets_out
        assert s.current_epoch_counter == 3
        assert s.current_t_counter == 42


# ---------------------------------------------------------------------------
# snapshot_completed_callback
# ---------------------------------------------------------------------------


class TestSnapshotCompletedCallback:
    def test_increments_completed(self):
        s = _snap(n_partitions=4)
        s.start_snapshotting({}, {}, 1, 1)
        s.snapshot_completed_callback(MagicMock())
        assert s.completed_snapshots == 1
        assert s.snapshot_in_progress is True  # not done yet

    @patch("worker.fault_tolerance.async_snapshots.socket")
    def test_all_completed_sends_message(self, mock_socket):
        mock_conn = MagicMock()
        mock_socket.socket.return_value = mock_conn

        s = _snap(n_partitions=2, snapshot_id=5)
        s.start_snapshotting({("op", 0): 1}, {("op", 0): 0}, 10, 100)

        s.snapshot_completed_callback(MagicMock())
        assert s.snapshot_in_progress is True

        s.snapshot_completed_callback(MagicMock())
        assert s.snapshot_in_progress is False
        assert s.completed_snapshots == 0
        assert s.snapshot_id == 6  # incremented
        assert s.total_snapshot_size == 0

        mock_conn.connect.assert_called_once()
        mock_conn.send.assert_called_once()
        mock_conn.close.assert_called_once()


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
        data, tp_off, tp_out_off, epoch, t_counter, migration_blob = s.retrieve_snapshot(-1, [])
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
        data, _tp_off, _tp_out_off, _epoch, _t_counter, _migration_blob = s.retrieve_snapshot(
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

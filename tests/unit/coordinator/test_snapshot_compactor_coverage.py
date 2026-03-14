"""Additional coverage tests for coordinator/snapshot_compactor.py

Covers: s3_get_bytes, s3_put_bytes, compact_deltas, cleanup_compacted_files,
start_snapshot_compaction.
"""

from unittest.mock import MagicMock, patch

from styx.common.serialization import zstd_msgpack_serialization

from coordinator.snapshot_compactor import (
    cleanup_compacted_files,
    compact_deltas,
    s3_get_bytes,
    s3_put_bytes,
    start_snapshot_compaction,
)

# ---------------------------------------------------------------------------
# s3_get_bytes / s3_put_bytes
# ---------------------------------------------------------------------------


class TestS3Helpers:
    def test_s3_get_bytes(self):
        mock_body = MagicMock()
        mock_body.read.return_value = b"data"
        mock_client = MagicMock()
        mock_client.get_object.return_value = {"Body": mock_body}

        result = s3_get_bytes(mock_client, "bucket", "key")
        assert result == b"data"
        mock_client.get_object.assert_called_once_with(Bucket="bucket", Key="key")

    def test_s3_put_bytes(self):
        mock_client = MagicMock()
        s3_put_bytes(mock_client, "bucket", "key", b"data")
        mock_client.put_object.assert_called_once_with(Bucket="bucket", Key="key", Body=b"data")


# ---------------------------------------------------------------------------
# cleanup_compacted_files
# ---------------------------------------------------------------------------


class TestCleanupCompactedFiles:
    def test_deletes_all_files(self):
        mock_client = MagicMock()
        cleanup_compacted_files(
            mock_client,
            ["sequencer/1.bin", "sequencer/2.bin"],
            ["data/op/0/1.bin", "data/op/0/2.bin"],
        )
        assert mock_client.delete_object.call_count == 4

    def test_empty_lists_no_deletes(self):
        mock_client = MagicMock()
        cleanup_compacted_files(mock_client, [], [])
        mock_client.delete_object.assert_not_called()


# ---------------------------------------------------------------------------
# compact_deltas
# ---------------------------------------------------------------------------


class TestCompactDeltas:
    def test_compact_data_snapshots(self):
        mock_client = MagicMock()
        # Two data snapshots for same partition
        files = ["data/users/0/0.bin", "data/users/0/1.bin"]

        # Serialize some test data
        data0 = zstd_msgpack_serialization({"k1": "v1"})
        data1 = zstd_msgpack_serialization({"k2": "v2"})

        def get_bytes_side_effect(Bucket=None, Key=None):
            body = MagicMock()
            if Key == "data/users/0/0.bin":
                body.read.return_value = data0
            else:
                body.read.return_value = data1
            return {"Body": body}

        mock_client.get_object.side_effect = get_bytes_side_effect
        compact_deltas(mock_client, files, max_snap_id=5)

        # Should write compacted data
        put_calls = mock_client.put_object.call_args_list
        assert any("data/users/0/0.bin" in str(c) for c in put_calls)

    def test_compact_sequencer_snapshots(self):
        mock_client = MagicMock()
        files = ["sequencer/0.bin", "sequencer/1.bin", "sequencer/2.bin"]

        seq_data = zstd_msgpack_serialization(({}, {}, 0, 0))

        def get_bytes_side_effect(Bucket=None, Key=None):
            body = MagicMock()
            body.read.return_value = seq_data
            return {"Body": body}

        mock_client.get_object.side_effect = get_bytes_side_effect
        compact_deltas(mock_client, files, max_snap_id=5)

        # Should write the latest sequencer snapshot as 0.bin
        put_calls = mock_client.put_object.call_args_list
        assert any("sequencer/0.bin" in str(c) for c in put_calls)

    def test_compact_empty_no_crash(self):
        mock_client = MagicMock()
        compact_deltas(mock_client, [], max_snap_id=5)
        mock_client.delete_object.assert_not_called()


# ---------------------------------------------------------------------------
# start_snapshot_compaction
# ---------------------------------------------------------------------------


class TestStartSnapshotCompaction:
    def test_lists_and_compacts(self):
        with patch("coordinator.snapshot_compactor.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client

            mock_paginator = MagicMock()
            mock_client.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "data/users/0/0.bin"},
                        {"Key": "data/users/0/1.bin"},
                        {"Key": "sequencer/0.bin"},
                        {"Key": "some_file.txt"},  # not .bin-like but has contents
                    ]
                }
            ]

            # Mock s3 reads for compact_deltas
            data0 = zstd_msgpack_serialization({"k1": "v1"})
            data1 = zstd_msgpack_serialization({"k2": "v2"})

            def get_side_effect(Bucket=None, Key=None):
                body = MagicMock()
                body.read.return_value = data0 if "0.bin" in Key else data1
                return {"Body": body}

            mock_client.get_object.side_effect = get_side_effect

            start_snapshot_compaction(max_snap_id=5)

            mock_boto3.client.assert_called_once()
            mock_client.get_paginator.assert_called_once_with("list_objects_v2")

    def test_filters_non_bin_files(self):
        with patch("coordinator.snapshot_compactor.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client

            mock_paginator = MagicMock()
            mock_client.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "readme.md"},
                        {"Key": "config.yaml"},
                    ]
                }
            ]

            start_snapshot_compaction(max_snap_id=5)
            # No .bin files → compact_deltas called with empty list
            mock_client.get_object.assert_not_called()

    def test_handles_empty_pages(self):
        with patch("coordinator.snapshot_compactor.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client

            mock_paginator = MagicMock()
            mock_client.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [{}]  # no Contents key

            start_snapshot_compaction(max_snap_id=5)
            mock_client.get_object.assert_not_called()

"""Integration tests for AsyncSnapshotsS3 against a real MinIO container."""

import pytest
from styx.common.serialization import zstd_msgpack_serialization

pytestmark = pytest.mark.integration


class TestStoreSnapshot:
    def test_store_snapshot_writes_to_s3(self, minio_bucket):
        """store_snapshot() should write data retrievable via boto3."""
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        data = zstd_msgpack_serialization({"key1": "value1", "key2": "value2"})
        snapshot_name = "data/store_test_op/0/1.bin"
        result = AsyncSnapshotsS3.store_snapshot(snapshot_name, data)
        assert result is True

        resp = minio_bucket.get_object(Bucket="styx-snapshots", Key=snapshot_name)
        assert resp["Body"].read() == data

    def test_store_multiple_snapshots(self, minio_bucket):
        """Multiple snapshots can be stored under different keys."""
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        for sid in range(1, 4):
            data = zstd_msgpack_serialization({"epoch": sid})
            AsyncSnapshotsS3.store_snapshot(f"data/multi_op/0/{sid}.bin", data)

        for sid in range(1, 4):
            resp = minio_bucket.get_object(Bucket="styx-snapshots", Key=f"data/multi_op/0/{sid}.bin")
            assert resp["Body"].read() is not None


class TestRetrieveSnapshot:
    def test_retrieve_snapshot_negative_id_returns_empty(self, minio_bucket):
        """retrieve_snapshot(-1) should return empty data."""
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        snap = AsyncSnapshotsS3(worker_id=0, n_assigned_partitions=1)
        data, off_in, off_out, ep, tc = snap.retrieve_snapshot(-1, [("any_op", 0)])
        assert data == {}
        assert off_in == {}
        assert off_out == {}
        assert ep == 0
        assert tc == 0

    def test_store_and_retrieve_round_trip(self, minio_bucket):
        """store_snapshot + retrieve_snapshot should round-trip operator and sequencer state."""
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        operator_name = "roundtrip_op"
        partition = 0
        snapshot_id = 1

        # Store operator state
        state_data = {"user_1": {"name": "Alice"}, "user_2": {"name": "Bob"}}
        sn_data = zstd_msgpack_serialization(state_data)
        AsyncSnapshotsS3.store_snapshot(f"data/{operator_name}/{partition}/{snapshot_id}.bin", sn_data)

        # Store sequencer state
        tp_offsets = {(operator_name, partition): 10}
        tp_out_offsets = {(operator_name, partition): 5}
        epoch = 3
        t_counter = 42
        seq_data = zstd_msgpack_serialization((tp_offsets, tp_out_offsets, epoch, t_counter))
        AsyncSnapshotsS3.store_snapshot(f"sequencer/{snapshot_id}.bin", seq_data)

        # Retrieve
        snap = AsyncSnapshotsS3(worker_id=0, n_assigned_partitions=1)
        data, _off_in, _off_out, ep, tc = snap.retrieve_snapshot(snapshot_id, [(operator_name, partition)])
        assert data[(operator_name, partition)] == state_data
        assert ep == epoch
        assert tc == t_counter

    def test_retrieve_merges_incremental_snapshots(self, minio_bucket):
        """Incremental snapshots (same prefix, increasing IDs) should be merged."""
        from worker.fault_tolerance.async_snapshots import AsyncSnapshotsS3

        op = "incr_op"
        partition = 0

        # Snapshot 1: initial state
        AsyncSnapshotsS3.store_snapshot(
            f"data/{op}/{partition}/1.bin",
            zstd_msgpack_serialization({"a": 1, "b": 2}),
        )
        # Snapshot 2: delta
        AsyncSnapshotsS3.store_snapshot(
            f"data/{op}/{partition}/2.bin",
            zstd_msgpack_serialization({"b": 20, "c": 3}),
        )
        # Sequencer for snapshot 2
        AsyncSnapshotsS3.store_snapshot(
            "sequencer/1.bin",
            zstd_msgpack_serialization(({(op, 0): 5}, {(op, 0): 3}, 2, 10)),
        )
        AsyncSnapshotsS3.store_snapshot(
            "sequencer/2.bin",
            zstd_msgpack_serialization(({(op, 0): 10}, {(op, 0): 7}, 4, 20)),
        )

        snap = AsyncSnapshotsS3(worker_id=0, n_assigned_partitions=1)
        data, _off_in, _off_out, ep, tc = snap.retrieve_snapshot(2, [(op, partition)])

        # b should be overwritten by snapshot 2
        assert data[(op, partition)]["a"] == 1
        assert data[(op, partition)]["b"] == 20
        assert data[(op, partition)]["c"] == 3
        # Sequencer state should be from latest
        assert ep == 4
        assert tc == 20


class TestIterSnapshotFiles:
    def test_filters_by_max_snapshot_id(self, minio_bucket):
        """_iter_snapshot_files should only return files up to max_snapshot_id."""
        from worker.fault_tolerance.async_snapshots import (
            AsyncSnapshotsS3,
            _get_s3_client,
        )

        prefix = "data/iter_op/0/"
        for sid in [1, 2, 3, 5]:
            data = zstd_msgpack_serialization({"k": f"v{sid}"})
            minio_bucket.put_object(Bucket="styx-snapshots", Key=f"{prefix}{sid}.bin", Body=data)

        snap = AsyncSnapshotsS3(worker_id=0)
        s3 = _get_s3_client()
        result = snap._iter_snapshot_files(s3, prefix, max_snapshot_id=3)

        assert len(result) == 3
        ids = [pair[0] for pair in result]
        assert ids == [1, 2, 3]

    def test_returns_sorted(self, minio_bucket):
        """Results should be sorted by snapshot ID ascending."""
        from worker.fault_tolerance.async_snapshots import (
            AsyncSnapshotsS3,
            _get_s3_client,
        )

        prefix = "data/sort_op/0/"
        for sid in [5, 1, 3]:
            minio_bucket.put_object(
                Bucket="styx-snapshots",
                Key=f"{prefix}{sid}.bin",
                Body=zstd_msgpack_serialization({"s": sid}),
            )

        snap = AsyncSnapshotsS3(worker_id=0)
        s3 = _get_s3_client()
        result = snap._iter_snapshot_files(s3, prefix, max_snapshot_id=10)
        ids = [pair[0] for pair in result]
        assert ids == [1, 3, 5]


class TestListBinKeys:
    def test_only_returns_bin_files(self, minio_bucket):
        """_list_bin_keys should filter out non-.bin files."""
        from worker.fault_tolerance.async_snapshots import (
            AsyncSnapshotsS3,
            _get_s3_client,
        )

        prefix = "data/binfilter_op/0/"
        minio_bucket.put_object(Bucket="styx-snapshots", Key=f"{prefix}1.bin", Body=b"data")
        minio_bucket.put_object(Bucket="styx-snapshots", Key=f"{prefix}2.txt", Body=b"text")
        minio_bucket.put_object(Bucket="styx-snapshots", Key=f"{prefix}3.bin", Body=b"data")

        snap = AsyncSnapshotsS3(worker_id=0)
        s3 = _get_s3_client()
        keys = snap._list_bin_keys(s3, prefix)

        assert len(keys) == 2
        assert all(k.endswith(".bin") for k in keys)

    def test_empty_prefix_returns_empty(self, minio_bucket):
        """Listing a non-existent prefix should return empty."""
        from worker.fault_tolerance.async_snapshots import (
            AsyncSnapshotsS3,
            _get_s3_client,
        )

        snap = AsyncSnapshotsS3(worker_id=0)
        s3 = _get_s3_client()
        keys = snap._list_bin_keys(s3, "data/nonexistent_op_xyz/0/")
        assert keys == []

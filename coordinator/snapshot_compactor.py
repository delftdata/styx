from collections import defaultdict
import logging
import os
from typing import TYPE_CHECKING

import boto3
from styx.common.serialization import (
    zstd_msgpack_deserialization,
    zstd_msgpack_serialization,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient as S3Client
    from styx.common.types import KVPairs, OperatorPartition

# Keep your existing env var names for minimal diff
S3_ENDPOINT: str = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY: str = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY: str = os.environ["S3_SECRET_KEY"]
S3_REGION: str = os.getenv("S3_REGION", "us-east-1")

SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")


def get_snapshots_per_worker(
    snapshot_files: list[str],
    max_snap_id: int,
) -> tuple[dict[OperatorPartition, list[tuple[int, str]]], list[tuple[int, str]]]:
    snapshots_per_operator_partition: dict[OperatorPartition, list[tuple[int, str]]] = defaultdict(list)
    sequencer_snapshots: list[tuple[int, str]] = []

    for sn_file in snapshot_files:
        sn_file_parts: list[str] = sn_file.split("/")
        sn_type: str = sn_file_parts[0]  # sequencer, data or migration

        if sn_type == "sequencer":
            snapshot_id = int(sn_file_parts[1].split(".")[0])
            if snapshot_id <= max_snap_id:
                sequencer_snapshots.append((snapshot_id, sn_file))

        elif sn_type == "data":
            operator_name, partition, snapshot_id = (
                sn_file_parts[1],
                int(sn_file_parts[2]),
                int(sn_file_parts[3].split(".")[0]),
            )
            if snapshot_id <= max_snap_id:
                snapshots_per_operator_partition[(operator_name, partition)].append((snapshot_id, sn_file))

    snapshots_per_operator_partition = {
        operator_partition: sn_info
        for operator_partition, sn_info in sorted(
            snapshots_per_operator_partition.items(),
            key=lambda item: item[1][0],
        )
        if len(sn_info) > 1
    }
    sequencer_snapshots = sorted(sequencer_snapshots, key=lambda item: item[0])
    return snapshots_per_operator_partition, sequencer_snapshots


def s3_get_bytes(s3_client: S3Client, bucket: str, key: str) -> bytes:
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def s3_put_bytes(s3_client: S3Client, bucket: str, key: str, data: bytes) -> None:
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)


def compact_deltas(
    s3_client: S3Client,
    snapshot_files: list[str],
    max_snap_id: int,
) -> None:
    snapshots_per_operator_partition, sequencer_snapshots = get_snapshots_per_worker(snapshot_files, max_snap_id)
    logging.warning("Compacting data snapshots: %s", snapshots_per_operator_partition)
    logging.warning("Compacting sequencer snapshots: %s", sequencer_snapshots)

    # Sequencer
    sequencer_snapshots_to_delete: list[str] = []
    if len(sequencer_snapshots) > 1:
        latest_sequencer_snapshot = sequencer_snapshots.pop()
        last_sequencer_data = s3_get_bytes(s3_client, SNAPSHOT_BUCKET_NAME, latest_sequencer_snapshot[1])

        # the last becomes the first
        s3_put_bytes(s3_client, SNAPSHOT_BUCKET_NAME, "sequencer/0.bin", last_sequencer_data)

        sequencer_snapshots_to_delete = [s_sn for _, s_sn in sequencer_snapshots if s_sn != "sequencer/0.bin"]

    # Partition Data
    data: dict[OperatorPartition, KVPairs] = {}
    snapshots_to_delete: list[str] = []

    for operator_partition, sn_info in snapshots_per_operator_partition.items():
        for _, sn_name in sn_info:
            partition_data = zstd_msgpack_deserialization(
                s3_get_bytes(s3_client, SNAPSHOT_BUCKET_NAME, sn_name),
            )
            if operator_partition in data:
                snapshots_to_delete.append(sn_name)
                if partition_data:
                    data[operator_partition].update(partition_data)
            else:
                data[operator_partition] = partition_data

    for operator_partition, partition_data in data.items():
        operator_name, partition = operator_partition
        sn_data: bytes = zstd_msgpack_serialization(partition_data)
        snapshot_name: str = f"data/{operator_name}/{partition}/0.bin"

        # Store the primary snapshot after compaction
        s3_put_bytes(s3_client, SNAPSHOT_BUCKET_NAME, snapshot_name, sn_data)

    # Cleanup the compacted deltas
    logging.warning("Deleting data snapshots: %s", snapshots_to_delete)
    logging.warning("Deleting sequencer snapshots: %s", sequencer_snapshots_to_delete)
    cleanup_compacted_files(s3_client, sequencer_snapshots_to_delete, snapshots_to_delete)


def cleanup_compacted_files(
    s3_client: S3Client,
    sequencer_files_to_remove: list[str],
    data_files_to_remove: list[str],
) -> None:
    for key in sequencer_files_to_remove:
        s3_client.delete_object(Bucket=SNAPSHOT_BUCKET_NAME, Key=key)
    for key in data_files_to_remove:
        s3_client.delete_object(Bucket=SNAPSHOT_BUCKET_NAME, Key=key)


def start_snapshot_compaction(max_snap_id: int) -> None:
    s3_client: S3Client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )

    # List all *.bin objects recursively
    paginator = s3_client.get_paginator("list_objects_v2")
    snapshot_files: list[str] = []
    for page in paginator.paginate(Bucket=SNAPSHOT_BUCKET_NAME):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".bin"):
                snapshot_files.append(key)

    compact_deltas(s3_client, snapshot_files, max_snap_id)

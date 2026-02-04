from collections import defaultdict
import io
import logging
import os
from typing import TYPE_CHECKING

from minio import Minio
from styx.common.serialization import (
    zstd_msgpack_deserialization,
    zstd_msgpack_serialization,
)

if TYPE_CHECKING:
    from styx.common.types import KVPairs, OperatorPartition

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY: str = os.environ["MINIO_ROOT_PASSWORD"]
SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")


def get_snapshots_per_worker(
    snapshot_files: list[str],
    max_snap_id: int,
) -> tuple[dict[OperatorPartition, list[tuple[int, str]]], list[tuple[int, str]]]:
    snapshots_per_operator_partition: dict[OperatorPartition, list[tuple[int, str]]] = defaultdict(list)
    sequencer_snapshots: list[tuple[int, str]] = []
    for sn_file in snapshot_files:
        sn_file_parts: list[str] = sn_file.split("/")
        sn_type: str = sn_file_parts[
            0
        ]  # sequencer, data or migration (we should never include migration to the compaction)
        if sn_type == "sequencer":
            # sequencer
            snapshot_id = int(sn_file_parts[1].split(".")[0])
            if snapshot_id <= max_snap_id:
                sequencer_snapshots.append((snapshot_id, sn_file))
        elif sn_type == "data":
            # data
            operator_name, partition, snapshot_id = (
                sn_file_parts[1],
                int(sn_file_parts[2]),
                int(sn_file_parts[3].split(".")[0]),
            )
            if snapshot_id <= max_snap_id:
                snapshots_per_operator_partition[(operator_name, partition)].append(
                    (snapshot_id, sn_file),
                )

    snapshots_per_operator_partition = {
        operator_partition: sn_info
        # sort by snapshot id so that the merging order is correct
        for operator_partition, sn_info in sorted(
            snapshots_per_operator_partition.items(),
            key=lambda item: item[1][0],
        )
        if len(sn_info) > 1
    }
    # sort by snapshot id so that the merging order is correct
    sequencer_snapshots = sorted(sequencer_snapshots, key=lambda item: item[0])
    return snapshots_per_operator_partition, sequencer_snapshots


def compact_deltas(
    minio_client: Minio,
    snapshot_files: list[str],
    max_snap_id: int,
) -> None:
    snapshots_per_operator_partition, sequencer_snapshots = get_snapshots_per_worker(
        snapshot_files,
        max_snap_id,
    )
    logging.warning("Compacting data snapshots: %s", snapshots_per_operator_partition)
    logging.warning("Compacting sequencer snapshots: %s", sequencer_snapshots)

    # Sequencer
    sequencer_snapshots_to_delete: list[str] = []
    if len(sequencer_snapshots) > 1:
        # only makes sense to compact if we have more than 1 files
        latest_sequencer_snapshot = sequencer_snapshots.pop()
        last_sequencer_data = minio_client.get_object(
            SNAPSHOT_BUCKET_NAME,
            latest_sequencer_snapshot[1],
        ).data
        # the last becomes the first
        minio_client.put_object(
            SNAPSHOT_BUCKET_NAME,
            "sequencer/0.bin",
            io.BytesIO(last_sequencer_data),
            len(last_sequencer_data),
        )
        sequencer_snapshots_to_delete = [s_sn for _, s_sn in sequencer_snapshots if s_sn != "sequencer/0.bin"]
    # Partition Data
    data: dict[OperatorPartition, KVPairs] = {}
    snapshots_to_delete: list[str] = []
    for operator_partition, sn_info in snapshots_per_operator_partition.items():
        # The snapshots must be sorted
        for _, sn_name in sn_info:
            partition_data = zstd_msgpack_deserialization(
                minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data,
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
        minio_client.put_object(
            SNAPSHOT_BUCKET_NAME,
            snapshot_name,
            io.BytesIO(sn_data),
            len(sn_data),
        )

    # Cleanup the compacted deltas
    logging.warning("Deleting data snapshots: %s", snapshots_to_delete)
    logging.warning("Deleting sequencer snapshots: %s", sequencer_snapshots_to_delete)
    cleanup_compacted_files(
        minio_client,
        sequencer_snapshots_to_delete,
        snapshots_to_delete,
    )


def cleanup_compacted_files(
    minio_client: Minio,
    sequencer_files_to_remove: list[str],
    data_files_to_remove: list[str],
) -> None:
    for snapshot_to_delete in sequencer_files_to_remove:
        minio_client.remove_object(
            bucket_name=SNAPSHOT_BUCKET_NAME,
            object_name=snapshot_to_delete,
        )
    for snapshot_to_delete in data_files_to_remove:
        minio_client.remove_object(
            bucket_name=SNAPSHOT_BUCKET_NAME,
            object_name=snapshot_to_delete,
        )


def start_snapshot_compaction(max_snap_id: int) -> None:
    minio_client: Minio = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    snapshot_files: list[str] = [
        sn_file.object_name
        for sn_file in minio_client.list_objects(
            bucket_name=SNAPSHOT_BUCKET_NAME,
            recursive=True,
        )
        if sn_file.object_name.endswith(".bin")
    ]
    compact_deltas(minio_client, snapshot_files, max_snap_id)

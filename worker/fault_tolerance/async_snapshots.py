import os
import socket
import time
from typing import TYPE_CHECKING

import boto3
from styx.common.message_types import MessageType
from styx.common.serialization import Serializer, zstd_msgpack_deserialization
from styx.common.tcp_networking import NetworkingManager

from worker.fault_tolerance.base_snapshoter import BaseSnapshotter

if TYPE_CHECKING:
    from collections.abc import Iterable
    from concurrent.futures import Future

    from botocore.client import BaseClient as S3Client
    from styx.common.types import KVPairs, OperatorPartition

COORDINATOR_HOST: str = os.environ["DISCOVERY_HOST"]
COORDINATOR_PORT: int = int(os.environ["DISCOVERY_PORT"])

# Keep current env var names for minimal diff (works for RustFS too)
S3_ENDPOINT: str = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY: str = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY: str = os.environ["S3_SECRET_KEY"]
S3_REGION: str = os.getenv("S3_REGION", "us-east-1")

SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")
SEQUENCER_PREFIX = "sequencer/"

# Per-process cached S3 client (avoids re-creating boto3 client on every call)
_s3_client: S3Client | None = None


def _get_s3_client() -> S3Client:
    """Return a cached S3 client for the current process."""
    global _s3_client  # noqa: PLW0603
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
        )
    return _s3_client


def warm_s3_client() -> None:
    """Pre-warm the S3 client in a subprocess. Used as ProcessPoolExecutor initializer."""
    _get_s3_client()


class AsyncSnapshotsS3(BaseSnapshotter):
    def __init__(
        self,
        worker_id: int,
        n_assigned_partitions: int = 0,
        snapshot_id: int = 1,
    ) -> None:
        self.worker_id: int = worker_id
        self.snapshot_id: int = snapshot_id
        self.n_assigned_partitions: int = n_assigned_partitions
        self.completed_snapshots: int = 0
        self.snapshot_in_progress: bool = False
        self.snapshot_start: float = 0.0
        self.current_input_offsets = None
        self.current_output_offsets = None
        self.current_epoch_counter = -1
        self.current_t_counter = -1
        self.total_snapshot_size = 0

    def update_n_assigned_partitions(self, n_assigned_partitions: int) -> None:
        self.n_assigned_partitions = n_assigned_partitions

    def set_snapshot_id(self, snapshot_id: int) -> None:
        self.snapshot_id = snapshot_id + 1

    def snapshot_completed_callback(self, _: Future) -> None:
        self.completed_snapshots += 1
        if self.completed_snapshots == self.n_assigned_partitions:
            snapshot_end = time.time() * 1000
            msg = NetworkingManager.encode_message(
                msg=(
                    self.worker_id,
                    self.snapshot_id,
                    self.snapshot_start,
                    snapshot_end,
                    self.current_input_offsets,
                    self.current_output_offsets,
                    self.current_epoch_counter,
                    self.current_t_counter,
                    self.total_snapshot_size,
                ),
                msg_type=MessageType.SnapID,
                serializer=Serializer.MSGPACK,
            )
            s = socket.socket()
            s.connect((COORDINATOR_HOST, COORDINATOR_PORT))
            s.send(msg)
            s.close()
            self.snapshot_id += 1
            self.snapshot_in_progress = False
            self.completed_snapshots = 0
            self.total_snapshot_size = 0

    def start_snapshotting(
        self,
        current_input_offsets: dict[OperatorPartition, int],
        current_output_offsets: dict[OperatorPartition, int],
        current_epoch_counter: int,
        current_t_counter: int,
    ) -> None:
        self.snapshot_in_progress = True
        self.snapshot_start = time.time() * 1000
        self.current_input_offsets = current_input_offsets
        self.current_output_offsets = current_output_offsets
        self.current_epoch_counter = current_epoch_counter
        self.current_t_counter = current_t_counter

    def register_size(self, file_size_bytes: int) -> None:
        self.total_snapshot_size += file_size_bytes

    @staticmethod
    def store_snapshot(snapshot_name: str, sn_data: bytes) -> bool:
        _get_s3_client().put_object(
            Bucket=SNAPSHOT_BUCKET_NAME,
            Key=snapshot_name,
            Body=sn_data,
        )
        return True

    def retrieve_snapshot(
        self,
        snapshot_id: int,
        registered_operators: Iterable[OperatorPartition],
    ) -> tuple[
        dict[OperatorPartition, KVPairs],
        dict[OperatorPartition, int],
        dict[OperatorPartition, int],
        int,
        int,
    ]:
        self.snapshot_id = snapshot_id + 1
        if snapshot_id == -1:
            return {}, {}, {}, 0, 0

        s3 = _get_s3_client()
        data = self._load_operator_state(s3, snapshot_id, registered_operators)
        tp_offsets, tp_out_offsets, epoch, t_counter = self._load_sequencer_state(s3, snapshot_id)
        return data, tp_offsets, tp_out_offsets, epoch, t_counter

    def _load_operator_state(
        self,
        s3: S3Client,
        snapshot_id: int,
        registered_operators: Iterable[OperatorPartition],
    ) -> dict[OperatorPartition, KVPairs]:
        data: dict[OperatorPartition, KVPairs] = {}
        for operator_partition in registered_operators:
            operator_name, partition = operator_partition
            prefix = f"data/{operator_name}/{partition}/"

            for _, key in self._iter_snapshot_files(s3, prefix, snapshot_id):
                partition_data = self._get_zstd_msgpack(s3, key)
                if operator_partition in data and partition_data:
                    data[operator_partition].update(partition_data)
                else:
                    data[operator_partition] = partition_data
        return data

    def _load_sequencer_state(
        self,
        s3: S3Client,
        snapshot_id: int,
    ) -> tuple[dict[OperatorPartition, int], dict[OperatorPartition, int], int, int]:
        topic_partition_offsets: dict[OperatorPartition, int] = {}
        topic_partition_output_offsets: dict[OperatorPartition, int] = {}
        epoch = 0
        t_counter = 0

        for _, key in self._iter_snapshot_files(s3, SEQUENCER_PREFIX, snapshot_id):
            (
                topic_partition_offsets,
                topic_partition_output_offsets,
                epoch,
                t_counter,
            ) = self._get_zstd_msgpack(s3, key)

        return topic_partition_offsets, topic_partition_output_offsets, epoch, t_counter

    def _iter_snapshot_files(
        self,
        s3: S3Client,
        prefix: str,
        max_snapshot_id: int,
    ) -> list[tuple[int, str]]:
        keys = self._list_bin_keys(s3, prefix)
        pairs = []
        for key in keys:
            sn_id = int(key.split("/")[-1].split(".")[0])
            if sn_id <= max_snapshot_id:
                pairs.append((sn_id, key))
        pairs.sort(key=lambda item: item[0])
        return pairs

    def _list_bin_keys(self, s3: S3Client, prefix: str) -> list[str]:
        paginator = s3.get_paginator("list_objects_v2")
        out: list[str] = []
        for page in paginator.paginate(Bucket=SNAPSHOT_BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".bin"):
                    out.append(key)
        return out

    def _get_zstd_msgpack(self, s3: S3Client, key: str) -> object | dict:
        resp = s3.get_object(Bucket=SNAPSHOT_BUCKET_NAME, Key=key)
        return zstd_msgpack_deserialization(resp["Body"].read())

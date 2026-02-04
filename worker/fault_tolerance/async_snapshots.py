import io
import os
import socket
import time
from typing import TYPE_CHECKING

from minio import Minio
from styx.common.message_types import MessageType
from styx.common.serialization import Serializer, zstd_msgpack_deserialization
from styx.common.tcp_networking import NetworkingManager

from worker.fault_tolerance.base_snapshoter import BaseSnapshotter

if TYPE_CHECKING:
    from collections.abc import Iterable
    from concurrent.futures import Future

    from styx.common.types import KVPairs, OperatorPartition

COORDINATOR_HOST: str = os.environ["DISCOVERY_HOST"]
COORDINATOR_PORT: int = int(os.environ["DISCOVERY_PORT"])
MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY: str = os.environ["MINIO_ROOT_PASSWORD"]
SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")


class AsyncSnapshotsMinio(BaseSnapshotter):
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
        minio_client: Minio = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        minio_client.put_object(
            SNAPSHOT_BUCKET_NAME,
            snapshot_name,
            io.BytesIO(sn_data),
            len(sn_data),
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

        minio_client: Minio = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        data: dict[OperatorPartition, KVPairs] = {}

        for operator_partition in registered_operators:
            operator_name, partition = operator_partition
            # Get the delta files in the correct order 0 <- 1 <- 2 ...
            snapshot_files: list[str] = [
                sn_file.object_name
                for sn_file in minio_client.list_objects(
                    bucket_name=SNAPSHOT_BUCKET_NAME,
                    prefix=f"data/{operator_name}/{partition}/",
                    recursive=True,
                )
                if sn_file.object_name.endswith(".bin")
            ]
            snapshot_merge_order: list[tuple[int, str]] = sorted(
                [(int(snapshot_file.split("/")[-1].split(".")[0]), snapshot_file) for snapshot_file in snapshot_files],
                key=lambda item: item[0],
            )
            for sn_id, sn_name in snapshot_merge_order:
                if sn_id > snapshot_id:
                    # recover only from a stable snapshot
                    continue
                partition_data = zstd_msgpack_deserialization(
                    minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data,
                )
                if operator_partition in data and partition_data:
                    data[operator_partition].update(partition_data)
                else:
                    data[operator_partition] = partition_data
        # Sequencer
        snapshot_files: list[str] = [
            sn_file.object_name
            for sn_file in minio_client.list_objects(
                bucket_name=SNAPSHOT_BUCKET_NAME,
                prefix="sequencer/",
                recursive=True,
            )
            if sn_file.object_name.endswith(".bin")
        ]
        snapshot_merge_order: list[tuple[int, str]] = sorted(
            [(int(snapshot_file.split("/")[-1].split(".")[0]), snapshot_file) for snapshot_file in snapshot_files],
            key=lambda item: item[0],
        )
        topic_partition_offsets: dict[OperatorPartition, int] = {}
        topic_partition_output_offsets: dict[OperatorPartition, int] = {}
        epoch: int = 0
        t_counter: int = 0
        for sn_id, sn_name in snapshot_merge_order:
            if sn_id > snapshot_id:
                # recover only from a stable snapshot
                continue
            (
                topic_partition_offsets,
                topic_partition_output_offsets,
                epoch,
                t_counter,
            ) = zstd_msgpack_deserialization(
                minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data,
            )
        # The recovered snapshot will have the latest metadata and merged operator state
        return (
            data,
            topic_partition_offsets,
            topic_partition_output_offsets,
            epoch,
            t_counter,
        )

import os
import socket
import time
import io
from typing import Iterable

from minio import Minio
from styx.common.message_types import MessageType
from styx.common.types import OperatorPartition, KVPairs
from styx.common.tcp_networking import NetworkingManager

from styx.common.serialization import Serializer, msgpack_deserialization, msgpack_serialization

from worker.fault_tolerance.base_snapshoter import BaseSnapshotter

COORDINATOR_HOST: str = os.environ['DISCOVERY_HOST']
COORDINATOR_PORT: int = int(os.environ['DISCOVERY_PORT'])
MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")


class AsyncSnapshotsMinio(BaseSnapshotter):

    def __init__(self, worker_id, snapshot_id: int = 0):
        self.worker_id = worker_id
        self.snapshot_id = snapshot_id

    def snapshot_completed_callback(self, _):
        self.snapshot_id += 1

    @staticmethod
    def store_snapshot(snapshot_id: int,
                       worker_id: str,
                       data: dict[OperatorPartition, KVPairs],
                       input_offsets: dict[OperatorPartition, int],
                       epoch: int,
                       t_counter: int,
                       output_offsets: dict[OperatorPartition, int],
                       start):
        minio_client: Minio = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

        for operator_partition, operator_data in data.items():
            operator_name, partition = operator_partition
            sn_data: bytes = msgpack_serialization((input_offsets[operator_partition],
                                                    output_offsets[operator_partition],
                                                    operator_data))
            snapshot_name: str = f"data/{operator_name}/{partition}/{snapshot_id}.bin"
            minio_client.put_object(SNAPSHOT_BUCKET_NAME, snapshot_name, io.BytesIO(sn_data), len(sn_data))

        sn_data: bytes = msgpack_serialization((epoch, t_counter))
        snapshot_name: str = f"sequencer/{snapshot_id}.bin"
        minio_client.put_object(SNAPSHOT_BUCKET_NAME, snapshot_name, io.BytesIO(sn_data), len(sn_data))

        end = time.time()*1000
        msg = NetworkingManager.encode_message(msg=(worker_id, snapshot_id, start, end),
                                               msg_type=MessageType.SnapID,
                                               serializer=Serializer.MSGPACK)

        s = socket.socket()
        s.connect((COORDINATOR_HOST, COORDINATOR_PORT))
        s.send(msg)
        s.close()
        return True

    def retrieve_snapshot(self, snapshot_id: int, registered_operators: Iterable[OperatorPartition]):
        self.snapshot_id = snapshot_id + 1
        if snapshot_id == -1:
            return {}, None, None, 0, 0

        minio_client: Minio = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

        data: dict[OperatorPartition, KVPairs] = {}
        topic_partition_offsets: dict[OperatorPartition, int] = {}
        topic_partition_output_offsets: dict[OperatorPartition, int] = {}

        for operator_partition in registered_operators:
            operator_name, partition = operator_partition
            # Get the delta files in correct order 0 <- 1 <- 2 ...
            snapshot_files: list[str] = [sn_file.object_name
                                         for sn_file in minio_client.list_objects(bucket_name=SNAPSHOT_BUCKET_NAME,
                                                                                  prefix=f"data/{operator_name}/{partition}/",
                                                                                  recursive=True)
                                         if sn_file.object_name.endswith(".bin")]
            snapshot_merge_order: list[tuple[int, str]] = sorted([(int(snapshot_file.split("/")[-1].split(".")[0]),
                                                                   snapshot_file)
                                                                  for snapshot_file in snapshot_files],
                                                                 key=lambda item: item[0])
            input_offset: int = -1
            output_offset: int = -1
            for sn_id, sn_name in snapshot_merge_order:
                if sn_id > snapshot_id:
                    # recover only from a stable snapshot
                    continue
                input_offset, output_offset, partition_data = msgpack_deserialization(
                    minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data
                )
                if operator_partition in data and partition_data:
                    data[operator_partition].update(partition_data)
                else:
                    data[operator_partition] = partition_data
            topic_partition_offsets[operator_partition] = input_offset
            topic_partition_output_offsets[operator_partition] = output_offset
        # Sequencer
        snapshot_files: list[str] = [sn_file.object_name
                                     for sn_file in minio_client.list_objects(bucket_name=SNAPSHOT_BUCKET_NAME,
                                                                              prefix="sequencer/",
                                                                              recursive=True)
                                     if sn_file.object_name.endswith(".bin")]
        snapshot_merge_order: list[tuple[int, str]] = sorted([(int(snapshot_file.split("/")[-1].split(".")[0]),
                                                               snapshot_file)
                                                              for snapshot_file in snapshot_files],
                                                             key=lambda item: item[0])
        epoch: int = 0
        t_counter: int = 0
        for sn_id, sn_name in snapshot_merge_order:
            if sn_id > snapshot_id:
                # recover only from a stable snapshot
                continue
            epoch, t_counter = msgpack_deserialization(minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data)
        # The recovered snapshot will have the latest metadata and merged operator state
        return data, topic_partition_offsets, topic_partition_output_offsets, epoch, t_counter

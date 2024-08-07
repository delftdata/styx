import os
import socket
import time
import io

from minio import Minio
from styx.common.message_types import MessageType
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
                       data: dict,
                       start):
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        bytes_file: bytes = msgpack_serialization(data)
        snapshot_name: str = f"w{worker_id}/{snapshot_id}.bin"
        minio_client.put_object(SNAPSHOT_BUCKET_NAME, snapshot_name, io.BytesIO(bytes_file), len(bytes_file))
        end = time.time()*1000
        msg = NetworkingManager.encode_message(msg=(worker_id, snapshot_id, start, end),
                                               msg_type=MessageType.SnapID,
                                               serializer=Serializer.MSGPACK)

        s = socket.socket()
        s.connect((COORDINATOR_HOST, COORDINATOR_PORT))
        s.send(msg)
        s.close()
        return True

    def retrieve_snapshot(self, snapshot_id):
        self.snapshot_id = snapshot_id + 1
        if snapshot_id == -1:
            return {}, None
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        snapshot_files: list[str] = [sn_file.object_name
                                     for sn_file in minio_client.list_objects(bucket_name=SNAPSHOT_BUCKET_NAME,
                                                                              prefix=f"w{self.worker_id}/",
                                                                              recursive=True)
                                     if sn_file.object_name.endswith(".bin")]
        snapshot_merge_order: list[tuple[int, str]] = sorted([(int(snapshot_file.split("/")[1].split(".")[0]),
                                                               snapshot_file)
                                                              for snapshot_file in snapshot_files],
                                                             key=lambda item: item[0])
        data: dict[str, dict[any, any]] = {}
        metadata: dict[str, any] = {}
        for sn_id, sn_name in snapshot_merge_order:
            if sn_id > snapshot_id:
                # recover only from a stable snapshot
                continue
            deser = msgpack_deserialization(minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data)
            loaded_data = deser["data"]
            metadata = deser["metadata"]
            if data:
                for operator_name, operator_state in loaded_data.items():
                    data[operator_name].update(operator_state)
            else:
                data = loaded_data
        # The recovered snapshot will have the latest metadata and merged operator state
        return data, metadata

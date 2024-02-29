import io
import os
from collections import defaultdict

from minio import Minio
from styx.common.serialization import msgpack_deserialization, msgpack_serialization

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")


def get_snapshots_per_worker(snapshot_files: list[str], max_snap_id: int) -> dict[int, list[tuple[int, str]]]:
    snapshots_per_worker: dict[int, list[tuple[int, str]]] = defaultdict(list)
    for sn_file in snapshot_files:
        worker_id, snapshot_id = sn_file.split("/")
        worker_id = int(worker_id[1:])
        snapshot_id = int(snapshot_id.split(".")[0])
        if snapshot_id <= max_snap_id:
            snapshots_per_worker[worker_id].append((snapshot_id, sn_file))
    snapshots_per_worker = {worker_id: sn_info
                            for worker_id, sn_info in sorted(snapshots_per_worker.items(),
                                                             key=lambda item: item[1][0])
                            if len(sn_info) > 1}
    return snapshots_per_worker


def compact_deltas(minio_client, snapshot_files, max_snap_id):
    snapshots_per_worker = get_snapshots_per_worker(snapshot_files, max_snap_id)
    for worker_id, sn_info in snapshots_per_worker.items():
        data: dict[str, dict[any, any]] = {}
        metadata: dict[str, any] = {}
        snapshots_to_delete: list[str] = []
        # The snapshots must be sorted
        for sn_id, sn_name in sn_info:
            deser = msgpack_deserialization(minio_client.get_object(SNAPSHOT_BUCKET_NAME, sn_name).data)
            loaded_data = deser["data"]
            metadata = deser["metadata"]
            if data:
                snapshots_to_delete.append(sn_name)
                for operator_name, operator_state in loaded_data.items():
                    data[operator_name] |= operator_state
            else:
                data = loaded_data
        # The new snapshot will have the latest metadata and merged operator state
        bytes_file: bytes = msgpack_serialization({"data": data, "metadata": metadata})
        snapshot_name: str = f"w{worker_id}/0.bin"
        # Store the primary snapshot after compaction
        minio_client.put_object(SNAPSHOT_BUCKET_NAME, snapshot_name, io.BytesIO(bytes_file), len(bytes_file))
        # Cleanup the compacted deltas
        for snapshot_to_delete in snapshots_to_delete:
            minio_client.remove_object(bucket_name=SNAPSHOT_BUCKET_NAME, object_name=snapshot_to_delete)


def start_snapshot_compaction(max_snap_id: int):
    minio_client: Minio = Minio(
        MINIO_URL, access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY, secure=False
    )
    snapshot_files: list[str] = [sn_file.object_name
                                 for sn_file in minio_client.list_objects(bucket_name=SNAPSHOT_BUCKET_NAME,
                                                                          recursive=True)
                                 if sn_file.object_name.endswith(".bin")]
    compact_deltas(minio_client, snapshot_files, max_snap_id)

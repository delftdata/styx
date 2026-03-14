"""
Conftest for integration tests using testcontainers.

Environment variables are set at module level (before any worker or styx
imports) because several modules read env vars at import time.  Session-
scoped fixtures start real Kafka / MinIO containers and update the env
vars (+ reload affected modules) so the components-under-test connect to
the containers automatically.
"""

import importlib
import os
from pathlib import Path
import sys

import pytest

# ---- sys.path setup (same pattern as unit test conftest files) ----
_REPO_ROOT = str(Path(__file__).parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ---- Pre-set env vars with placeholders ----
# Must exist before worker modules are imported during pytest collection.
_INTEGRATION_DEFAULTS = {
    "DISCOVERY_HOST": "localhost",
    "DISCOVERY_PORT": "8888",
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "minioadmin",
    "S3_SECRET_KEY": "minioadmin",
    "S3_REGION": "us-east-1",
    "SNAPSHOT_BUCKET_NAME": "styx-snapshots",
    "KAFKA_URL": "localhost:9092",
    "USE_COMPOSITE_KEYS": "true",
}
for _k, _v in _INTEGRATION_DEFAULTS.items():
    os.environ.setdefault(_k, _v)


# ---------------------------------------------------------------------------
# MinIO fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def minio_container():
    """Start a MinIO container for the entire test session."""
    from testcontainers.minio import MinioContainer

    with MinioContainer() as minio:
        config = minio.get_config()
        endpoint = f"http://{config['endpoint']}"

        os.environ["S3_ENDPOINT"] = endpoint
        os.environ["S3_ACCESS_KEY"] = config["access_key"]
        os.environ["S3_SECRET_KEY"] = config["secret_key"]

        # Reload modules that read S3 env vars at import time
        import worker.fault_tolerance.async_snapshots as snap_mod

        importlib.reload(snap_mod)

        yield {
            "endpoint": endpoint,
            "access_key": config["access_key"],
            "secret_key": config["secret_key"],
            "region": "us-east-1",
        }


@pytest.fixture(scope="session")
def minio_bucket(minio_container):
    """Create the styx-snapshots bucket and yield a boto3 S3 client."""
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_container["endpoint"],
        aws_access_key_id=minio_container["access_key"],
        aws_secret_access_key=minio_container["secret_key"],
        region_name=minio_container["region"],
    )
    import contextlib

    bucket = os.environ.get("SNAPSHOT_BUCKET_NAME", "styx-snapshots")
    with contextlib.suppress(s3.exceptions.BucketAlreadyOwnedByYou):
        s3.create_bucket(Bucket=bucket)
    return s3


# ---------------------------------------------------------------------------
# Kafka fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_container():
    """Start a Kafka (KRaft) container for the entire test session."""
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer().with_kraft() as kafka:
        bootstrap = kafka.get_bootstrap_server()
        os.environ["KAFKA_URL"] = bootstrap

        # Reload modules that read KAFKA_URL at import time
        import worker.egress.styx_kafka_batch_egress as egress_mod

        importlib.reload(egress_mod)

        yield {"bootstrap_server": bootstrap}


def _create_topic(bootstrap_server: str, topic_name: str, num_partitions: int = 1) -> None:
    """Helper to create a Kafka topic using confluent_kafka (already a dependency)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_server})
    fs = admin.create_topics([NewTopic(topic_name, num_partitions=num_partitions, replication_factor=1)])
    import contextlib

    for f in fs.values():
        with contextlib.suppress(Exception):
            f.result()


@pytest.fixture
def create_topic(kafka_container):
    """Factory fixture: call create_topic(name, partitions) to make a Kafka topic."""
    created: list[str] = []

    def _factory(topic_name: str, num_partitions: int = 1) -> str:
        _create_topic(kafka_container["bootstrap_server"], topic_name, num_partitions)
        created.append(topic_name)
        return topic_name

    return _factory

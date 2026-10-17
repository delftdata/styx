"""
Conftest for integration tests using testcontainers.

Environment variables are set at module level (before any worker or styx
imports) because several modules read env vars at import time.  Session-
scoped fixtures start real Kafka / S3 (RustFS) containers and update the env
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
    "S3_ACCESS_KEY": "rustfsadmin",
    "S3_SECRET_KEY": "rustfsadmin",
    "S3_REGION": "us-east-1",
    "SNAPSHOT_BUCKET_NAME": "styx-snapshots",
    "KAFKA_URL": "localhost:9092",
    "USE_COMPOSITE_KEYS": "true",
}
for _k, _v in _INTEGRATION_DEFAULTS.items():
    os.environ.setdefault(_k, _v)


# ---------------------------------------------------------------------------
# S3 (RustFS) fixtures
# ---------------------------------------------------------------------------

# MinIO no longer publishes images on Docker Hub, so use RustFS (S3-compatible,
# same as docker-compose-s3.yml) via a generic container.
_RUSTFS_IMAGE = "rustfs/rustfs:1.0.2"
_RUSTFS_PORT = 9000
_RUSTFS_ACCESS_KEY = "rustfsadmin"
_RUSTFS_SECRET_KEY = "rustfsadmin"  # noqa: S105 - throwaway test container credential


@pytest.fixture(scope="session")
def s3_container():
    """Start a RustFS (S3-compatible) container for the entire test session."""
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import HttpWaitStrategy

    container = (
        DockerContainer(_RUSTFS_IMAGE)
        .with_exposed_ports(_RUSTFS_PORT)
        .with_env("RUSTFS_ACCESS_KEY", _RUSTFS_ACCESS_KEY)
        .with_env("RUSTFS_SECRET_KEY", _RUSTFS_SECRET_KEY)
        .waiting_for(HttpWaitStrategy(_RUSTFS_PORT, "/health"))
    )
    with container as rustfs:
        endpoint = f"http://{rustfs.get_container_host_ip()}:{rustfs.get_exposed_port(_RUSTFS_PORT)}"

        os.environ["S3_ENDPOINT"] = endpoint
        os.environ["S3_ACCESS_KEY"] = _RUSTFS_ACCESS_KEY
        os.environ["S3_SECRET_KEY"] = _RUSTFS_SECRET_KEY

        # Reload modules that read S3 env vars at import time
        import worker.fault_tolerance.async_snapshots as snap_mod

        importlib.reload(snap_mod)

        yield {
            "endpoint": endpoint,
            "access_key": _RUSTFS_ACCESS_KEY,
            "secret_key": _RUSTFS_SECRET_KEY,
            "region": "us-east-1",
        }


@pytest.fixture(scope="session")
def s3_bucket(s3_container):
    """Create the styx-snapshots bucket and yield a boto3 S3 client."""
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_container["endpoint"],
        aws_access_key_id=s3_container["access_key"],
        aws_secret_access_key=s3_container["secret_key"],
        region_name=s3_container["region"],
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
    from testcontainers.community.kafka import KafkaContainer

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

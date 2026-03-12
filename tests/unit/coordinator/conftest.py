"""
Conftest for coordinator unit tests.

Adds coordinator/ to sys.path so local-import modules (worker_pool,
aria_sync_metadata, …) resolve correctly, mirroring how the coordinator
process runs (it starts from the coordinator/ directory).

Also sets required env vars before any coordinator module is imported at
collection time (snapshot_compactor reads S3_* at module level).
"""

import os
from pathlib import Path
import sys

# Set S3 / Kafka env vars before any coordinator module is imported.
_defaults = {
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "testkey",
    "S3_SECRET_KEY": "testsecret",
    "S3_REGION": "us-east-1",
    "KAFKA_URL": "localhost:9092",
    "SNAPSHOT_BUCKET_NAME": "styx-snapshots",
}
for _k, _v in _defaults.items():
    os.environ.setdefault(_k, _v)

# Add repo root to the front of sys.path so coordinator.* imports work.
_REPO_ROOT = str(Path(__file__).parent.parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Also add coordinator/ so bare imports like `from snapshot_compactor import ...` work.
_COORDINATOR_DIR = str(Path(__file__).parent.parent.parent.parent / "coordinator")
if _COORDINATOR_DIR not in sys.path:
    sys.path.insert(0, _COORDINATOR_DIR)

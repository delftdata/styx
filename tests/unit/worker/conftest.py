"""
Conftest for worker unit tests.

Adds the repo root to sys.path so the worker package resolves correctly
(worker modules use `from worker.X import Y` style imports).

Also sets required env vars before any worker module is imported at
collection time (async_snapshots reads S3_*/DISCOVERY_* at module level).
"""

import os
from pathlib import Path
import sys

# Set env vars before any worker module is imported.
_defaults = {
    "DISCOVERY_HOST": "localhost",
    "DISCOVERY_PORT": "8888",
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "testkey",
    "S3_SECRET_KEY": "testsecret",
    "S3_REGION": "us-east-1",
    "KAFKA_URL": "localhost:9092",
    "SNAPSHOT_BUCKET_NAME": "styx-snapshots",
    "USE_COMPOSITE_KEYS": "true",
}
for _k, _v in _defaults.items():
    os.environ.setdefault(_k, _v)

# Add repo root so `import worker.sequencer.sequencer` etc. work.
_REPO_ROOT = str(Path(__file__).parent.parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

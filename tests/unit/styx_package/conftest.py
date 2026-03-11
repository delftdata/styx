"""
Conftest for styx-package unit tests.

Sets USE_COMPOSITE_KEYS before any styx.common.operator module is imported,
since operator.py reads the env var at module-import time.
"""

import os

os.environ.setdefault("USE_COMPOSITE_KEYS", "true")

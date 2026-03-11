"""
Conftest for worker unit tests.

Adds the repo root to sys.path so the worker package resolves correctly
(worker modules use `from worker.X import Y` style imports).
"""

from pathlib import Path
import sys

# Add repo root so `import worker.sequencer.sequencer` etc. work.
_REPO_ROOT = str(Path(__file__).parent.parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

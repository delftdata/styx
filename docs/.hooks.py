import sys
from pathlib import Path

def on_pre_build(config):
    sys.path.insert(0, str(Path("styx-package").resolve()))

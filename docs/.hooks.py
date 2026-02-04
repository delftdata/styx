from pathlib import Path
import sys


def on_pre_build(config):
    sys.path.insert(0, str(Path("styx-package").resolve()))

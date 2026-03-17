"""Build script for Cython extensions in the worker package.

Usage:
    # Build in-place (for development)
    python worker/setup.py build_ext --inplace

    # The Dockerfile runs this automatically during image build.

If Cython is not installed, the build is skipped and the pure-Python
fallbacks in fast_copy.py / base_aria_state.py are used instead.
"""

from __future__ import annotations

import logging
from pathlib import Path

from setuptools import Extension, setup

try:
    from Cython.Build import cythonize

    HAS_CYTHON = True
except ImportError:
    HAS_CYTHON = False


def main() -> None:
    if not HAS_CYTHON:
        logging.warning("Cython not found -- skipping C extension build (pure-Python fallback will be used).")
        return

    extensions = [
        Extension(
            "worker.operator_state.aria._fast_copy",
            ["worker/operator_state/aria/_fast_copy.pyx"],
        ),
        Extension(
            "worker.operator_state.aria._aria_state",
            ["worker/operator_state/aria/_aria_state.pyx"],
        ),
        Extension(
            "worker.sequencer._sequencer",
            ["worker/sequencer/_sequencer.pyx"],
        ),
    ]

    setup(
        name="styx-worker-cython",
        ext_modules=cythonize(
            extensions,
            compiler_directives={
                "language_level": "3",
                "boundscheck": False,
                "wraparound": False,
            },
        ),
        zip_safe=False,
    )


if __name__ == "__main__":
    # Allow running from project root: `python worker/setup.py build_ext --inplace`
    project_root = Path(__file__).resolve().parent.parent
    if Path.cwd() != project_root:
        import os

        os.chdir(project_root)
    main()

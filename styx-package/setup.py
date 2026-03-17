"""Build script for Cython extensions in the styx package.

Usage:
    # Build in-place (for development)
    cd styx-package && python setup.py build_ext --inplace

    # The Dockerfile runs this automatically during image build.
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
        logging.warning("Cython not found -- skipping styx-package C extension build.")
        return

    extensions = [
        Extension(
            "styx.common.partitioning._hash_partitioner",
            ["styx/common/partitioning/_hash_partitioner.pyx"],
        ),
    ]

    setup(
        name="styx-cython",
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
    project_root = Path(__file__).resolve().parent
    if Path.cwd() != project_root:
        import os

        os.chdir(project_root)
    main()

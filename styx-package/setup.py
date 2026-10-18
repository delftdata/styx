"""Build script for Cython extensions in the styx package.

Usage:
    # Build in-place (for development)
    cd styx-package && python setup.py build_ext --inplace

    # The Dockerfile runs this automatically during image build.

With STYX_CYTHONIZE_HOT_PATHS=1 (the worker image default) the modules on the
worker's per-transaction path are also compiled as they are, from their .py
sources; see HOT_PY_MODULES. A plain `pip install` leaves them pure Python.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import re

from setuptools import Extension, setup

try:
    from Cython.Build import cythonize

    HAS_CYTHON = True
except ImportError:
    HAS_CYTHON = False

# Pure-Python modules on the worker's per-transaction path, compiled unchanged
# (no static typing; see HOT_PY_DIRECTIVES). The .py sources stay the source of
# truth: imports prefer the compiled module, and unit tests and coverage run
# against the sources because CI does not set STYX_CYTHONIZE_HOT_PATHS.
HOT_PY_MODULES = [
    "styx/common/stateful_function.py",
    "styx/common/operator.py",
    "styx/common/tcp_networking.py",
    "styx/common/base_networking.py",
    "styx/common/serialization.py",
    "styx/common/run_func_payload.py",
    "styx/common/util/aio_task_scheduler.py",
    "styx/common/partitioning/hash_partitioner.py",
]

# Keep Python semantics: annotations are not C types (an `int` annotation would
# otherwise become a C long), and bounds/wraparound checks stay on.
HOT_PY_DIRECTIVES = {"language_level": "3", "annotation_typing": False, "binding": True}

# Cython 3.3 parses Python 3.14's unparenthesized `except A, B:` as Python 2's
# `except A as B`: it compiles, then catches only A and rebinds B. Ruff's
# formatter strips the parentheses for py314, so those lines need `# fmt: skip`.
_BARE_MULTI_EXCEPT = re.compile(r"^\s*except\*?\s+(?!\()[\w.]+\s*,")


def check_except_syntax(paths: list[str]) -> None:
    bad = [
        f"{path}:{n}"
        for path in paths
        for n, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1)
        if _BARE_MULTI_EXCEPT.match(line)
    ]
    if bad:
        msg = "Cython would compile `except A, B:` as `except A as B`; write `except (A, B):  # fmt: skip` at: " + (
            ", ".join(bad)
        )
        raise SystemExit(msg)


def hot_path_extensions(modules: list[str]) -> list[Extension]:
    if os.getenv("STYX_CYTHONIZE_HOT_PATHS", "0").lower() not in {"1", "true"}:
        return []
    check_except_syntax(modules)
    return cythonize(
        [Extension(m.removesuffix(".py").replace("/", "."), [m]) for m in modules],
        compiler_directives=HOT_PY_DIRECTIVES,
    )


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
        )
        + hot_path_extensions(HOT_PY_MODULES),
        zip_safe=False,
    )


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent
    if Path.cwd() != project_root:
        import os

        os.chdir(project_root)
    main()

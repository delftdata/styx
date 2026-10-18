"""Build script for Cython extensions in the worker package.

Usage:
    # Build in-place (for development)
    python worker/setup.py build_ext --inplace

    # The Dockerfile runs this automatically during image build.

If Cython is not installed, the build is skipped and the pure-Python
fallbacks in fast_copy.py / base_aria_state.py are used instead.

With STYX_CYTHONIZE_HOT_PATHS=1 (the Docker image default) the worker's
hot-path modules are also compiled as they are, from their .py sources; see
HOT_PY_MODULES.
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
    "worker/transactional_protocols/aria.py",
    "worker/operator_state/aria/base_aria_state.py",
    "worker/operator_state/aria/in_memory_state.py",
    "worker/worker_service.py",
    "worker/ingress/styx_kafka_ingress.py",
    "worker/egress/styx_kafka_batch_egress.py",
    "worker/sequencer/sequencer.py",
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
        )
        + hot_path_extensions(HOT_PY_MODULES),
        zip_safe=False,
    )


if __name__ == "__main__":
    # Allow running from project root: `python worker/setup.py build_ext --inplace`
    project_root = Path(__file__).resolve().parent.parent
    if Path.cwd() != project_root:
        import os

        os.chdir(project_root)
    main()

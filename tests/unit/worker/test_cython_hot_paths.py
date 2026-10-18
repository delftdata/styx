"""The hot-path modules compiled by worker/setup.py and styx-package/setup.py.

CI runs the unit tests against the .py sources (it does not set
STYX_CYTHONIZE_HOT_PATHS), so these checks are what keeps a module that the
Docker image compiles from silently changing meaning under Cython.
"""

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]


def _load_setup(path: Path):
    spec = importlib.util.spec_from_file_location(f"_setup_{path.parent.name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


WORKER_SETUP = _load_setup(REPO / "worker" / "setup.py")
STYX_SETUP = _load_setup(REPO / "styx-package" / "setup.py")

HOT_MODULES = [REPO / m for m in WORKER_SETUP.HOT_PY_MODULES] + [
    REPO / "styx-package" / m for m in STYX_SETUP.HOT_PY_MODULES
]


@pytest.mark.parametrize("module", HOT_MODULES, ids=lambda p: p.relative_to(REPO).as_posix())
def test_hot_module_compiles_with_python_semantics(module):
    assert module.exists()
    # Raises SystemExit naming the line if a bare `except A, B:` slipped in.
    WORKER_SETUP.check_except_syntax([str(module)])


@pytest.mark.parametrize("setup", [WORKER_SETUP, STYX_SETUP], ids=["worker", "styx-package"])
class TestExceptGuard:
    @pytest.mark.parametrize(
        "line",
        [
            "except ValueError, KeyError:",
            "    except asyncio.CancelledError, GeneratorExit:",
            "except* OSError, RuntimeError:",
        ],
    )
    def test_rejects_bare_multi_except(self, setup, tmp_path, line):
        f = tmp_path / "m.py"
        f.write_text(f"try:\n    pass\n{line}\n    pass\n", encoding="utf-8")
        with pytest.raises(SystemExit, match=r"m\.py:3"):
            setup.check_except_syntax([str(f)])

    @pytest.mark.parametrize(
        "line",
        [
            "except (ValueError, KeyError):  # fmt: skip",
            "except ValueError:",
            "except ValueError as e:",
            "except (ValueError, KeyError) as e:",
            "except:",
        ],
    )
    def test_accepts(self, setup, tmp_path, line):
        f = tmp_path / "m.py"
        f.write_text(f"try:\n    pass\n{line}\n    pass\n", encoding="utf-8")
        setup.check_except_syntax([str(f)])

    def test_hot_paths_off_by_default(self, setup, monkeypatch):
        monkeypatch.delenv("STYX_CYTHONIZE_HOT_PATHS", raising=False)
        assert setup.hot_path_extensions(setup.HOT_PY_MODULES) == []

    def test_hot_paths_on_compiles_every_module_with_python_semantics(self, setup, monkeypatch):
        monkeypatch.setenv("STYX_CYTHONIZE_HOT_PATHS", "true")
        calls = []

        def fake_cythonize(extensions, compiler_directives):
            calls.append(compiler_directives)
            return extensions

        monkeypatch.setattr(setup, "cythonize", fake_cythonize, raising=False)
        root = REPO if setup is WORKER_SETUP else REPO / "styx-package"
        monkeypatch.chdir(root)
        extensions = setup.hot_path_extensions(setup.HOT_PY_MODULES)

        assert [e.sources for e in extensions] == [[m] for m in setup.HOT_PY_MODULES]
        assert [e.name for e in extensions] == [m.removesuffix(".py").replace("/", ".") for m in setup.HOT_PY_MODULES]
        assert calls == [{"language_level": "3", "annotation_typing": False, "binding": True}]

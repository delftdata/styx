"""Unit tests for styx.common.logging — LOG_LEVEL env var support."""

import importlib
import logging as _stdlib_logging
import os


def test_default_log_level_is_warning():
    """Without LOG_LEVEL env var, logger should be WARNING."""
    os.environ.pop("LOG_LEVEL", None)
    import styx.common.logging as mod

    importlib.reload(mod)
    assert mod.logging.level == _stdlib_logging.WARNING


def test_log_level_from_env(monkeypatch):
    """LOG_LEVEL=DEBUG should set the logger to DEBUG."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    import styx.common.logging as mod

    importlib.reload(mod)
    assert mod.logging.level == _stdlib_logging.DEBUG
    # Reset
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    importlib.reload(mod)


def test_invalid_log_level_falls_back_to_warning(monkeypatch):
    """An unrecognized LOG_LEVEL should fall back to WARNING."""
    monkeypatch.setenv("LOG_LEVEL", "INVALID_LEVEL")
    import styx.common.logging as mod

    importlib.reload(mod)
    assert mod.logging.level == _stdlib_logging.WARNING
    # Reset
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    importlib.reload(mod)

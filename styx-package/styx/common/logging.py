import logging as _stdlib_logging
import sys


def _can_use_aiologger() -> bool:
    """Check whether aiologger can safely use stderr as a write pipe."""
    try:
        return hasattr(sys.stderr, "fileno") and sys.stderr.fileno() >= 0
    except Exception:
        return False


def _make_stdlib_logger() -> _stdlib_logging.Logger:
    logger = _stdlib_logging.getLogger("styx")
    logger.setLevel(_stdlib_logging.WARNING)
    if not logger.handlers:
        handler = _stdlib_logging.StreamHandler()
        handler.setFormatter(
            _stdlib_logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s")
        )
        logger.addHandler(handler)
    return logger


try:
    from aiologger import Logger
    from aiologger.formatters.base import Formatter
    from aiologger.levels import LogLevel

    if _can_use_aiologger():
        logging = Logger.with_default_handlers(
            name="styx",
            level=LogLevel.WARNING,
            formatter=Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s"),
        )
    else:
        logging = _make_stdlib_logger()
except Exception:
    logging = _make_stdlib_logger()

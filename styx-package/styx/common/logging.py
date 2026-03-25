import logging as _stdlib_logging
from logging.handlers import QueueHandler, QueueListener
import os
from queue import SimpleQueue

_log_queue: SimpleQueue = SimpleQueue()

# Root "styx" logger — uses QueueHandler so callers never block.
# Log level is configurable via LOG_LEVEL env var (default: WARNING).
logging = _stdlib_logging.getLogger("styx")
_log_level = getattr(_stdlib_logging, os.getenv("LOG_LEVEL", "WARNING").upper(), _stdlib_logging.WARNING)
logging.setLevel(_log_level)

if not logging.handlers:
    logging.addHandler(QueueHandler(_log_queue))

# Background thread drains the queue to stderr.
_stream_handler = _stdlib_logging.StreamHandler()
_stream_handler.setFormatter(_stdlib_logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s"))
_listener = QueueListener(_log_queue, _stream_handler, respect_handler_level=True)
_listener.start()

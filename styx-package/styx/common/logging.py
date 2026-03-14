import logging as _stdlib_logging
from logging.handlers import QueueHandler, QueueListener
from queue import SimpleQueue

_log_queue: SimpleQueue = SimpleQueue()

# Root "styx" logger — uses QueueHandler so callers never block.
logging = _stdlib_logging.getLogger("styx")
logging.setLevel(_stdlib_logging.WARNING)

if not logging.handlers:
    logging.addHandler(QueueHandler(_log_queue))

# Background thread drains the queue to stderr.
_stream_handler = _stdlib_logging.StreamHandler()
_stream_handler.setFormatter(_stdlib_logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s"))
_listener = QueueListener(_log_queue, _stream_handler, respect_handler_level=True)
_listener.start()

from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.levels import LogLevel

logging = Logger.with_default_handlers(
    name="styx",
    level=LogLevel.WARNING,
    formatter=Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s"),
)

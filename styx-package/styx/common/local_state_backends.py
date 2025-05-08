from enum import Enum, auto


class LocalStateBackend(Enum):
    """Enumeration of local state backend implementations for Styx operators.

    This enum specifies which in-memory or local structure is used
    to manage operator state.

    Values:
        DICT: Uses a plain Python dictionary as the state backend.
    """
    DICT = auto()

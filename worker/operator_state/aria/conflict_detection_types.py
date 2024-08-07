from enum import Enum


class AriaConflictDetectionType(Enum):
    DEFAULT_SERIALIZABLE = 0
    DETERMINISTIC_REORDERING = 1
    SNAPSHOT_ISOLATION = 2

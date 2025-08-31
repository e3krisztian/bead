from enum import Enum
from enum import auto


class QueryCondition(Enum):
    BEAD_NAME = auto()
    KIND = auto()
    CONTENT_ID = auto()
    AT_TIME = auto()
    NEWER_THAN = auto()
    OLDER_THAN = auto()
    AT_OR_NEWER = auto()
    AT_OR_OLDER = auto()

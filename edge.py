from enum import Enum, IntEnum


class Priority(IntEnum):
    next_hop = 1
    unreliable = 2
    multi = 3
    none = 4
    modified = 5


class Type(Enum):
    next_hop = 1
    same_as = 2
    echo = 3
    unreachable = 4

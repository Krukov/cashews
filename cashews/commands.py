from enum import Enum


class Command(Enum):
    ALL = "_"

    GET = "get"
    GET_MANY = "get_many"
    GET_RAW = "get_raw"
    GET_MATCH = "get_match"
    SET = "set"
    SET_RAW = "set_raw"
    SET_MANY = "set_many"
    DELETE = "delete"
    DELETE_MATCH = "delete_match"

    EXIST = "exists"
    KEY_MATCH = "keys_match"
    SCAN = "scan"
    INCR = "incr"
    EXPIRE = "expire"
    GET_EXPIRE = "get_expire"
    CLEAR = "clear"
    SET_LOCK = "set_lock"
    UNLOCK = "unlock"
    IS_LOCKED = "is_locked"

    GET_BITS = "get_bits"
    INCR_BITS = "incr_bits"

    PING = "ping"
    GET_SIZE = "get_size"


PATTERN_CMDS = (Command.GET_MATCH, Command.DELETE_MATCH, Command.KEY_MATCH, Command.SCAN)
RETRIEVE_CMDS = (Command.GET, Command.INCR, Command.GET_MANY, Command.GET_MATCH)

from enum import Enum


class Command(Enum):
    GET = "get"
    GET_MANY = "get_many"
    GET_RAW = "get_raw"
    GET_MATCH = "get_match"
    SET = "set"
    SET_RAW = "set_raw"
    SET_MANY = "set_many"
    DELETE = "delete"
    DELETE_MANY = "delete_many"
    DELETE_MATCH = "delete_match"

    EXISTS = "exists"
    SCAN = "scan"
    INCR = "incr"
    EXPIRE = "expire"
    GET_EXPIRE = "get_expire"
    CLEAR = "clear"

    GET_BITS = "get_bits"
    INCR_BITS = "incr_bits"

    PING = "ping"
    GET_SIZE = "get_size"
    GET_KEYS_COUNT = "get_keys_count"


ALL = set(Command)
PATTERN_CMDS = {Command.GET_MATCH, Command.DELETE_MATCH, Command.SCAN}
RETRIEVE_CMDS = {Command.GET, Command.INCR, Command.GET_MANY, Command.GET_MATCH}

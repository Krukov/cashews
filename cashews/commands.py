from enum import Enum


class Command(Enum):
    GET = "get"
    GET_MANY = "get_many"
    GET_MATCH = "get_match"
    INCR = "incr"

    SET = "set"
    SET_MANY = "set_many"

    DELETE = "delete"
    DELETE_MANY = "delete_many"
    DELETE_MATCH = "delete_match"


ALL = set(Command)
RETRIEVE_CMDS = {Command.GET, Command.INCR, Command.GET_MANY, Command.GET_MATCH}

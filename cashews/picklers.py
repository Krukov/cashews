import pickle

from ._typing import Value
from .exceptions import UnsupportedPicklerError

_SQLALC_PICKLE = True
try:
    from sqlalchemy.ext import serializer as sqlalchemy_pickle
except ImportError:
    _SQLALC_PICKLE = False
    sqlalchemy_pickle = pickle

_DILL_PICKLE = True
try:
    import dill
except ImportError:
    _DILL_PICKLE = False
    dill = pickle


class Pickler:
    PickleError = pickle.PickleError
    UnpicklingError = (pickle.UnpicklingError, TypeError)

    @staticmethod
    def loads(value: bytes) -> Value:
        return pickle.loads(value, fix_imports=False, encoding="bytes")

    @staticmethod
    def dumps(value: Value) -> bytes:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)


class SQLAlchemyPickler(Pickler):
    @staticmethod
    def loads(value: bytes) -> Value:
        return sqlalchemy_pickle.loads(
            value,
        )

    @staticmethod
    def dumps(value: Value) -> bytes:
        return sqlalchemy_pickle.dumps(value)


class DillPickler(Pickler):
    @staticmethod
    def loads(value: bytes) -> Value:
        return dill.loads(value)

    @staticmethod
    def dumps(value: Value) -> bytes:
        return dill.dumps(value)


class NonPickler(Pickler):
    @staticmethod
    def loads(value: bytes) -> Value:
        return value

    @staticmethod
    def dumps(value: Value) -> bytes:
        return value


DEFAULT_PICKLE = "default"
NULL_PICKLE = "null"

_picklers = {
    DEFAULT_PICKLE: Pickler,
    "sqlalchemy": SQLAlchemyPickler,
    "dill": DillPickler,
    NULL_PICKLE: NonPickler,
}


def get_pickler(name: str):
    if name not in _picklers:
        raise UnsupportedPicklerError()

    if name == "sqlalchemy" and not _SQLALC_PICKLE:
        raise UnsupportedPicklerError()

    if name == "dill" and not _DILL_PICKLE:
        raise UnsupportedPicklerError()

    return _picklers[name]

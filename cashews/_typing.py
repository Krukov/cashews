from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Protocol, TypeVar, Union

if TYPE_CHECKING:  # pragma: no cover
    from . import Command
    from .backends.interface import Backend


_TTLTypes = Union[int, float, str, timedelta, None]
TTL = Union[_TTLTypes, Callable[..., _TTLTypes]]


class CallableCacheCondition(Protocol):
    def __call__(
        self, result: Any, args: tuple, kwargs: dict[str, Any], key: str = ""
    ) -> bool | Exception:  # pragma: no cover
        ...


Key = str
KeyTemplate = str
KeyOrTemplate = Union[KeyTemplate, Key]
Value = Any
Default = TypeVar("Default")
Tag = str
Tags = Iterable[Tag]
Exceptions = Union[type[Exception], Iterable[type[Exception]], None]

CacheCondition = Union[CallableCacheCondition, str, None]

Result_T = TypeVar("Result_T")
AsyncCallable_T = Callable[..., Awaitable[Result_T]]
Callable_T = Callable[..., Result_T]

DecoratedFunc = TypeVar("DecoratedFunc", bound=AsyncCallable_T)


class Middleware(Protocol):
    def __call__(
        self,
        call: AsyncCallable_T,
        cmd: Command,
        backend: Backend,
        *args,
        **kwargs,
    ) -> Awaitable[Result_T | None]:  # pragma: no cover
        ...


class OnRemoveCallback(Protocol):
    async def __call__(
        self,
        keys: Iterable[Key],
        backend: Backend,
    ) -> None:  # pragma: no cover
        ...


class Callback(Protocol):
    async def __call__(self, cmd: Command, key: Key, result: Any, backend: Backend) -> None:
        pass


class ShortCallback(Protocol):
    def __call__(self, key: Key, result: Any) -> None:
        pass


class ICustomEncoder(Protocol):
    async def __call__(
        self, value: Value, backend: Backend, key: Key, expire: float | None
    ) -> bytes:  # pragma: no cover
        ...


class ICustomDecoder(Protocol):
    async def __call__(self, value: bytes, backend: Backend, key: Key) -> Value:  # pragma: no cover
        ...

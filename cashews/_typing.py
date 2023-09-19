from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Type, TypeVar, Union

try:
    from typing import Protocol
except ImportError:  # 3.7 python
    from typing_extensions import Protocol  # type: ignore[assignment]

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
Tag = str
Tags = Iterable[Tag]
Exceptions = Union[Type[Exception], Iterable[Type[Exception]], None]

CacheCondition = Union[CallableCacheCondition, str, None]

AsyncCallableResult_T = TypeVar("AsyncCallableResult_T")
AsyncCallable_T = Callable[..., Awaitable[AsyncCallableResult_T]]

DecoratedFunc = TypeVar("DecoratedFunc", bound=AsyncCallable_T)
Decorator = Callable[[DecoratedFunc], DecoratedFunc]

if TYPE_CHECKING:  # pragma: no cover
    from . import Command
    from .backends.interface import Backend


class Middleware(Protocol):
    def __call__(
        self,
        call: AsyncCallable_T,
        cmd: Command,
        backend: Backend,
        *args,
        **kwargs,
    ) -> Awaitable[AsyncCallableResult_T | None]:  # pragma: no cover
        ...


class Callback(Protocol):
    async def __call__(
        self,
        keys: Iterable[Key],
        backend: Backend,
    ) -> None:  # pragma: no cover
        ...


class ICustomEncoder(Protocol):
    async def __call__(self, value: Value, backend, key: Key, expire: float | None) -> bytes:
        ...


class ICustomDecoder(Protocol):
    async def __call__(self, value: bytes, backend, key: Key) -> Value:
        ...

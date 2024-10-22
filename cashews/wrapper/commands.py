from __future__ import annotations

import inspect
from functools import partial
from typing import TYPE_CHECKING, AsyncIterator, Iterable, Mapping, overload

from cashews.backends.interface import Backend
from cashews.commands import Command
from cashews.ttl import ttl_to_seconds

from .wrapper import Wrapper

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import TTL, AsyncCallable_T, Callable_T, Default, Key, Result_T, Value

_empty = object()


class CommandWrapper(Wrapper):
    async def set(
        self,
        key: Key,
        value: Value,
        expire: TTL = None,
        exist: bool | None = None,
    ) -> bool:
        return await self._with_middlewares(Command.SET, key)(
            key=key,
            value=value,
            expire=ttl_to_seconds(expire),
            exist=exist,
        )

    async def set_raw(self, key: Key, value: Value, **kwargs) -> None:
        return await self._with_middlewares(Command.SET_RAW, key)(key=key, value=value, **kwargs)

    @overload
    async def get(self, key: Key, default: Default) -> Value | Default: ...

    @overload
    async def get(self, key: Key, default: None = None) -> Value | None: ...

    async def get(self, key: Key, default: Default | None = None) -> Value | Default | None:
        return await self._with_middlewares(Command.GET, key)(key=key, default=default)

    async def get_or_set(
        self, key: Key, default: Default | AsyncCallable_T | Callable_T, expire: TTL = None
    ) -> Value | Default | Result_T:
        value = await self.get(key, default=_empty)
        if value is not _empty:
            return value
        if inspect.isawaitable(default):
            _default = await default
        elif callable(default):
            if inspect.iscoroutinefunction(default):
                _default = await default()
            else:
                _default = default()
        else:
            _default = default
        await self.set(key, _default, expire=expire)
        return default

    async def get_raw(self, key: Key) -> Value:
        return await self._with_middlewares(Command.GET_RAW, key)(key=key)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.scan(pattern, batch_size=batch_size)

        for middleware in middlewares:
            call = partial(middleware, call, Command.SCAN, backend)
        async for key in await call(pattern=pattern, batch_size=batch_size):
            yield key

    async def get_match(
        self,
        pattern: str,
        batch_size: int = 100,
    ) -> AsyncIterator[tuple[Key, Value]]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.get_match(pattern, batch_size=batch_size)

        for middleware in middlewares:
            call = partial(middleware, call, Command.GET_MATCH, backend)
        async for key, value in await call(pattern=pattern, batch_size=batch_size):
            yield key, value

    async def get_many(self, *keys: Key, default: Value | None = None) -> tuple[Value | None, ...]:
        backends: dict[Backend, list[str]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        result: dict[Key, Value] = {}
        for _keys in backends.values():
            _values = await self._with_middlewares(Command.GET_MANY, _keys[0])(*_keys, default=default)
            result.update(dict(zip(_keys, _values)))
        return tuple(result.get(key) for key in keys)

    async def set_many(self, pairs: Mapping[Key, Value], expire: TTL = None):
        backends: dict[Backend, list[Key]] = {}
        for key in pairs:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for keys in backends.values():
            data = {key: pairs[key] for key in keys}
            await self._with_middlewares(Command.SET_MANY, keys[0])(
                pairs=data,
                expire=ttl_to_seconds(expire),
            )

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> tuple[int, ...]:
        return await self._with_middlewares(Command.GET_BITS, key)(key, *indexes, size=size)

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> tuple[int, ...]:
        return await self._with_middlewares(Command.INCR_BITS, key)(key, *indexes, size=size, by=by)

    async def slice_incr(
        self,
        key: Key,
        start: int | float,
        end: int | float,
        maxvalue: int,
        expire: TTL = None,
    ) -> int:
        return await self._with_middlewares(Command.SLICE_INCR, key)(
            key=key,
            start=start,
            end=end,
            maxvalue=maxvalue,
            expire=ttl_to_seconds(expire),
        )

    async def incr(self, key: Key, value: int = 1, expire: float | None = None) -> int:
        return await self._with_middlewares(Command.INCR, key)(key=key, value=value, expire=expire)

    async def delete(self, key: Key) -> bool:
        return await self._with_middlewares(Command.DELETE, key)(key=key)

    async def delete_many(self, *keys: Key) -> None:
        backends: dict[Backend, list[Key]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for _keys in backends.values():
            await self._with_middlewares(Command.DELETE_MANY, _keys[0])(*_keys)

    async def delete_match(self, pattern: str) -> None:
        return await self._with_middlewares(Command.DELETE_MATCH, pattern)(pattern=pattern)

    async def expire(self, key: Key, timeout: TTL):
        return await self._with_middlewares(Command.EXPIRE, key)(key=key, timeout=ttl_to_seconds(timeout))

    async def get_expire(self, key: Key) -> int:
        return await self._with_middlewares(Command.GET_EXPIRE, key)(key=key)

    async def exists(self, key: Key) -> bool:
        return await self._with_middlewares(Command.EXIST, key)(key=key)

    async def set_lock(self, key: Key, value: Value, expire: TTL) -> bool:
        return await self._with_middlewares(Command.SET_LOCK, key)(key=key, value=value, expire=ttl_to_seconds(expire))

    async def unlock(self, key: Key, value: Value) -> bool:
        return await self._with_middlewares(Command.UNLOCK, key)(key=key, value=value)

    async def get_size(self, key: Key) -> int:
        return await self._with_middlewares(Command.GET_SIZE, key)(key=key)

    async def ping(self, message: bytes | None = None) -> bytes:
        message = b"PING" if message is None else message
        return await self._with_middlewares(Command.PING, message.decode())(message=message)

    async def get_keys_count(self) -> int:
        result = 0
        for backend, _ in self._backends.values():
            count = await self._with_middlewares_for_backend(
                Command.GET_KEYS_COUNT, backend, self._default_middlewares
            )()
            result += count
        return result

    async def clear(self) -> None:
        for backend, _ in self._backends.values():
            await self._with_middlewares_for_backend(Command.CLEAR, backend, self._default_middlewares)()

    async def is_locked(
        self,
        key: Key,
        wait: TTL = None,
        step: int | float = 0.1,
    ) -> bool:
        return await self._with_middlewares(Command.IS_LOCKED, key)(key=key, wait=ttl_to_seconds(wait), step=step)

    async def set_add(self, key: Key, *values: str, expire: TTL = None) -> None:
        return await self._with_middlewares(Command.SET_ADD, key)(key, *values, expire=ttl_to_seconds(expire))

    async def set_remove(self, key: Key, *values: str) -> None:
        return await self._with_middlewares(Command.SET_REMOVE, key)(key, *values)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        return await self._with_middlewares(Command.SET_POP, key)(key=key, count=count)

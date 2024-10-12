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
        backend = self._get_backend(key)
        return await self._call_with_middlewares_for_backend(
            call=backend.set,
            cmd=Command.SET,
            backend=backend,
        )(
            key=key,
            value=value,
            expire=ttl_to_seconds(expire),
            exist=exist,
        )

    async def set_raw(self, key: Key, value: Value, **kwargs) -> None:
        backend = self._get_backend(key)
        return await backend.set_raw(key=key, value=value, **kwargs)

    @overload
    async def get(self, key: Key, default: Default) -> Value | Default: ...

    @overload
    async def get(self, key: Key, default: None = None) -> Value | None: ...

    async def get(self, key: Key, default: Default | None = None) -> Value | Default | None:
        backend = self._get_backend(key)
        return await self._call_with_middlewares_for_backend(
            call=backend.get,
            cmd=Command.GET,
            backend=backend,
        )(key=key, default=default)

    async def get_or_set(
        self, key: Key, default: Default | AsyncCallable_T | Callable_T, expire: TTL = None
    ) -> Value | Default | Result_T:
        value = await self.get(key, default=_empty)
        if value is not _empty:
            return value
        if callable(default):
            if inspect.iscoroutinefunction(default):
                _default = await default()
            else:
                _default = default()
        else:
            _default = default
        await self.set(key, _default, expire=expire)
        return default

    async def get_raw(self, key: Key) -> Value:
        backend = self._get_backend(key)
        return await backend.get_raw(key=key)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        backend = self._get_backend(pattern)
        async for key in backend.scan(pattern=pattern, batch_size=batch_size):
            yield key

    async def get_match(
        self,
        pattern: str,
        batch_size: int = 100,
    ) -> AsyncIterator[tuple[Key, Value]]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.get_match(pattern=pattern, batch_size=batch_size)

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
        for backend, _keys in backends.items():
            _values = await self._call_with_middlewares_for_backend(
                call=backend.get_many,
                cmd=Command.GET_MANY,
                backend=backend,
            )(*_keys, default=default)
            result.update(dict(zip(_keys, _values)))
        return tuple(result.get(key) for key in keys)

    async def set_many(self, pairs: Mapping[Key, Value], expire: TTL = None):
        backends: dict[Backend, list[Key]] = {}
        for key in pairs:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for backend, keys in backends.items():
            data = {key: pairs[key] for key in keys}
            await self._call_with_middlewares_for_backend(
                call=backend.set_many,
                cmd=Command.SET_MANY,
                backend=backend,
            )(
                pairs=data,
                expire=ttl_to_seconds(expire),
            )

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> tuple[int, ...]:
        backend = self._get_backend(key)
        return await backend.get_bits(key, *indexes, size=size)

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> tuple[int, ...]:
        backend = self._get_backend(key)
        return await backend.incr_bits(key, *indexes, size=size, by=by)

    async def slice_incr(
        self,
        key: Key,
        start: int | float,
        end: int | float,
        maxvalue: int,
        expire: TTL = None,
    ) -> int:
        backend = self._get_backend(key)
        return await backend.slice_incr(
            key=key,
            start=start,
            end=end,
            maxvalue=maxvalue,
            expire=ttl_to_seconds(expire),
        )

    async def incr(self, key: Key, value: int = 1, expire: float | None = None) -> int:
        backend = self._get_backend(key)
        return await self._call_with_middlewares_for_backend(
            call=backend.incr,
            cmd=Command.INCR,
            backend=backend,
        )(key=key, value=value, expire=expire)

    async def delete(self, key: Key) -> bool:
        backend = self._get_backend(key)
        return await self._call_with_middlewares_for_backend(
            call=backend.delete,
            cmd=Command.DELETE,
            backend=backend,
        )(key=key)

    async def delete_many(self, *keys: Key) -> None:
        backends: dict[Backend, list[Key]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for backend, _keys in backends.items():
            await self._call_with_middlewares_for_backend(
                call=backend.delete_many,
                cmd=Command.DELETE_MANY,
                backend=backend,
            )(*_keys)

    async def delete_match(self, pattern: str) -> None:
        backend = self._get_backend(pattern)
        return await self._call_with_middlewares_for_backend(
            call=backend.delete_match,
            cmd=Command.DELETE_MATCH,
            backend=backend,
        )(pattern=pattern)

    async def expire(self, key: Key, timeout: TTL):
        backend = self._get_backend(key)
        ttl = ttl_to_seconds(timeout)
        if ttl is None:
            raise ValueError("timeout can't be none")
        return await backend.expire(key=key, timeout=ttl)

    async def get_expire(self, key: Key) -> int:
        backend = self._get_backend(key)
        return await backend.get_expire(key=key)

    async def exists(self, key: Key) -> bool:
        backend = self._get_backend(key)
        return await self._call_with_middlewares_for_backend(
            call=backend.exists,
            cmd=Command.EXISTS,
            backend=backend,
        )(key=key)

    async def set_lock(self, key: Key, value: Value, expire: TTL) -> bool:
        backend = self._get_backend(key)
        return await backend.set_lock(key=key, value=value, expire=ttl_to_seconds(expire))

    async def unlock(self, key: Key, value: Value) -> bool:
        backend = self._get_backend(key)
        return await backend.unlock(key=key, value=value)

    async def ping(self, message: bytes | None = None) -> bytes | None:
        _message = b"PING" if message is None else message
        for backend in self._backends.values():
            pong = await backend.ping(message=_message)
            if pong is None:  # disabled
                return
        return message or b"PONG"

    async def get_keys_count(self) -> int:
        result = 0
        for backend in self._backends.values():
            result += await backend.get_keys_count()
        return result

    async def clear(self) -> None:
        for backend in self._backends.values():
            await backend.clear()

    async def is_locked(
        self,
        key: Key,
        wait: TTL = None,
        step: int | float = 0.1,
    ) -> bool:
        backend = self._get_backend(key)
        return await backend.is_locked(key=key, wait=ttl_to_seconds(wait), step=step)

    # REMOVE
    async def set_add(self, key: Key, *values: str, expire: TTL = None) -> None:
        backend = self._get_backend(key)
        return await backend.set_add(key, *values, expire=ttl_to_seconds(expire))

    async def set_remove(self, key: Key, *values: str) -> None:
        backend = self._get_backend(key)
        return await backend.set_remove(key, *values)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        backend = self._get_backend(key)
        return await backend.set_pop(key=key, count=count)

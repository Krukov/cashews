from functools import partial
from typing import AsyncIterator, Dict, Iterable, List, Mapping, Optional, Tuple, Union

from cashews._typing import TTL, Key, Value
from cashews.backends.interface import Backend
from cashews.commands import Command
from cashews.ttl import ttl_to_seconds

from .wrapper import Wrapper


class CommandWrapper(Wrapper):
    async def set(
        self,
        key: Key,
        value: Value,
        expire: Union[float, TTL, None] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        return await self._with_middlewares(Command.SET, key)(
            key=key,
            value=value,
            expire=ttl_to_seconds(expire),
            exist=exist,
        )

    async def set_raw(self, key: Key, value: Value, **kwargs):
        return await self._with_middlewares(Command.SET_RAW, key)(key=key, value=value, **kwargs)

    async def get(self, key: Key, default: Optional[Value] = None) -> Value:
        return await self._with_middlewares(Command.GET, key)(key=key, default=default)

    async def get_raw(self, key: Key) -> Value:
        return await self._with_middlewares(Command.GET_RAW, key)(key=key)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:  # type: ignore
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.scan(pattern, batch_size=batch_size)

        for middleware in middlewares:
            call = partial(middleware, call, Command.SCAN, backend)
        async for key in (await call(pattern=pattern, batch_size=batch_size)):
            yield key

    async def get_match(  # type: ignore
        self,
        pattern: str,
        batch_size: int = 100,
    ) -> AsyncIterator[Tuple[Key, Value]]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.get_match(pattern, batch_size=batch_size)

        for middleware in middlewares:
            call = partial(middleware, call, Command.GET_MATCH, backend)
        async for key, value in (await call(pattern=pattern, batch_size=batch_size)):
            yield key, value

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Tuple[Optional[Value], ...]:
        backends: Dict[Backend, List[str]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        result: Dict[Key, Value] = {}
        for _keys in backends.values():
            _values = await self._with_middlewares(Command.GET_MANY, _keys[0])(*_keys, default=default)
            result.update(dict(zip(_keys, _values)))
        return tuple(result.get(key) for key in keys)

    async def set_many(self, pairs: Mapping[Key, Value], expire: Union[float, TTL, None] = None):
        backends: Dict[Backend, List[Key]] = {}
        for key in pairs:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for backend, keys in backends.items():
            data = {key: pairs[key] for key in keys}
            await self._with_middlewares(Command.SET_MANY, keys[0])(
                pairs=data,
                expire=ttl_to_seconds(expire),
            )

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        return await self._with_middlewares(Command.GET_BITS, key)(key, *indexes, size=size)

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        return await self._with_middlewares(Command.INCR_BITS, key)(key, *indexes, size=size, by=by)

    async def slice_incr(
        self, key: Key, start: int, end: int, maxvalue: int, expire: Union[float, TTL, None] = None
    ) -> int:
        return await self._with_middlewares(Command.SLICE_INCR, key)(
            key=key, start=start, end=end, maxvalue=maxvalue, expire=ttl_to_seconds(expire)
        )

    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None) -> int:
        return await self._with_middlewares(Command.INCR, key)(key=key, value=value, expire=expire)

    async def delete(self, key: Key) -> bool:
        return await self._with_middlewares(Command.DELETE, key)(key=key)

    async def delete_many(self, *keys: Key) -> None:
        backends: Dict[Backend, List[Key]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for _keys in backends.values():
            await self._with_middlewares(Command.DELETE_MANY, _keys[0])(*_keys)

    async def delete_match(self, pattern: str):
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

    async def ping(self, message: Optional[bytes] = None) -> bytes:
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

    async def clear(self):
        for backend, _ in self._backends.values():
            await self._with_middlewares_for_backend(Command.CLEAR, backend, self._default_middlewares)()

    async def is_locked(
        self,
        key: Key,
        wait: Union[float, None, TTL] = None,
        step: Union[int, float] = 0.1,
    ) -> bool:
        return await self._with_middlewares(Command.IS_LOCKED, key)(key=key, wait=ttl_to_seconds(wait), step=step)

    async def set_add(self, key: Key, *values: str, expire: Optional[float] = None):
        return await self._with_middlewares(Command.SET_ADD, key)(key, *values, expire=expire)

    async def set_remove(self, key: Key, *values: str):
        return await self._with_middlewares(Command.SET_REMOVE, key)(key, *values)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        return await self._with_middlewares(Command.SET_POP, key)(key=key, count=count)

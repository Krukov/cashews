from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Callable

from cashews import decorators, validation
from cashews.cache_condition import get_cache_condition
from cashews.ttl import ttl_to_seconds

from .time_condition import create_time_condition
from .wrapper import Wrapper

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import TTL, AsyncCallable_T, CacheCondition, DecoratedFunc, Exceptions, KeyOrTemplate, Tags
    from cashews.decorators.bloom import IntOrPair


def _skip_thunder_protection(func: DecoratedFunc) -> DecoratedFunc:
    return func


class DecoratorsWrapper(Wrapper):
    _default_fail_exceptions: tuple[type[Exception], ...] = (Exception,)

    def set_default_fail_exceptions(self, *exc: type[Exception]) -> None:
        self._default_fail_exceptions = exc

    def _wrap_on(self, decorator_fabric, upper: bool, protected=False, **decor_kwargs):
        if decor_kwargs.get("lock") and "ttl" in decor_kwargs and decor_kwargs["ttl"] is None:
            raise ValueError("ttl can't be None with lock")

        if upper:
            return self._wrap_with_condition(decorator_fabric, **decor_kwargs)
        return self._wrap(decorator_fabric, protected=protected, **decor_kwargs)

    def _wrap(
        self,
        decorator_fabric,
        lock=False,
        time_condition=None,
        protected=False,
        **decor_kwargs,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        def _decorator(func: DecoratedFunc) -> DecoratedFunc:
            if time_condition is not None:
                condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
                decor_kwargs["condition"] = condition

            decorator = decorator_fabric(self, **decor_kwargs)(func)
            thunder_protection: Callable[[DecoratedFunc], DecoratedFunc] = _skip_thunder_protection
            if protected:
                thunder_protection = decorators.thunder_protection(key=decor_kwargs.get("key"))

            @wraps(func)
            async def _call(*args, **kwargs):
                self._check_setup()
                if self.is_full_disable:
                    return await func(*args, **kwargs)
                if lock:
                    _locked = decorators.locked(
                        backend=self,
                        key=decor_kwargs.get("key"),
                        ttl=decor_kwargs["ttl"],
                        wait=True,
                    )
                    return await thunder_protection(_locked(decorator))(*args, **kwargs)
                else:
                    return await thunder_protection(decorator)(*args, **kwargs)

            return _call  # type: ignore[return-value]

        return _decorator

    def _wrap_with_condition(
        self,
        decorator_fabric,
        condition,
        lock=False,
        time_condition=None,
        **decor_kwargs,
    ):
        def _decorator(func: AsyncCallable_T) -> AsyncCallable_T:
            _condition = condition
            if time_condition is not None:
                _condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
            decorator_fabric(self, **decor_kwargs)(func)  # to register cache templates

            @wraps(func)
            async def _call(*args, **kwargs):
                self._check_setup()
                if self.is_full_disable:
                    return await func(*args, **kwargs)
                with decorators.context_cache_detect as detect:

                    def new_condition(result, _args, _kwargs, key):
                        if detect.calls:
                            return False
                        return _condition(result, _args, _kwargs, key=key) if _condition else result is not None

                    decorator = decorator_fabric(self, **decor_kwargs, condition=new_condition)
                    if lock:
                        _locked = decorators.locked(
                            backend=self,
                            key=decor_kwargs.get("key"),
                            ttl=decor_kwargs["ttl"],
                            wait=True,
                        )
                        _result = await _locked(decorator(func))(*args, **kwargs)
                    else:
                        _result = await decorator(func)(*args, **kwargs)

                return _result

            return _call

        return _decorator

    def __call__(
        self,
        ttl: TTL,
        key: KeyOrTemplate | None = None,
        condition: CacheCondition = None,
        time_condition: TTL | None = None,
        prefix: str = "",
        upper: bool = False,
        lock: bool = False,
        tags: Tags = (),
        protected: bool = True,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return self._wrap_on(
            decorators.cache,
            upper,
            lock=lock,
            ttl=ttl,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
            tags=tags,
            protected=protected,
        )

    cache = __call__

    def failover(
        self,
        ttl: TTL,
        exceptions: Exceptions = None,
        key: KeyOrTemplate | None = None,
        condition: CacheCondition = None,
        time_condition: TTL | None = None,
        prefix: str = "fail",
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        exceptions = exceptions or self._default_fail_exceptions
        return self._wrap_with_condition(
            decorators.failover,
            ttl=ttl,
            exceptions=exceptions,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def early(
        self,
        ttl: TTL,
        key: KeyOrTemplate | None = None,
        early_ttl: TTL | None = None,
        condition: CacheCondition = None,
        time_condition: TTL | None = None,
        prefix: str = "early",
        upper: bool = False,
        tags: Tags = (),
        background: bool = True,
        protected: bool = True,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return self._wrap_on(
            decorators.early,
            upper,
            ttl=ttl,
            key=key,
            early_ttl=early_ttl,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
            tags=tags,
            background=background,
            protected=protected,
        )

    def soft(
        self,
        ttl: TTL,
        key: KeyOrTemplate | None = None,
        soft_ttl: TTL | None = None,
        exceptions: Exceptions = Exception,
        condition: CacheCondition = None,
        time_condition: TTL | None = None,
        prefix: str = "soft",
        upper: bool = False,
        tags: Tags = (),
        protected: bool = True,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return self._wrap_on(
            decorators.soft,
            upper,
            ttl=ttl,
            key=key,
            soft_ttl=ttl_to_seconds(soft_ttl),
            exceptions=exceptions,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
            tags=tags,
            protected=protected,
        )

    def hit(
        self,
        ttl: TTL,
        cache_hits: int,
        update_after: int = 0,
        key: KeyOrTemplate | None = None,
        condition: CacheCondition = None,
        time_condition: TTL | None = None,
        prefix: str = "hit",
        upper: bool = False,
        tags: Tags = (),
        background: bool = True,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl,
            cache_hits=cache_hits,
            update_after=update_after,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
            tags=tags,
            background=background,
        )

    def dynamic(
        self,
        ttl: TTL = 60 * 60 * 24,
        key: KeyOrTemplate | None = None,
        condition: CacheCondition = None,
        time_condition: TTL | None = None,
        prefix: str = "dynamic",
        upper: bool = False,
        tags: Tags = (),
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl,
            cache_hits=3,
            update_after=1,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
            tags=tags,
        )

    def iterator(
        self,
        ttl: TTL,
        key: KeyOrTemplate | None = None,
        condition: CacheCondition = None,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return decorators.iterator(
            self,  # type: ignore[arg-type]
            ttl=ttl,
            key=key,
            condition=get_cache_condition(condition),
        )

    def invalidate(
        self,
        key_template: KeyOrTemplate,
        args_map: dict[str, str] | None = None,
        defaults: dict | None = None,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return validation.invalidate(
            backend=self,  # type: ignore[arg-type]
            key_template=key_template,
            args_map=args_map,
            defaults=defaults,
        )

    def circuit_breaker(
        self,
        errors_rate: int,
        period: TTL,
        ttl: TTL,
        half_open_ttl: TTL = None,
        exceptions: Exceptions = None,
        key: KeyOrTemplate | None = None,
        min_calls: int = 1,
        prefix: str = "circuit_breaker",
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        _exceptions = exceptions or self._default_fail_exceptions
        return decorators.circuit_breaker(
            backend=self,  # type: ignore[arg-type]
            errors_rate=errors_rate,
            period=ttl_to_seconds(period),
            ttl=ttl_to_seconds(ttl),
            half_open_ttl=ttl_to_seconds(half_open_ttl),
            exceptions=_exceptions,
            min_calls=min_calls,
            key=key,
            prefix=prefix,
        )

    def rate_limit(
        self,
        limit: int,
        period: TTL,
        ttl: TTL | None = None,
        action: Callable | None = None,
        prefix="rate_limit",
        key: KeyOrTemplate | None = None,
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:  # pylint: disable=too-many-arguments
        return decorators.rate_limit(
            backend=self,  # type: ignore[arg-type]
            limit=limit,
            period=period,
            ttl=ttl,
            action=action,
            key=key,
            prefix=prefix,
        )

    def slice_rate_limit(
        self,
        limit: int,
        period: TTL,
        key: KeyOrTemplate | None = None,
        action: Callable | None = None,
        prefix="srl",
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return decorators.slice_rate_limit(
            backend=self,  # type: ignore[arg-type]
            limit=limit,
            period=period,
            key=key,
            action=action,
            prefix=prefix,
        )

    def locked(
        self,
        ttl: TTL | None = None,
        key: KeyOrTemplate | None = None,
        wait: bool = True,
        prefix: str = "locked",
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return decorators.locked(
            backend=self,  # type: ignore[arg-type]
            ttl=ttl,
            key=key,
            wait=wait,
            prefix=prefix,
        )

    def bloom(
        self,
        *,
        capacity: int,
        name: KeyOrTemplate | None = None,
        false_positives: float | int = 1,
        check_false_positive: bool = True,
        prefix: str = "bloom",
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return decorators.bloom(
            backend=self,  # type: ignore[arg-type]
            name=name,
            false_positives=false_positives,
            capacity=capacity,
            check_false_positive=check_false_positive,
            prefix=prefix,
        )

    def dual_bloom(
        self,
        *,
        capacity: int,
        name: KeyOrTemplate | None = None,
        false: IntOrPair = 1,
        no_collisions: bool = False,
        prefix: str = "dual_bloom",
    ) -> Callable[[DecoratedFunc], DecoratedFunc]:
        return decorators.dual_bloom(
            backend=self,  # type: ignore[arg-type]
            name=name,
            false=false,
            no_collisions=no_collisions,
            capacity=capacity,
            prefix=prefix,
        )

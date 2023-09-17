from functools import wraps
from typing import Callable, Dict, Optional, Tuple, Type, Union

from cashews import decorators, validation
from cashews._typing import TTL, AsyncCallable_T, CacheCondition, Exceptions, KeyOrTemplate, Tags
from cashews.cache_condition import get_cache_condition
from cashews.ttl import ttl_to_seconds

from .time_condition import create_time_condition
from .wrapper import Wrapper


def _skip_thunder_protection(func: Callable) -> Callable:
    return func


class DecoratorsWrapper(Wrapper):
    _default_fail_exceptions: Tuple[Type[Exception], ...] = (Exception,)

    def set_default_fail_exceptions(self, *exc: Type[Exception]) -> None:
        self._default_fail_exceptions = exc

    def _wrap_on(self, decorator_fabric, upper: bool, protected=False, **decor_kwargs):
        if decor_kwargs.get("lock") and "ttl" in decor_kwargs and decor_kwargs["ttl"] is None:
            raise ValueError("ttl can't be None with lock")

        if upper:
            return self._wrap_with_condition(decorator_fabric, **decor_kwargs)
        return self._wrap(decorator_fabric, protected=protected, **decor_kwargs)

    def _wrap(self, decorator_fabric, lock=False, time_condition=None, protected=False, **decor_kwargs):
        def _decorator(func: AsyncCallable_T) -> AsyncCallable_T:
            if time_condition is not None:
                condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
                decor_kwargs["condition"] = condition

            decorator = decorator_fabric(self, **decor_kwargs)(func)
            thunder_protection = _skip_thunder_protection
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

            return _call

        return _decorator

    def _wrap_with_condition(self, decorator_fabric, condition, lock=False, time_condition=None, **decor_kwargs):
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
        key: Optional[KeyOrTemplate] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "",
        upper: bool = False,
        lock: bool = False,
        tags: Tags = (),
        protected: bool = True,
    ):
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
        key: Optional[KeyOrTemplate] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "fail",
    ):
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
        key: Optional[KeyOrTemplate] = None,
        early_ttl: Optional[TTL] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "early",
        upper: bool = False,
        tags: Tags = (),
        protected: bool = True,
    ):
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
            protected=protected,
        )

    def soft(
        self,
        ttl: TTL,
        key: Optional[KeyOrTemplate] = None,
        soft_ttl: Optional[TTL] = None,
        exceptions: Exceptions = Exception,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "soft",
        upper: bool = False,
        tags: Tags = (),
        protected: bool = True,
    ):
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
        key: Optional[KeyOrTemplate] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "hit",
        upper: bool = False,
        tags: Tags = (),
    ):
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
        )

    def dynamic(
        self,
        ttl: TTL = 60 * 60 * 24,
        key: Optional[KeyOrTemplate] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "dynamic",
        upper: bool = False,
        tags: Tags = (),
    ):
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
        key: Optional[str] = None,
        condition: CacheCondition = None,
    ):
        return decorators.iterator(
            self,
            ttl=ttl,
            key=key,
            condition=get_cache_condition(condition),
        )

    def invalidate(
        self,
        func,
        args_map: Optional[Dict[str, str]] = None,
        defaults: Optional[Dict] = None,
    ):
        return validation.invalidate(
            backend=self,
            target=func,
            args_map=args_map,
            defaults=defaults,
        )

    invalidate_func = validation.invalidate_func

    def circuit_breaker(
        self,
        errors_rate: int,
        period: TTL,
        ttl: TTL,
        half_open_ttl: TTL = None,
        exceptions: Exceptions = None,
        key: Optional[KeyOrTemplate] = None,
        min_calls: int = 1,
        prefix: str = "circuit_breaker",
    ):
        _exceptions = exceptions or self._default_fail_exceptions
        return decorators.circuit_breaker(
            backend=self,
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
        ttl: Optional[TTL] = None,
        action: Optional[Callable] = None,
        prefix="rate_limit",
        key: Optional[KeyOrTemplate] = None,
    ):  # pylint: disable=too-many-arguments
        return decorators.rate_limit(
            backend=self,
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
        key: Optional[KeyOrTemplate] = None,
        action: Optional[Callable] = None,
        prefix="srl",
    ):
        return decorators.slice_rate_limit(
            backend=self,
            limit=limit,
            period=period,
            key=key,
            action=action,
            prefix=prefix,
        )

    def locked(
        self,
        ttl: Optional[TTL] = None,
        key: Optional[KeyOrTemplate] = None,
        wait: bool = True,
        prefix: str = "locked",
    ):
        return decorators.locked(
            backend=self,
            ttl=ttl,
            key=key,
            wait=wait,
            prefix=prefix,
        )

    def bloom(
        self,
        *,
        capacity: int,
        name: Optional[KeyOrTemplate] = None,
        false_positives: Optional[Union[float, int]] = 1,
        check_false_positive: bool = True,
        prefix: str = "bloom",
    ):
        return decorators.bloom(
            backend=self,
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
        name: Optional[KeyOrTemplate] = None,
        false: Optional[Union[float, int]] = 1,
        no_collisions: bool = False,
        prefix: str = "dual_bloom",
    ):
        return decorators.dual_bloom(
            backend=self,
            name=name,
            false=false,
            no_collisions=no_collisions,
            capacity=capacity,
            prefix=prefix,
        )

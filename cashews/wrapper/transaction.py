from contextvars import ContextVar
from enum import Enum
from functools import wraps
from typing import Optional, Tuple

from cashews._typing import AsyncCallable_T, Middleware
from cashews.backends.interface import Backend
from cashews.backends.transaction import LockTransactionBackend, TransactionBackend

from .wrapper import Wrapper

_transaction = ContextVar("transaction", default=None)


class TransactionMode(Enum):
    FAST = "fast"  # simple inmemory impl, allow to have lost updates,
    LOCKED = "locked"  # lock per key - not allow to work in parallel with the same key
    SERIALIZABLE = "serializable"  # global lock - not allow any parallel changes


class TransactionWrapper(Wrapper):
    transaction_mode = TransactionMode.LOCKED
    transaction_timeout = 10

    def set_transaction_timeout(self, timeout: int):
        self.transaction_timeout = timeout

    def set_transaction_mode(self, mode: TransactionMode):
        self.transaction_mode = mode

    def _get_backend_and_config(self, key: str) -> Tuple[Backend, Tuple[Middleware, ...]]:
        backend, config = super()._get_backend_and_config(key)
        tx: Optional[Transaction] = _transaction.get()
        if not tx:
            return backend, config
        return tx.wrap(backend), config

    def transaction(
        self, mode: Optional[TransactionMode] = None, timeout: Optional[float] = None
    ) -> "TransactionContextDecorator":
        mode = mode or self.transaction_mode
        timeout = timeout or self.transaction_timeout
        return TransactionContextDecorator(mode, timeout)


class TransactionContextDecorator:
    __slots__ = ["_mode", "_timeout", "_inner"]

    def __init__(self, mode: Optional[TransactionMode] = None, timeout: Optional[float] = None):
        self._mode = mode
        self._timeout = timeout
        self._inner = False

    @property
    def current_tx(self) -> Optional["Transaction"]:
        return _transaction.get()

    async def __aenter__(self) -> "Transaction":
        if self.current_tx:
            self._inner = True
            return self.current_tx
        return self.start()

    def start(self) -> "Transaction":
        tx = Transaction(self._mode, self._timeout)
        _transaction.set(tx)
        return tx

    def close(self):
        _transaction.set(None)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if not self.current_tx or self._inner:
            self._inner = False
            return
        if not exc_tb:
            await self.commit()
        else:
            await self.rollback()
        self.close()

    def __call__(self, func: AsyncCallable_T) -> AsyncCallable_T:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with self:
                return await func(*args, **kwargs)

        return wrapper

    async def commit(self):
        if self.current_tx:
            await self.current_tx.commit()

    async def rollback(self):
        if self.current_tx:
            await self.current_tx.rollback()


class Transaction:
    __slots__ = ["_mode", "_timeout", "_backends"]

    def __init__(self, mode: Optional[TransactionMode] = None, timeout: Optional[float] = None):
        self._mode = mode
        self._timeout = timeout
        self._backends = {}

    def wrap(self, backend: Backend) -> Backend:
        if id(backend) not in self._backends:
            self._backends[id(backend)] = self._get_tx_backend(backend)
        return self._backends[id(backend)]

    def _get_tx_backend(self, backend: Backend) -> "TransactionBackend":
        if self._mode == TransactionMode.FAST:
            return TransactionBackend(backend)
        if self._mode == TransactionMode.SERIALIZABLE:
            return LockTransactionBackend(backend, serializable=True, timeout=self._timeout)
        return LockTransactionBackend(backend, serializable=False, timeout=self._timeout)

    async def commit(self):
        for tx_backend in list(self._backends.values()):
            await tx_backend.commit()

    async def rollback(self):
        for tx_backend in list(self._backends.values()):
            await tx_backend.rollback()

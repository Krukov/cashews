from prometheus_client import Counter, Histogram

from cashews import cache
from cashews._typing import Middleware
from cashews.backends.interface import Backend
from cashews.commands import Command

_DEFAULT_METRIC = Histogram(
    "cashews_operations_latency_seconds",
    "Latency of different operations with a cache",
    labelnames=["operation", "backend_class", "tag"],
)
_HIT_MISS = Counter(
    "cashews_get_operation",
    "Count of hits or missed GET operations",
    labelnames=["result", "backend_class", "tag"],
)


def create_metrics_middleware(with_tag: bool = False) -> Middleware:
    async def metrics_middleware(call, cmd: Command, backend: Backend, *args, **kwargs):
        with _DEFAULT_METRIC.time() as metric:
            tag = ""

            if with_tag and "key" in kwargs:
                tags = cache.get_key_tags(kwargs["key"])
                tag = next((t for t in tags), "")
            metric.labels(operation=cmd.value, backend_class=backend.__class__.__name__, tag=tag)
            result = await call(*args, **kwargs)
            if cmd is Command.GET:
                op_result = "hit" if result is not kwargs["default"] else "miss"
                _HIT_MISS.labels(result=op_result, backend_class=backend.__class__.__name__, tag=tag).inc()
            return result

    return metrics_middleware

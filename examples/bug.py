import asyncio
from collections.abc import Mapping
from typing import Any

from cashews import cache, default_formatter

cache.setup("mem://?size=1000000&check_interval=5")


@default_formatter.register("get_item", preformat=False)
def _getitem_func(mapping: Mapping[str, Any], key: str) -> str:
    try:
        return str(mapping[key])
    except Exception as e:
        # when key/tag matching, this may be called with the rendered value
        raise RuntimeError(f"{mapping=}, {key=}") from e


@cache(
    ttl="1h",
    key="prefix:keys:{mapping:get_item(bar)}",
    tags=["prefix:tags:{mapping:get_item(bar)}"],
)
async def foo(mapping: str) -> None:
    print("Foo", mapping)


@cache.invalidate("prefix:keys:{mapping:get_item(bar)}")
async def bar(mapping: str) -> None:
    print("Bar", mapping)


async def main() -> None:
    await foo({"bar": "baz"})
    await bar({"bar": "baz"})


if __name__ == "__main__":
    asyncio.run(main())

# prints Foo {'bar': 'baz'}
# prints Bar {'bar': 'baz'}

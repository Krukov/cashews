import asyncio

from cashews import cache, default_formatter
from cashews.formatter import get_templates_for_func

cache.setup("mem://")


@cache(ttl="10m", prefix="auto")
async def auto_key(foo, **kwargs):
    return


@cache(ttl="10m", key="my:key:{foo}", prefix="manual")
async def manual_key(foo, **kwargs):
    return


INT_TO_STR_MAP = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}


@default_formatter.register("human")
def _human(value, upper=False):
    res = ""
    for char in value:
        if res:
            res += "-"
        res += INT_TO_STR_MAP.get(char)
    if upper:
        return res.upper()
    return res


@cache(ttl="10m", key="my:key:{foo:human(t)}")
async def key_with_format(foo):
    return


async def main():
    await _call(auto_key, "fooval", key="test")
    await _call(manual_key, "fooval", key="test")
    await _call(key_with_format, 521)


async def _call(function, *args, **kwargs):
    await function(*args, **kwargs)
    with cache.detect as detector:
        await function(*args, **kwargs)
        key = list(detector.keys.keys())[-1]

    template = next(get_templates_for_func(function))

    print(
        f"""
    function "{function.__name__}" called with args={args} and kwargs={kwargs}
    the key template:    {template}
    the key:             {key}
    """
    )


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import uuid

from cashews.decorators.bloom import bloom, counting_bloom, dual_bloom
from cashews.backends.memory import Memory

b = Memory(size=1_000)
capacity = 20000
expect_false_positive = 1
fill_elements = 1000
test_n = 10000

call_count = 0


@dual_bloom(b, capacity=capacity, false_positives=expect_false_positive)
async def _simple_bloom(key):
    await asyncio.sleep(0)
    global call_count
    call_count += 1
    return str(key)[0] == "c"


async def main():
    global call_count
    all = {}
    # fill bloom
    for _ in range(fill_elements):
        k = uuid.uuid4()
        v = await _simple_bloom(k)
        all[k] = v

    call_count = 0
    for _ in range(test_n):
        await _simple_bloom(uuid.uuid4())

    print("False not exist: ", 100 * (test_n - call_count)/test_n)
    print("Expect False positive: ", expect_false_positive)

    call_count = 0
    false = 0
    for k, v in all.items():
        if not await _simple_bloom(k) == v:
            false += 1

    print("False exist and calls: ", false, call_count)


asyncio.run(main())
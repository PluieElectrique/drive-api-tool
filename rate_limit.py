import asyncio
import collections
import time

# Very short sleeps will probably just waste CPU. And, I'm not sure if
# `time.monotonic()` has a guaranteed resolution (e.g. 1 millisecond).
MIN_SLEEP = 0.020


# Rate-limited version of `asyncio.as_completed`. Not thread-safe. Up to
# `max_concurrent` tasks can run simultaneously. Up to `quota` tasks can be
# running in any time interval of `period` seconds.
def rate_limited_as_completed(coros, max_concurrent, quota, period=1):
    if max_concurrent > quota:
        raise ValueError("max_concurrent must be less than or equal to quota")

    # Without a list, we have to track the length of `coros` so that we know
    # when to stop calling `_wait_to_get()`. This is messy. And, holding all
    # coroutines in memory shouldn't be an issue unless you have millions.
    coros = list(coros)

    semaphore = asyncio.Semaphore(max_concurrent)
    done = asyncio.Queue()
    # Tracking end times instead of start times means that we stay under the
    # rate limit even if tasks take longer than `period` seconds.
    end_times = collections.deque()

    def _flush_times():
        now = time.monotonic()
        while end_times and (now - end_times[0]) > period:
            end_times.popleft()

    async def _submit():
        for coro in coros:
            await semaphore.acquire()
            _flush_times()
            # Relying on the internal `_value` isn't great, but we need to know
            # how many active tasks there are. We aren't thread-safe, anyway.
            if (max_concurrent - semaphore._value - 1) + len(end_times) >= quota:
                sleep_time = max(period - (time.monotonic() - end_times[0]), MIN_SLEEP)
                await asyncio.sleep(sleep_time)
                # If the sleep time is inaccurate, we might wake up too early.
                # This ensures that we make progress on `end_times`. It might
                # put us over the rate limit if we have razor-thin tolerances,
                # but that seems too messy to handle.
                end_times.popleft()

            task = asyncio.create_task(coro)
            task.add_done_callback(_on_completion)

    def _on_completion(f):
        end_times.append(time.monotonic())
        done.put_nowait(f)
        semaphore.release()

    async def _wait_to_get():
        f = await done.get()
        return f.result()

    asyncio.create_task(_submit())
    for _ in range(len(coros)):
        yield _wait_to_get()

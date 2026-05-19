import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


async def gather_bounded(
    items: Iterable[T],
    fn: Callable[[T], Awaitable[R]],
    *,
    concurrency: int,
    return_exceptions: bool = True,
) -> list[R | BaseException]:
    sem = asyncio.Semaphore(concurrency)

    async def _wrapped(item: T) -> R:
        async with sem:
            return await fn(item)

    return await asyncio.gather(
        *(_wrapped(item) for item in items),
        return_exceptions=return_exceptions,
    )

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from nuget_pipeline.config import settings
from nuget_pipeline.utils.logging import get_logger

log = get_logger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass
class HTTPMetrics:
    """Per-run counters for what the HTTP layer is seeing.

    Bound via `http_metrics_var` for the duration of a sync run so
    `get_json` can increment without callers having to thread a metrics
    object through every signature. Always present on `SyncContext`.
    """

    requests: int = 0
    http_2xx: int = 0
    http_4xx: int = 0
    http_429: int = 0  # subset of 4xx, surfaced separately because rate limits matter most
    http_5xx: int = 0
    transport_errors: int = 0
    retries_exhausted: int = 0


http_metrics_var: ContextVar[HTTPMetrics | None] = ContextVar("http_metrics", default=None)


class RetryableHTTPError(Exception):
    def __init__(self, status: int, url: str, body_preview: str) -> None:
        super().__init__(f"HTTP {status} for {url}: {body_preview}")
        self.status = status
        self.url = url


@asynccontextmanager
async def http_client(
    *,
    timeout_s: float | None = None,
    user_agent: str | None = None,
) -> AsyncIterator[httpx.AsyncClient]:
    timeout = httpx.Timeout(timeout_s or settings.http_request_timeout_s)
    headers = {
        "User-Agent": user_agent or settings.nuget_user_agent,
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(
        timeout=timeout,
        headers=headers,
        follow_redirects=True,
    ) as client:
        yield client


async def get_json(client: httpx.AsyncClient, url: str) -> dict:
    retrying = AsyncRetrying(
        reraise=True,
        stop=stop_after_attempt(settings.http_max_retries),
        wait=wait_exponential_jitter(initial=1.0, max=30.0),
        retry=retry_if_exception_type((httpx.TransportError, RetryableHTTPError)),
    )

    metrics = http_metrics_var.get()

    try:
        async for attempt in retrying:
            with attempt:
                if metrics is not None:
                    metrics.requests += 1
                try:
                    response = await client.get(url)
                except httpx.TransportError:
                    if metrics is not None:
                        metrics.transport_errors += 1
                    raise

                status = response.status_code
                if metrics is not None:
                    if 200 <= status < 300:
                        metrics.http_2xx += 1
                    elif 400 <= status < 500:
                        metrics.http_4xx += 1
                        if status == 429:
                            metrics.http_429 += 1
                    elif 500 <= status < 600:
                        metrics.http_5xx += 1

                if status in RETRYABLE_STATUS:
                    body = response.text[:500]
                    raise RetryableHTTPError(status, url, body)
                response.raise_for_status()
                return response.json()
    except (RetryableHTTPError, httpx.TransportError) as e:
        # tenacity's reraise=True surfaces the underlying exception (not
        # RetryError) once attempts are exhausted; count it as exhaustion.
        if metrics is not None:
            metrics.retries_exhausted += 1
        log.error("http.retries_exhausted", url=url, error=str(e))
        raise
    except RetryError as e:
        # Defensive: if reraise behaviour ever changes, still record it.
        if metrics is not None:
            metrics.retries_exhausted += 1
        log.error("http.retries_exhausted", url=url, error=str(e))
        raise

    raise RuntimeError("unreachable")  # pragma: no cover

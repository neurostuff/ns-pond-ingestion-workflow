"""How the provider clients ask politely and ask again: what a failed request says about the
provider, how far apart requests are kept, and which failures are retried."""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

import httpx
import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


def transient(exc: BaseException) -> bool:
    """A provider that was down or busy, not one that refused the request; only these are
    worth asking again: a timeout, a connection dropped before or during the answer, a body
    cut short or not JSON (a gateway's page in place of the answer), 429 or 5xx."""
    if isinstance(exc, (requests.HTTPError, httpx.HTTPStatusError)):
        status = getattr(exc.response, "status_code", None)
        return status == 429 or (status is not None and status >= 500)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout,
                            requests.exceptions.ChunkedEncodingError, requests.JSONDecodeError,
                            httpx.TransportError))


class Pacer:
    """Keeps requests at least an interval apart; share one among the callers a provider
    counts as one."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self, interval: float) -> None:
        with self._lock:
            wait = self._last + interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


def wait_retry_after(longest: float, backoff: float) -> Callable:
    """A tenacity wait: the provider's Retry-After when it sends one, at most `longest`
    seconds, else an exponential backoff to `backoff` seconds."""
    def wait(retry_state) -> float:
        response = getattr(retry_state.outcome.exception(), "response", None)
        after = response.headers.get("Retry-After", "") if response is not None else ""
        if after.strip().isdigit():
            return min(float(after), longest)
        return wait_exponential(multiplier=1, max=backoff)(retry_state)

    return wait


def retry_transient(attempts: int, max_wait: float, *, wait: Optional[Callable] = None,
                    retry_on: Callable[[BaseException], bool] = transient):
    """Ask again, up to `attempts` times in all, after `retry_on` failures; the last failure is
    raised as it was. Waits grow exponentially to `max_wait` seconds unless `wait` says."""
    return retry(stop=stop_after_attempt(attempts),
                 wait=wait or wait_exponential(multiplier=1, max=max_wait),
                 retry=retry_if_exception(retry_on), reraise=True)

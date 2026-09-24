"""Stop ACE's table scraper from paying full retries to a host that is gone.

ACE fetches each table's expansion page while parsing an article, through
``ace.scrape.get_url``, which retries five times with a ``2 ** n`` backoff. A
host that refuses connections therefore costs about 31 seconds of sleeping per
URL, and an article with fourteen tables costs seven minutes -- for nothing. A
2026 extraction run over 35,847 ACE articles was projected at 17 hours, almost
all of it waiting on `jn.physiology.org`, which no longer resolves.

The publisher's own successor is no better: `journals.physiology.org` answers
403 to everything, its journal homepage included. So rewriting the host does
not rescue those articles, and the win here is spending nothing on them rather
than 31 seconds each.
"""

from __future__ import annotations

import threading
from collections import Counter
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from ingestion_workflow.services.logging import get_logger

logger = get_logger(__name__)

#: Hosts that moved somewhere that still serves the same paths. Kept for the
#: cases where a rewrite does work; it is deliberately not a dumping ground for
#: hosts whose successor blocks us, since a 403 is no cheaper than a 404.
HOST_MOVES: dict[str, str] = {}

#: Transport failures a host may accumulate before it is presumed gone. One is
#: too eager -- a single timeout happens to live hosts under load.
DEAD_AFTER = 3


class DeadHosts:
    """Per-host transport failure counts, shared across a run."""

    def __init__(self, dead_after: int = DEAD_AFTER) -> None:
        self._dead_after = dead_after
        self._failures: Counter[str] = Counter()
        self._dead: set[str] = set()
        self._lock = threading.Lock()

    def is_dead(self, host: str) -> bool:
        with self._lock:
            return host in self._dead

    def record_failure(self, host: str) -> bool:
        """Count a transport failure; True once the host is presumed gone."""
        with self._lock:
            self._failures[host] += 1
            if self._failures[host] >= self._dead_after and host not in self._dead:
                self._dead.add(host)
                logger.warning(
                    "host %s failed %d times; skipping its remaining tables",
                    host,
                    self._failures[host],
                )
            return host in self._dead

    def record_success(self, host: str) -> None:
        with self._lock:
            self._failures.pop(host, None)
            self._dead.discard(host)

    @property
    def dead(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._dead)


def rewrite(url: str) -> str:
    """Point a moved host at its successor, leaving everything else alone."""
    parts = urlsplit(url)
    target = HOST_MOVES.get(parts.netloc)
    if not target:
        return url
    return urlunsplit((parts.scheme, target, parts.path, parts.query, parts.fragment))


def install(dead_hosts: Optional[DeadHosts] = None, n_retries: int = 2) -> DeadHosts:
    """Wrap ``ace.scrape.get_url`` with the rewrite and the breaker.

    ``ace.sources`` reaches it as ``scrape.get_url(...)``, an attribute lookup
    at call time, so replacing the module attribute is enough and no ACE source
    needs editing.
    """
    from ace import scrape

    tracker = dead_hosts or DeadHosts()
    original = getattr(scrape, "_ns_pond_original_get_url", None) or scrape.get_url

    def guarded(url, *args, **kwargs):
        url = rewrite(url)
        host = urlsplit(url).netloc
        if tracker.is_dead(host):
            return None
        kwargs.setdefault("n_retries", n_retries)
        try:
            result = original(url, *args, **kwargs)
        except Exception:
            tracker.record_failure(host)
            raise
        if result is None:
            tracker.record_failure(host)
        else:
            tracker.record_success(host)
        return result

    scrape._ns_pond_original_get_url = original
    scrape.get_url = guarded
    return tracker


__all__ = ["DeadHosts", "HOST_MOVES", "install", "rewrite"]

"""One item's failure kept to itself: the rule each batched loop shares."""

from __future__ import annotations

import logging
from typing import Callable, Dict, Hashable, Iterable, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K", bound=Hashable)


def error_text(exc: BaseException) -> str:
    """How an error is recorded: its type and message."""
    return f"{type(exc).__name__}: {exc}"


def parsed_each(
    items: Iterable[T],
    parse: Callable[[T], Optional[Tuple[K, R]]],
    what: str,
    key: Callable[[T], Optional[K]] = lambda item: None,
) -> Tuple[Dict[K, R], Dict[K, str]]:
    """`parse` applied to each item alone: the `(key, result)` of each that parsed, and the
    error of each that raised, by `key(item)` (an item it gives no key for is logged only).
    `parse` returning None leaves the item out."""
    results: Dict[K, R] = {}
    failed: Dict[K, str] = {}
    for item in items:
        try:
            parsed = parse(item)
        except Exception as exc:  # noqa: BLE001 - this item's failure, not the batch's
            name = _key_of(key, item)
            logger.warning("%s %s could not be read: %s", what, name or "record", exc)
            if name is not None:
                failed[name] = error_text(exc)
            continue
        if parsed is not None:
            results[parsed[0]] = parsed[1]
    return results, failed


def _key_of(key: Callable, item) -> Optional[Hashable]:
    try:
        return key(item)
    except Exception:  # noqa: BLE001 - an item too broken to name is still left alone
        return None

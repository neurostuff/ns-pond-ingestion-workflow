"""Which article an id is now, once articles have been merged."""

from __future__ import annotations

from typing import Callable, Collection, Dict, List, Optional


def follow(article_id: str, merged_into: Callable[[str], Optional[str]]) -> str:
    """The article `article_id` is now. A cycle ends where it closes."""
    seen = set()
    while article_id not in seen:
        seen.add(article_id)
        survivor = merged_into(article_id)
        if not survivor:
            return article_id
        article_id = survivor
    return article_id


class Merges:
    """Every merged_into pair as one read, for resolving many ids without a query each.

    A snapshot: it does not see a merge made after it was taken, so write paths,
    which merge as they go, follow through the database instead.
    """

    def __init__(self, merged_into: Dict[str, str]):
        self._into = merged_into

    def live(self, article_id: str) -> str:
        return follow(article_id, self._into.get)

    def retired_into(self, article_ids: Collection[str]) -> List[str]:
        """Every retired id whose live article is one of `article_ids`."""
        return [retired for retired in self._into if self.live(retired) in article_ids]

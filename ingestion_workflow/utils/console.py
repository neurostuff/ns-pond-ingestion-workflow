"""Progress bars, shown only when a console run asked for them."""

from __future__ import annotations

from typing import Optional

from tqdm.auto import tqdm


def progress_bar(settings, total: int, desc: str, *, unit: str = "item") -> Optional[tqdm]:
    if not getattr(settings, "show_progress", False) or total <= 0:
        return None
    return tqdm(total=total, desc=desc, leave=False, unit=unit, dynamic_ncols=True)


__all__ = ["progress_bar"]

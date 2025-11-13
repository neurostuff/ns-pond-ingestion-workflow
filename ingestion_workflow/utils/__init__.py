import re

from .progress import emit_progress, progress_callback


def slugify(value: str) -> str:
    """
    Create a filesystem-safe slug from input strings
    """
    mapped = re.sub(r"[^a-z0-9]+", "-", value.lower())
    slug = mapped.strip("-")
    return slug


__all__ = ["emit_progress", "progress_callback", "slugify"]

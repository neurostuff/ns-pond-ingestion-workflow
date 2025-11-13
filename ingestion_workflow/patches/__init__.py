"""Runtime patches that adjust third-party behavior for the workflow."""

from __future__ import annotations

from .ace_patch import apply_patch as apply_ace_patch

__all__ = ["apply_ace_patch"]

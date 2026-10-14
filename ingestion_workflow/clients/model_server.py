"""What every stage that asks a model server shares."""

from __future__ import annotations


class ModelUnavailable(RuntimeError):
    """The model server cannot be used in this run: it cannot be reached or it is misconfigured.

    Raised from a stage's `plan` or `execute`, with the reason as its message. The
    scheduler stops the stage for the rest of the run and leaves its articles pending.
    """

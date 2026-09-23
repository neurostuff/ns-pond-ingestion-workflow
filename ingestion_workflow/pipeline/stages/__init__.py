"""The six stages, in the order they run."""

from .analyses import AnalysesStage
from .download import DownloadStage
from .extract import ExtractStage
from .metadata import MetadataStage
from .sync import SyncStage
from .upload import UploadStage

#: Canonical order. A stage may only require one that appears before it.
STAGE_ORDER = ("download", "extract", "metadata", "analyses", "upload", "sync")

STAGE_TYPES = {
    "download": DownloadStage,
    "extract": ExtractStage,
    "metadata": MetadataStage,
    "analyses": AnalysesStage,
    "upload": UploadStage,
    "sync": SyncStage,
}


def build(names, settings):
    """Instantiate the requested stages in canonical order."""
    wanted = {name.lower() for name in names} if names else set(STAGE_ORDER)
    unknown = wanted - set(STAGE_ORDER)
    if unknown:
        raise ValueError(f"Unknown stages: {', '.join(sorted(unknown))}")
    return [STAGE_TYPES[name](settings) for name in STAGE_ORDER if name in wanted]


__all__ = [
    "AnalysesStage",
    "DownloadStage",
    "ExtractStage",
    "MetadataStage",
    "STAGE_ORDER",
    "STAGE_TYPES",
    "SyncStage",
    "UploadStage",
    "build",
]

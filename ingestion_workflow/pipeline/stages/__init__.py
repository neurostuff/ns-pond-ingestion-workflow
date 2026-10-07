"""The stages, in the order they run."""

from .analyses import AnalysesStage
from .download import DownloadStage
from .extract import ExtractStage
from .metadata import MetadataStage
from .prose import ProseStage
from .resolve import ResolveStage
from .space import SpaceStage
from .sync import SyncStage
from .triage import TriageStage
from .upload import UploadStage

#: Canonical order. A stage may only require one that appears before it.
#: `triage` sits between `metadata` and `analyses`: it needs the tables
#: `extract` keeps and it decides which of them `analyses` spends a call on.
#: `space` sits between `analyses` and `upload`, so a table goes out with the
#: space its article states. `prose` and `resolve` run only when `prose_model`
#: is set: prose reads the coordinates written in the text, resolve
#: merges them with the tables', and `space` then reads resolve instead.
STAGE_ORDER = ("download", "extract", "metadata", "triage", "analyses",
               "prose", "resolve", "space", "upload", "sync")

#: Stages that exist only when prose is switched on.
PROSE_STAGES = ("prose", "resolve")

STAGE_TYPES = {
    "download": DownloadStage,
    "extract": ExtractStage,
    "metadata": MetadataStage,
    "triage": TriageStage,
    "analyses": AnalysesStage,
    "prose": ProseStage,
    "resolve": ResolveStage,
    "space": SpaceStage,
    "upload": UploadStage,
    "sync": SyncStage,
}


def build(names, settings):
    """Instantiate the requested stages in canonical order."""
    enabled = bool(getattr(settings, "prose_model", None))
    wanted = {name.lower() for name in names} if names else {
        name for name in STAGE_ORDER if enabled or name not in PROSE_STAGES}
    unknown = wanted - set(STAGE_ORDER)
    if unknown:
        raise ValueError(f"Unknown stages: {', '.join(sorted(unknown))}")
    if wanted & set(PROSE_STAGES) and not enabled:
        raise ValueError("the prose and resolve stages need prose_model: space reads "
                         "analyses without it, so their output would go nowhere")
    return [STAGE_TYPES[name](settings) for name in STAGE_ORDER if name in wanted]


__all__ = [
    "AnalysesStage",
    "DownloadStage",
    "ExtractStage",
    "MetadataStage",
    "PROSE_STAGES",
    "ProseStage",
    "ResolveStage",
    "STAGE_ORDER",
    "STAGE_TYPES",
    "SpaceStage",
    "SyncStage",
    "TriageStage",
    "UploadStage",
    "build",
]

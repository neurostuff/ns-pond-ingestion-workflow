"""The stages, in the order they run."""

from .analyses import AnalysesStage
from .download import DownloadStage
from .extract import ExtractStage
from .metadata import MetadataStage
from .passages import PassagesStage
from .prose import ProseStage
from .references import ReferencesStage
from .resolve import ResolveStage
from .space import SpaceStage
from .sync import SyncStage
from .triage import TriageStage
from .upload import UploadStage

#: Canonical order. A stage may only require one that appears before it.
#: `triage` sits between `metadata` and `analyses`: it needs the tables
#: `extract` keeps and it decides which of them `analyses` spends a call on.
#: `space` sits between `analyses` and `upload`, so a table goes out with the
#: space its article states.
#:
#: Prose has the same two steps as tables, and they run only when
#: `prose_model` is set: `passages` is its extraction (the download's Methods
#: and Results, the passages holding coordinates) and `prose` its model call.
#: `metadata` runs between them, after both extractions, so that an article
#: found only through its prose is fetched metadata and the prose model reads
#: its title and abstract. `resolve` merges prose results with the tables',
#: and `space` then reads resolve instead of analyses.
#:
#: `references` reads each extraction's reference list and citations; nothing
#: downstream reads it yet, so it runs only when asked for by name.
STAGE_ORDER = ("download", "extract", "references", "passages", "metadata", "triage", "analyses",
               "prose", "resolve", "space", "upload", "sync")

#: Stages that exist only when prose is switched on.
PROSE_STAGES = ("passages", "prose", "resolve")

#: Stages a run without `--stage` leaves out.
OPT_IN_STAGES = ("references",)

STAGE_TYPES = {
    "download": DownloadStage,
    "extract": ExtractStage,
    "metadata": MetadataStage,
    "triage": TriageStage,
    "analyses": AnalysesStage,
    "passages": PassagesStage,
    "prose": ProseStage,
    "references": ReferencesStage,
    "resolve": ResolveStage,
    "space": SpaceStage,
    "upload": UploadStage,
    "sync": SyncStage,
}


def build(names, settings):
    """Instantiate the requested stages in canonical order."""
    enabled = bool(getattr(settings, "prose_model", None))
    wanted = {name.lower() for name in names} if names else {
        name for name in STAGE_ORDER
        if (enabled or name not in PROSE_STAGES) and name not in OPT_IN_STAGES}
    unknown = wanted - set(STAGE_ORDER)
    if unknown:
        raise ValueError(f"Unknown stages: {', '.join(sorted(unknown))}")
    if wanted & set(PROSE_STAGES) and not enabled:
        raise ValueError("the passages, prose and resolve stages need prose_model: space "
                         "reads analyses without it, so their output would go nowhere")
    return [STAGE_TYPES[name](settings) for name in STAGE_ORDER if name in wanted]


__all__ = [
    "AnalysesStage",
    "DownloadStage",
    "ExtractStage",
    "MetadataStage",
    "OPT_IN_STAGES",
    "PROSE_STAGES",
    "PassagesStage",
    "ProseStage",
    "ReferencesStage",
    "ResolveStage",
    "STAGE_ORDER",
    "STAGE_TYPES",
    "SpaceStage",
    "SyncStage",
    "TriageStage",
    "UploadStage",
    "build",
]

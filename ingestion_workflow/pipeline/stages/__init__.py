"""The stages, in the order they run."""

from .analyses import AnalysesStage
from .download import DownloadStage
from .extract import ExtractStage
from .metadata import MetadataStage
from .notices import NoticesStage
from .passages import PassagesStage
from .prose import ProseStage
from .references import ReferencesStage
from .reflist import ReflistStage
from .resolve import ResolveStage
from .roles import RolesStage
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
#: `notices` is PubMed's retraction and erratum links for each paper. Only
#: `upload` reads it, so looking them up again re-runs nothing upstream.
#:
#: `reflist` fetches each paper's Crossref list and `references` reads each
#: extraction's list and citations, the Crossref list filling its gaps. Nothing
#: downstream reads them yet, so they run only when asked for by name.
#:
#: `roles` runs only when `role_model` is set: it decides what each set of
#: resolve's (or analyses', without prose) is for. Space does not read it yet.
STAGE_ORDER = ("download", "extract", "reflist", "references", "passages", "metadata", "notices", "triage", "analyses",
               "prose", "resolve", "roles", "space", "upload", "sync")

#: Stages that exist only when prose is switched on.
PROSE_STAGES = ("passages", "prose", "resolve")

#: Stages a run without `--stage` leaves out.
OPT_IN_STAGES = ("reflist", "references")

#: Stages that exist only when a role model is configured.
ROLE_STAGES = ("roles",)

STAGE_TYPES = {
    "download": DownloadStage,
    "extract": ExtractStage,
    "metadata": MetadataStage,
    "notices": NoticesStage,
    "triage": TriageStage,
    "analyses": AnalysesStage,
    "passages": PassagesStage,
    "prose": ProseStage,
    "references": ReferencesStage,
    "reflist": ReflistStage,
    "resolve": ResolveStage,
    "roles": RolesStage,
    "space": SpaceStage,
    "upload": UploadStage,
    "sync": SyncStage,
}


def build(names, settings):
    """Instantiate the requested stages in canonical order."""
    enabled = bool(getattr(settings, "prose_model", None))
    roles = bool(getattr(settings, "role_model", None))
    wanted = {name.lower() for name in names} if names else {
        name for name in STAGE_ORDER
        if (enabled or name not in PROSE_STAGES) and name not in OPT_IN_STAGES
        and (roles or name not in ROLE_STAGES)}
    unknown = wanted - set(STAGE_ORDER)
    if unknown:
        raise ValueError(f"Unknown stages: {', '.join(sorted(unknown))}")
    if wanted & set(PROSE_STAGES) and not enabled:
        raise ValueError("the passages, prose and resolve stages need prose_model: space "
                         "reads analyses without it, so their output would go nowhere")
    if wanted & set(ROLE_STAGES) and not roles:
        raise ValueError("the roles stage needs role_model")
    return [STAGE_TYPES[name](settings) for name in STAGE_ORDER if name in wanted]


__all__ = [
    "AnalysesStage",
    "DownloadStage",
    "ExtractStage",
    "MetadataStage",
    "NoticesStage",
    "OPT_IN_STAGES",
    "PROSE_STAGES",
    "PassagesStage",
    "ProseStage",
    "ROLE_STAGES",
    "ReferencesStage",
    "ReflistStage",
    "ResolveStage",
    "RolesStage",
    "STAGE_ORDER",
    "STAGE_TYPES",
    "SpaceStage",
    "SyncStage",
    "TriageStage",
    "UploadStage",
    "build",
]

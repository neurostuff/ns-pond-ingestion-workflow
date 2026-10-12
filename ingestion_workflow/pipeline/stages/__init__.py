"""The stages, in the order they run."""

import logging

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
from .sync import SyncBeforeUploadStage, SyncStage, legacy_upload
from .triage import TriageStage
from .upload import IngesterUploadStage, UploadStage

logger = logging.getLogger(__name__)

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
#: `roles` always runs: it decides what each set of resolve's (or analyses',
#: without prose) is for, and space, sync and upload read only what it passed.
#:
#: `sync` writes the corpus copy and the coordinate parse, and `upload` then posts
#: that parse to neurostore's ingester. The deprecated direct-database upload
#: (`upload_backend: legacy`) runs the other way round, `LEGACY_UPLOAD_ORDER`.
STAGE_ORDER = ("download", "extract", "reflist", "references", "passages", "metadata", "notices",
               "triage", "analyses", "prose", "resolve", "roles", "space", "sync", "upload")
LEGACY_UPLOAD_ORDER = STAGE_ORDER[:-2] + ("upload", "sync")

#: Stages that exist only when prose is switched on.
PROSE_STAGES = ("passages", "prose", "resolve")

#: Stages a run without `--stage` leaves out.
OPT_IN_STAGES = ("reflist", "references")

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
    "sync": SyncBeforeUploadStage,
    "upload": IngesterUploadStage,
}

#: The two stages the deprecated direct-database upload swaps in.
LEGACY_UPLOAD_TYPES = {**STAGE_TYPES, "upload": UploadStage, "sync": SyncStage}


def stage_order(settings):
    """The order the stages run in under this run's upload backend."""
    return LEGACY_UPLOAD_ORDER if legacy_upload(settings) else STAGE_ORDER


def stage_types(settings):
    """{name: stage class} under this run's upload backend."""
    return LEGACY_UPLOAD_TYPES if legacy_upload(settings) else STAGE_TYPES


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
    if legacy_upload(settings) and wanted & {"upload", "sync"}:
        logger.warning("upload_backend: legacy is deprecated: it writes neurostore's database "
                       "directly; the ingester (upload_backend: ingester) replaces it")
    types = stage_types(settings)
    return [types[name](settings) for name in stage_order(settings) if name in wanted]


__all__ = [
    "AnalysesStage",
    "DownloadStage",
    "ExtractStage",
    "IngesterUploadStage",
    "LEGACY_UPLOAD_ORDER",
    "LEGACY_UPLOAD_TYPES",
    "MetadataStage",
    "NoticesStage",
    "OPT_IN_STAGES",
    "PROSE_STAGES",
    "PassagesStage",
    "ProseStage",
    "ReferencesStage",
    "ReflistStage",
    "ResolveStage",
    "RolesStage",
    "STAGE_ORDER",
    "STAGE_TYPES",
    "SpaceStage",
    "SyncBeforeUploadStage",
    "SyncStage",
    "TriageStage",
    "UploadStage",
    "build",
    "stage_order",
    "stage_types",
]

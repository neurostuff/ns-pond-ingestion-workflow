"""What the normalize stage hands `ingestion_workflow.normalization`, and the run it records.

A fit is made once over the corpus (`fit_corpus`, `ingest normalize fit`) and saved; the
stage then assigns each article against it (`assign`). The run is the fit and the code that
assigns: `fit` is the fit's id, which is its fingerprint, and `version` hashes the assign
code, the vocabularies and pondie's commit, so a new fit or new assign code is a new run and
every normalization made under the old one is stale.

Before assigning, what code decides is decided again (`renormalize`): the siblings
study_schema#56 removed (`age_unit_normalized`, `category_normalized`) are dropped; pondie's
`apply_derived` fills the derived slots, replacing what the model answered there
(`groups[].is_healthy` among them); and its `apply_normalized` writes each field's
`normalized` sidecar. The record then holds to the schema pondie and this package pin, and
the fit's medical step reads the derived `is_healthy`.
"""

from __future__ import annotations

import json
import socket
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Sequence, Tuple

from ingestion_workflow.catalog import Artifact, Catalog, Run, RunKind
from ingestion_workflow.pipeline.stage import recorded_path

#: The pipeline a normalize run made in-process is filed under.
PIPELINE = "ns-pond normalize"
#: Bumped when `renormalize` changes what it does; part of the run's version.
RENORMALIZE_VERSION = 3


def describe(fit_dir: Path) -> Dict[str, Any]:
    """The fit's id and model, and a digest of what assigns with it."""
    from ingestion_workflow.normalization import api

    manifest = json.loads((Path(fit_dir) / "manifest.json").read_text("utf-8"))
    vocabulary = api.vocab_versions()
    assigner = {"assign_code_sha256": api.digest(api.code_hashes()), "vocab": vocabulary,
                "pondie_commit": api.pondie_commit(), "renormalize": RENORMALIZE_VERSION}
    embedding = manifest["models"]["embedding"]
    return {"fit_id": manifest["fit_id"], "vocabulary": vocabulary,
            "version": "assign-" + api.digest(assigner)[:12],
            "model": f"taskcat + {embedding['name']}@{embedding['revision']}",
            "assigner": assigner}


def run_for(source: str, fit_dir: Path, code_commit: str,
            about: Optional[Dict[str, Any]] = None) -> Run:
    """The run a normalize stage makes for `source` against the fit at `fit_dir`."""
    about = about or describe(fit_dir)
    made = Run("", RunKind.MODEL, "normalize", source, pipeline=PIPELINE,
               version=about["version"], model=about["model"],
               schema_version="extraction-record", fit=about["fit_id"],
               code_commit=code_commit, machine=socket.gethostname(),
               config={"fit_dir": str(fit_dir), "vocabulary": about["vocabulary"],
                       **about["assigner"]})
    return Run(**{**made.__dict__, "run_id": made.minted_id})


def text_of(catalog_root: Path, sync: Artifact) -> str:
    """The parse's text, the one the record's evidence offsets index, as the ingester reads it."""
    study = recorded_path(catalog_root, sync.summary["path"])
    paper = json.loads((study / "parse" / "parsed_paper.json").read_text("utf-8"))
    # Not read_text: it would turn a CRLF into LF, and the offsets would move.
    return (study / paper["text_path"]).read_bytes().decode("utf-8")


def text_source(catalog_root: Path, sync: Artifact) -> Optional[str]:
    """The extraction source the parse was read from: whose `references` list it names."""
    study = recorded_path(catalog_root, sync.summary["path"])
    paper = json.loads((study / "parse" / "parsed_paper.json").read_text("utf-8"))
    return paper.get("source")


def bibliography_of(catalog: Catalog, article_id: str, source: Optional[str]) -> Optional[dict]:
    """The `references` payload of the parse's source, or None when it has none."""
    if not source:
        return None
    listed = catalog.artifact(article_id, "references", source)
    return catalog.payload(listed) if listed is not None and listed.ok else None


def assign(record: Dict[str, Any], text: str, bibliography: Optional[dict], fit_dir: Path,
           article_id: str) -> Dict[str, Any]:
    """`record` with its `normalized` sidecars, against the fit at `fit_dir`."""
    from ingestion_workflow.normalization import normalize_record

    return normalize_record(record, text, bibliography, str(fit_dir), article_id=article_id)


def drop_siblings(record: Dict[str, Any]) -> int:
    """Remove the per-field normalizations study_schema#56 replaced with sidecars, in place;
    how many were removed."""
    dropped = 0
    for group in record.get("groups") or []:
        if not isinstance(group, dict):
            continue
        dropped += group.pop("age_unit_normalized", None) is not None
        # every distribution (sex, gender, race, handedness, ...) carried one per category
        for slot in group.values():
            for entry in slot if isinstance(slot, list) else ():
                if isinstance(entry, dict):
                    dropped += entry.pop("category_normalized", None) is not None
    return dropped


def apply_derived(record: Dict[str, Any]) -> None:
    """pondie's derived-slot fills (`is_healthy`, `population_characteristics`), in place."""
    from pondie.normalization import apply_derived as apply

    apply(record)


def apply_normalized(record: Dict[str, Any]) -> None:
    """pondie's per-field normalizers, each writing its field's `normalized` sidecar."""
    from pondie.normalization import apply_normalized as apply

    apply(record)


def renormalize(record: Dict[str, Any]) -> Dict[str, Any]:
    """`record` with its derived slots and per-field normalizations made again, in place.
    In this order: the fit's medical step reads the derived `is_healthy`."""
    drop_siblings(record)
    apply_derived(record)
    apply_normalized(record)
    return record


def _assign_one(job) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """(the normalized record, None), or (None, why it failed): a worker's whole answer."""
    record, text, bibliography, fit_dir, article_id = job
    try:
        return assign(renormalize(record), text, bibliography, Path(fit_dir), article_id), None
    except Exception as exc:  # noqa: BLE001 - this article's failure
        return None, f"{type(exc).__name__}: {exc}"


def assign_all(jobs: Iterable[Tuple[Any, Optional[tuple]]], workers: int,
               no_input: str) -> Iterator[Tuple[Any, Tuple[Optional[dict], Optional[str]]]]:
    """(key, (record, error)) for each (key, job) of `jobs`, in order; a job of None fails
    with `no_input`. `workers` > 1 assigns in forked processes: each inherits the fit the
    first job loaded here, rather than loading its own (spawn would, per process)."""
    import multiprocessing

    held = iter(jobs)
    if workers <= 1:
        for key, job in held:
            yield key, ((None, no_input) if job is None else _assign_one(job))
        return
    for key, job in held:  # the first one here, so the forked workers share its loaded fit
        yield key, ((None, no_input) if job is None else _assign_one(job))
        break
    pending = list(held)
    with multiprocessing.get_context("fork").Pool(workers) as pool:
        answers = pool.imap(_assign_one, [job for _, job in pending if job is not None],
                            chunksize=4)
        for key, job in pending:
            yield key, ((None, no_input) if job is None else next(answers))


class _Lazy(Mapping):
    """article -> a value read on access, so a corpus of 65k records is never all held."""

    def __init__(self, ids: Sequence[str], read: Callable[[str], Any]) -> None:
        self._ids, self._read = list(ids), read

    def __getitem__(self, article_id: str) -> Any:
        return self._read(article_id)

    def __iter__(self) -> Iterator[str]:
        return iter(self._ids)

    def __len__(self) -> int:
        return len(self._ids)


def fit_corpus(catalog: Catalog, inputs: Dict[str, Artifact], out_dir: Path, *,
               fixed_dir: Optional[Path], previous: Optional[str] = None,
               workers: int = 1, log=print) -> str:
    """Fit over each article's normalize input (`inputs`: article -> the artifact normalize
    reads) and save it under `out_dir`; the fit's id. `previous` makes it a refit of that fit."""
    from ingestion_workflow.normalization import fit, refit

    ids = sorted(inputs)
    synced = catalog.artifacts(ids, "sync")
    syncs = {a: synced[a][""] for a in ids if synced.get(a, {}).get("")}
    ids = [a for a in ids if a in syncs]
    records = _Lazy(ids, lambda a: catalog.payload(inputs[a]))
    texts = _Lazy(ids, lambda a: text_of(catalog.root, syncs[a]))
    bibliographies = _Lazy(ids, lambda a: bibliography_of(
        catalog, a, text_source(catalog.root, syncs[a])))
    kw = dict(fixed_dir=str(fixed_dir) if fixed_dir else None, workers=workers, log=log)
    if previous:
        return refit(records, texts, bibliographies, str(out_dir), previous=previous, **kw)
    return fit(records, texts, bibliographies, str(out_dir), **kw)


__all__ = ["PIPELINE", "assign", "assign_all", "bibliography_of", "describe", "drop_siblings",
           "fit_corpus", "renormalize", "run_for", "text_of", "text_source"]

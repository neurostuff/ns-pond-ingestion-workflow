"""Import what a run of an offloaded stage made: `ingest offload import`.

A run is one JSON object with the `runs` table's fields. Its results are JSON lines, one
per article:

    {"article_id" | "base_study_id", "status": "ok" | "skipped" | "failed", "reason",
     "text_sha256", "record", "record_digest"}

`record` is the stage's output: the extraction record, the record with its evidence, or
the normalization of a record. `text_sha256` is the text the model read (record,
evidence); `record_digest` is `record_digest()` of the record the stage read: the record
(evidence), or the evidence record, the record itself when evidence was skipped
(normalize). A `base_study_id` is found through the `neurostore` alias.

A record or evidence row's legacy per-field normalizations (`drop_siblings`) are dropped
before anything is checked: the schema refuses them, and normalize makes them again.

A row is refused, and recorded `failed` with the reason, when its text hash is not the
parse's, it names a parse key the parse does not have, the record it read is not the
article's current record of the run's source, or its record fails study_schema's
extraction-record schema. A refusal never replaces an OK artifact of another run. A row
whose article the catalog does not know is counted and left out.

A record the schema refuses is first given pondie's deterministic fixes, the ones normalize
runs on every record (`normalize_assign.repair`), without the paper's text, which import
does not read. When the fixed record passes and names the same parse keys, it is kept, and
its summary's `repaired` names the fixes that changed it. A record the schema accepts is kept
as the model wrote it. The fixes need pondie and its study_schema checkout (the `normalize`
extra); where they cannot load, a refused record stays refused and the report says why.
"""

from __future__ import annotations

import copy
import functools
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Run, RunKind, Status
from ingestion_workflow.pipeline.stages.offloaded import (
    NO_RECORD,
    OFFLOADED_STAGES,
    Parse,
    drop_siblings,
    evidence_fingerprint,
    normalize_fingerprint,
    normalize_input,
    parse_check,
    parse_keys,
    record_digest,
    record_fingerprint,
)
from ingestion_workflow.services.normalize_assign import repair

logger = logging.getLogger(__name__)

#: Rows read and recorded together.
BATCH = 500
#: The placeholder model name the first collected sets stamped on every record.
PLACEHOLDER_MODELS = ("vista-compact",)
#: How a refusal's reason starts, for counting refusals by kind.
SCHEMA_INVALID = "fails study_schema's extraction-record schema"
RECORD_MOVED = "built on another record than the article's current one"
UNKNOWN_ARTICLE = "unknown article"


@dataclass
class ImportReport:
    run_id: str
    stage: str
    source: str
    rows: int = 0
    #: {status: articles recorded}.
    recorded: Counter = field(default_factory=Counter)
    #: {reason, cut to its kind: rows refused}.
    refused: Counter = field(default_factory=Counter)
    unknown: int = 0
    #: Refusals not recorded because an OK artifact of another run is kept.
    kept: int = 0
    #: Rows whose base study's article is not their `article_id`'s; the base study's wins.
    disagree: int = 0
    #: Refused records not given pondie's fixes, and why they could not run.
    unrepaired: int = 0
    unrepaired_why: Optional[str] = None

    def lines(self) -> List[str]:
        out = [f"run {self.run_id}: {self.stage} / {self.source}, {self.rows:,} rows",
               "  recorded: " + ", ".join(f"{n:,} {s}" for s, n in sorted(self.recorded.items()))]
        out += [f"  refused {n:,}: {why}" for why, n in self.refused.most_common()]
        if self.unknown:
            out.append(f"  {self.unknown:,} rows name an article the catalog does not know")
        if self.disagree:
            out.append(f"  {self.disagree:,} rows' base study is held by another article than "
                       "their article_id; the base study's was taken")
        if self.kept:
            out.append(f"  {self.kept:,} refusals left an OK artifact of another run in place")
        if self.unrepaired:
            out.append(f"  {self.unrepaired:,} refused records were not given pondie's fixes: "
                       f"{self.unrepaired_why}")
        return out


def load_run(path: Path | str) -> Run:
    held = json.loads(Path(path).read_text("utf-8"))
    if held.get("stage") not in OFFLOADED_STAGES:
        raise ValueError(f"{path}: stage must be one of {', '.join(OFFLOADED_STAGES)}")
    unknown = set(held) - set(Run.__dataclass_fields__)
    if unknown:
        raise ValueError(f"{path}: fields no run has: {', '.join(sorted(unknown))}")
    return Run(**{**held, "kind": RunKind(held.get("kind", "model"))})


def _schema_problem(record: Any) -> Optional[str]:
    import jsonschema
    from study_schema.jsonschema import load

    global _VALIDATOR
    if _VALIDATOR is None:
        schema = load("extraction-record")
        _VALIDATOR = jsonschema.validators.validator_for(schema)(schema)
    error = next(iter(_VALIDATOR.iter_errors(record)), None)
    if error is None:
        return None
    where = "/".join(str(p) for p in error.absolute_path)
    return f"{SCHEMA_INVALID}: {where}: {error.message[:200]}"


_VALIDATOR = None


@functools.lru_cache(maxsize=1)
def _fixes_unavailable() -> Optional[str]:
    """Why pondie's fixes cannot run here, or None when they can."""
    try:
        import pondie.extraction.record.fix  # noqa: F401
    except (ImportError, RuntimeError) as exc:  # RuntimeError: no study_schema checkout
        return f"{type(exc).__name__}: {exc}"[:300]
    return None


def _repaired(record: Dict[str, Any], parse: Optional[Parse]) -> Optional[tuple]:
    """(`record` with pondie's fixes, the fixes that changed it) when the schema accepts
    that and its parse keys are `record`'s, else None."""
    fixed = copy.deepcopy(record)
    try:
        fired = repair(fixed, parse.path if parse is not None else None, "")
    except Exception:  # noqa: BLE001 - this record's failure: it stays refused
        logger.warning("pondie's fixes failed on a refused record", exc_info=True)
        return None
    if _schema_problem(fixed) is None and parse_keys(fixed) == parse_keys(record):
        return fixed, fired
    return None


def stamp(record: Dict[str, Any], run: Run, text_sha256: Optional[str]) -> Dict[str, Any]:
    """`record` with the run as its `extraction_metadata`, in place of what the run wrote."""
    held = dict(record.get("extraction_metadata") or {})
    held.update(extractor_model=run.model or held.get("extractor_model") or run.run_id,
                extractor_version=run.run_id)
    if text_sha256:
        held["source_text_hash"] = text_sha256
    if run.created_at:
        held["extraction_date"] = run.created_at[:10]
    return {**record, "extraction_metadata": held}


class _Importer:
    def __init__(self, catalog: Catalog, run: Run, report: ImportReport) -> None:
        self.catalog = catalog
        self.run = run
        self.report = report
        #: The run each record artifact names, read once.
        self._runs: Dict[str, Optional[Run]] = {}

    def article_of(self, row: Mapping[str, Any]) -> Optional[str]:
        """The live article a row names: its base study's, through the `neurostore` alias,
        else its `article_id`, each followed through merges."""
        named = row.get("article_id")
        held = (self.catalog.holder("neurostore", row["base_study_id"])
                if row.get("base_study_id") else None)
        found = [self.catalog.current_id(a) for a in (held, named) if a]
        found = [a for a in found if self.catalog.live(a)]
        if len(set(found)) > 1:
            self.report.disagree += 1
        return found[0] if found else None

    def run_of(self, artifact: Artifact) -> Optional[Run]:
        if artifact.run_id not in self._runs:
            self._runs[artifact.run_id] = self.catalog.run(artifact.run_id)
        return self._runs[artifact.run_id]

    def outcome(self, article_id: str, row: Mapping[str, Any], parse: Optional[Parse],
                record: Optional[Artifact]) -> Outcome:
        run, stage = self.run, self.run.stage
        status = row.get("status", "ok")
        text_sha256 = row.get("text_sha256")
        if status in ("skipped", "failed"):
            return Outcome(article_id, stage, run.source,
                           Status.SKIPPED if status == "skipped" else Status.FAILED,
                           fingerprint=record_fingerprint(run, text_sha256, ()),
                           summary={"text_sha256": text_sha256},
                           error=(row.get("reason") or status)[:2000], run_id=run.run_id)
        payload = row.get("record")
        if not isinstance(payload, dict):
            return self.refuse(article_id, "the row holds no record")
        if stage != "normalize":
            payload = copy.deepcopy(payload)  # the caller's row is left as it was
            drop_siblings(payload)
        if stage == "normalize":
            why = self.upstream_problem(row, record)
            if why:
                return self.refuse(article_id, why)
            return Outcome(article_id, stage, run.source, Status.OK,
                           fingerprint=normalize_fingerprint(run, record,
                                                             run.config.get("vocabulary")),
                           payload=payload,
                           summary={"record_digest": row["record_digest"],
                                    "vocabulary": run.config.get("vocabulary")},
                           run_id=run.run_id)
        keys = parse_keys(payload)
        why = parse_check(parse, text_sha256, keys)
        if why is None and stage == "evidence":
            why = self.upstream_problem(row, record)
        fired: List[str] = []
        if why is None:
            why = _schema_problem(payload)
            if why and stage == "record":
                why, payload, fired = self.repair(why, payload, parse)
        if why:
            return self.refuse(article_id, why)
        maker = run if stage == "record" else self.run_of(record)
        payload = stamp(payload, maker or run, text_sha256)
        summary = {"text_sha256": text_sha256, "parse_keys": sorted(keys),
                   "record_digest": record_digest(payload),
                   "analyses": len(payload.get("analyses") or []),
                   "coordinate_sets": len(payload.get("coordinate_sets") or [])}
        if fired:
            summary["repaired"] = fired
        fp = (record_fingerprint(run, text_sha256, keys) if stage == "record"
              else evidence_fingerprint(run, text_sha256, record))
        return Outcome(article_id, stage, run.source, Status.OK, fingerprint=fp,
                       payload=payload, summary=summary, run_id=run.run_id)

    def repair(self, why: str, record: Dict[str, Any],
               parse: Optional[Parse]) -> tuple:
        """(None, the fixed record, the fixes) when pondie's fixes make `record` valid, else
        (`why`, `record`, [])."""
        unavailable = _fixes_unavailable()
        if unavailable:
            self.report.unrepaired += 1
            self.report.unrepaired_why = unavailable
            return why, record, []
        fixed = _repaired(record, parse)
        return (None, *fixed) if fixed is not None else (why, record, [])

    @staticmethod
    def upstream_problem(row: Mapping[str, Any], record: Optional[Artifact]) -> Optional[str]:
        if record is None or not record.ok:
            return NO_RECORD
        if row.get("record_digest") != record.summary.get("record_digest"):
            return RECORD_MOVED
        return None

    def refuse(self, article_id: str, why: str) -> Outcome:
        self.report.refused[re.sub(r"\d+", "N", why.split(":")[0].split(" (")[0])] += 1
        outcome = Outcome.failure(article_id, self.run.stage, self.run.source, why,
                                  summary={"refused": True})
        outcome.run_id = self.run.run_id
        return outcome

    def batch(self, rows: List[Dict[str, Any]]) -> None:
        catalog, run = self.catalog, self.run
        resolved: List[tuple] = []
        for row in rows:
            article_id = self.article_of(row)
            if article_id is None:
                self.report.unknown += 1
                continue
            resolved.append((article_id, row))
        ids = sorted({article_id for article_id, _ in resolved})
        synced = catalog.artifacts(ids, "sync")
        records = catalog.artifacts(ids, "record") if run.stage != "record" else {}
        evidence = catalog.artifacts(ids, "evidence") if run.stage == "normalize" else {}
        mine = catalog.artifacts(ids, run.stage)
        outcomes = []
        for article_id, row in resolved:
            parse = (Parse.of(catalog.root, synced.get(article_id, {}).get(""))
                     if run.stage != "normalize" else None)
            read = records.get(article_id, {}).get(run.source)
            if run.stage == "normalize":
                read = normalize_input(evidence.get(article_id, {}).get(run.source), read)
            outcome = self.outcome(article_id, row, parse, read)
            held = mine.get(article_id, {}).get(run.source)
            if (outcome.status is Status.FAILED and held is not None and held.ok
                    and held.run_id != run.run_id):
                self.report.kept += 1
                continue
            self.report.recorded[outcome.status.value] += 1
            outcomes.append(outcome)
        catalog.record(outcomes)


def import_run(catalog: Catalog, run: Run, rows: Iterable[Dict[str, Any]],
               *, batch: int = BATCH) -> ImportReport:
    """Keep `run` and record each of its rows; see the module's docstring."""
    run = catalog.record_run(run)
    report = ImportReport(run.run_id, run.stage, run.source)
    importer = _Importer(catalog, run, report)
    pending: List[Dict[str, Any]] = []
    for row in rows:
        report.rows += 1
        pending.append(row)
        if len(pending) >= batch:
            importer.batch(pending)
            pending = []
    if pending:
        importer.batch(pending)
    logger.info("; ".join(report.lines()))
    return report


__all__ = ["ImportReport", "import_run", "load_run", "record_digest", "stamp"]

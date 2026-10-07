"""Merge an article's table analyses with the results its prose adds."""

from __future__ import annotations

import collections
import re
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import Analysis, AnalysisCollection, Coordinate
from ingestion_workflow.models.analysis import CoordinateSpace

from ..plan import StagePlan, Work
from ..stage import Context

RESOLVE_VERSION = 2

#: How close a prose coordinate may sit to a table's and still be the same
#: peak: papers round the same voxel differently between text and table.
SAME_PEAK_MM = 1.0

UNNAMED = "unnamed prose analysis"

#: Analyses that are computed at a peak rather than finding it. Of 277
#: hand-labelled prose results sitting on one of their article's table peaks,
#: 85 were a different analysis there -- mostly a correlation, a conjunction
#: or a follow-up -- and this keeps 46 of them while re-keeping 20 of the 192
#: that restate the table (79% agreement with a judge reading both).
COMPUTED_AT_PEAK = re.compile(
    r"correlat|regress|conjunction|interaction|\bx\b|\u00d7|\bppi\b|connectivity|coupling|parametric|modulat|covar",
    re.I)


def _norm(name: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _space(value: Optional[str]) -> CoordinateSpace:
    return {"MNI": CoordinateSpace.MNI, "TAL": CoordinateSpace.TALAIRACH}.get(value or "", CoordinateSpace.OTHER)


def _number(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve(tables: Dict[str, Any], prose: Dict[str, Any], slug: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The table collections, plus one collection of what the prose adds.

    Only results are kept: ROI centres, seeds, targets, display locations and
    other studies' peaks are not this study's findings. A result already in
    one of the article's tables is the text restating it -- unless the prose
    reports an analysis computed at that peak (a correlation, a conjunction)
    that no table analysis there is. A coordinate the prose reports under two
    contrasts stays under both.
    """
    table_points = [
        ((c["x"], c["y"], c["z"]), a.get("name") or "")
        for blob in (tables or {}).values()
        for a in (blob or {}).get("analyses", [])
        for c in a.get("coordinates", [])
    ]
    table_spaces = collections.Counter(
        (blob or {}).get("coordinate_space") for blob in (tables or {}).values() if (blob or {}).get("analyses"))

    groups: Dict[str, Analysis] = {}
    seen = set()
    dropped = collections.Counter()
    restated = at_table_peak = 0
    spaces = collections.Counter()
    for index, passage in enumerate((prose or {}).get("passages", [])):
        space = _space(passage.get("space"))
        for a in passage.get("analyses", []):
            name = (a.get("name") or "").strip() or UNNAMED
            for p in a.get("points", []):
                if p.get("role") != "result":
                    dropped[p.get("role") or "none"] += 1
                    continue
                xyz = (float(p["x"]), float(p["y"]), float(p["z"]))
                at_peak = [n for t, n in table_points
                           if all(abs(u - v) <= SAME_PEAK_MM for u, v in zip(xyz, t))]
                if at_peak and not (COMPUTED_AT_PEAK.search(name)
                                    and not any(COMPUTED_AT_PEAK.search(n) for n in at_peak)):
                    restated += 1
                    continue
                at_table_peak += bool(at_peak)
                key = (_norm(name), tuple(round(v) for v in xyz))
                if key in seen:
                    continue
                seen.add(key)
                analysis = groups.setdefault(_norm(name), Analysis(
                    name=name, table_id="prose", metadata={"source": "prose", "passages": []}))
                if index not in analysis.metadata["passages"]:
                    analysis.metadata["passages"].append(index)
                size = p.get("cluster_size")
                analysis.coordinates.append(Coordinate(
                    x=xyz[0], y=xyz[1], z=xyz[2], space=space,
                    statistic_value=_number(p.get("value")), statistic_type=p.get("statistic"),
                    cluster_size=int(size) if isinstance(size, (int, float)) else None,
                    cluster_measure=a.get("measure") if size is not None else None,
                ))
                spaces[space] += 1

    out = dict(tables or {})
    if groups:
        if spaces:
            space = spaces.most_common(1)[0][0]
        elif table_spaces:
            space = CoordinateSpace(table_spaces.most_common(1)[0][0])
        else:
            space = CoordinateSpace.OTHER
        out["prose"] = AnalysisCollection(slug=slug, analyses=list(groups.values()),
                                          coordinate_space=space).to_dict()
    summary = {
        "tables": sum(1 for blob in out.values() if (blob or {}).get("analyses")),
        "prose_analyses": len(groups),
        "prose_points": sum(len(a.coordinates) for a in groups.values()),
        "restated": restated,
        "at_table_peaks": at_table_peak,
        "not_results": dict(dropped),
    }
    return out, summary


class ResolveStage:
    name = "resolve"
    #: prose, and `analyses` read alongside: `requires` names one parent, and
    #: an article with no table worth parsing has no analyses at all.
    requires = "prose"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, prose: Artifact, analyses: Optional[Artifact]) -> str:
        return fingerprint("resolve", RESOLVE_VERSION, SAME_PEAK_MM, COMPUTED_AT_PEAK.pattern,
                           analyses.fingerprint if analyses is not None else "no tables",
                           upstream=prose.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        attempts = ctx.catalog.attempt_counts(ids, self.name, "")
        triaged = ctx.catalog.artifacts(ids, "triage")
        analysed = ctx.catalog.artifacts(ids, "analyses")
        for ref in refs:
            prose = upstream.get(ref.id, {}).get("")
            if prose is None or prose.status is not Status.OK:
                plan.blocked += 1
                continue
            passed = (triaged.get(ref.id, {}).get("") or Artifact(ref.id, "triage")).summary.get("passed", 0)
            analyses = analysed.get(ref.id, {}).get("")
            if passed and (analyses is None or analyses.status is Status.FAILED):
                plan.blocked += 1  # the tables are still to be read
                continue
            tables = analyses if analyses is not None and analyses.status is Status.OK else None
            if not prose.summary.get("results") and not (tables and tables.summary.get("tables")):
                plan.skipped += 1  # neither the tables nor the prose hold a result
                continue
            fp = self.fingerprint_for(prose, tables)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=prose))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        analysed = ctx.catalog.artifacts([w.article_id for w in works], "analyses")
        for work in works:
            tables_artifact = analysed.get(work.article_id, {}).get("")
            tables = ctx.payload(tables_artifact) if tables_artifact is not None and tables_artifact.ok else {}
            payload, summary = resolve(tables or {}, ctx.payload(work.upstream) or {},
                                       work.ref.identifier.slug)
            # What upload's freshness should follow. When the prose adds
            # nothing, the upload is the tables' upload, so it keeps the
            # fingerprint it already had.
            summary["basis"] = (tables_artifact.fingerprint
                                if not summary["prose_analyses"] and tables_artifact is not None else "")
            yield Outcome(article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                          fingerprint=work.fingerprint, payload=payload, summary=summary)

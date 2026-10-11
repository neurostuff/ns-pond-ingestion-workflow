"""Merge an article's table analyses with the results its prose adds."""

from __future__ import annotations

import collections
import re
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import Analysis, AnalysisCollection, Coordinate
from ingestion_workflow.models.analysis import UNKNOWN_SPACES, CoordinateSpace

from ..plan import StagePlan, Work
from ..stage import Context

RESOLVE_VERSION = 6

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


def _space(value: Optional[str]) -> Optional[CoordinateSpace]:
    """The prose's MNI/TAL/null."""
    # A deterministic reader returns only MNI, TAL or null, so a stated space
    # it can't match is null.
    space = CoordinateSpace.from_label(value)
    return space if space in (CoordinateSpace.MNI, CoordinateSpace.TALAIRACH) else None


def _number(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve(tables: Dict[str, Any], prose: Dict[str, Any], slug: str,
            identifier=None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The table collections, plus one collection of what the prose adds.

    One analysis per name within a passage, never across passages: a seed in
    the Methods and the results connected to it in the Results are different
    sets, whatever they are called. An unnamed analysis is its own set, in the
    model's order. What a set is for is the roles stage's to decide, not the
    prose model's. A point already in one of the article's tables is the text
    restating it -- unless the analysis was computed at that peak (a
    correlation, a conjunction) and no table analysis there is. A coordinate
    the prose reports under two contrasts stays under both. A point dropped
    from a set that is kept is listed in its `metadata.dropped`, with the reason.
    """
    table_points = [
        ((c["x"], c["y"], c["z"]), a.get("name") or "")
        for blob in (tables or {}).values()
        for a in (blob or {}).get("analyses", [])
        for c in a.get("coordinates", [])
    ]
    table_spaces = collections.Counter(
        (blob or {}).get("coordinate_space") for blob in (tables or {}).values()
        if (blob or {}).get("analyses")
        and (blob or {}).get("coordinate_space") not in UNKNOWN_SPACES)

    groups: Dict[tuple, Analysis] = {}
    dropped: Dict[tuple, List[Dict[str, Any]]] = collections.defaultdict(list)
    seen = set()
    restated = at_table_peak = 0
    spaces = collections.Counter()
    article_space = _space((prose or {}).get("space"))
    for index, passage in enumerate((prose or {}).get("passages", [])):
        space = _space(passage.get("space"))
        if space is None:
            space = article_space  # the space the article's Methods state
        for position, a in enumerate(passage.get("analyses", [])):
            name = (a.get("name") or "").strip() or UNNAMED
            # Named: one set per name in this passage. Unnamed: one per analysis.
            named = bool((a.get("name") or "").strip())
            group = (index, _norm(name)) if named else (index, "", position)
            for p in a.get("points", []):
                xyz = (float(p["x"]), float(p["y"]), float(p["z"]))
                point = {"x": xyz[0], "y": xyz[1], "z": xyz[2]}
                at_peak = [n for t, n in table_points
                           if all(abs(u - v) <= SAME_PEAK_MM for u, v in zip(xyz, t))]
                computed = (COMPUTED_AT_PEAK.search(name)
                            and not any(COMPUTED_AT_PEAK.search(n) for n in at_peak))
                if at_peak and not computed:
                    restated += 1
                    dropped[group].append({**point, "reason": "restates a table peak"})
                    continue
                at_table_peak += bool(at_peak)
                key = (group, tuple(round(v) for v in xyz))
                if key in seen:
                    dropped[group].append(
                        {**point, "reason": "repeats a point of this analysis"})
                    continue
                seen.add(key)
                analysis = groups.setdefault(group, Analysis(
                    name=name, table_id="prose",
                    metadata={"source": "prose", "passages": [index]}))
                size = p.get("cluster_size")
                analysis.coordinates.append(Coordinate(
                    x=xyz[0], y=xyz[1], z=xyz[2], space=space,
                    statistic_value=_number(p.get("value")), statistic_type=p.get("statistic"),
                    cluster_size=int(size) if isinstance(size, (int, float)) else None,
                    cluster_measure=a.get("measure") if size is not None else None,
                ))
                if space is not None:
                    spaces[space] += 1

    for group, analysis in groups.items():
        if dropped.get(group):
            analysis.metadata["dropped"] = dropped[group]
    out = dict(tables or {})
    if groups:
        stated = _space((prose or {}).get("space"))
        if spaces:
            space = spaces.most_common(1)[0][0]
        elif stated is not None:
            space = stated
        elif table_spaces:
            space = CoordinateSpace(table_spaces.most_common(1)[0][0])
        else:
            space = None
        out["prose"] = AnalysisCollection(slug=slug, identifier=identifier, analyses=list(groups.values()),
                                          coordinate_space=space).to_dict()
    summary = {
        "tables": sum(1 for blob in out.values() if (blob or {}).get("analyses")),
        "prose_analyses": len(groups),
        "prose_points": sum(len(a.coordinates) for a in groups.values()),
        "restated": restated,
        "repeated": sum(1 for d in dropped.values() for p in d
                        if p["reason"].startswith("repeats")),
        "at_table_peaks": at_table_peak,
    }
    return out, summary


class ResolveStage:
    name = "resolve"
    #: prose, and `analyses` read alongside when it exists: `requires` names
    #: one parent, and an article with no table that parsed has no analyses.
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
        analysed = ctx.catalog.artifacts(ids, "analyses")
        for ref in refs:
            prose = upstream.get(ref.id, {}).get("")
            if prose is None or prose.status is not Status.OK:
                plan.blocked += 1
                continue
            # The tables' analyses when there are any. Nothing waits for them:
            # when they arrive the fingerprint below changes, and this re-runs.
            analyses = analysed.get(ref.id, {}).get("")
            tables = analyses if analyses is not None and analyses.status is Status.OK else None
            if not prose.summary.get("coordinates") and not (tables and tables.summary.get("tables")):
                plan.skipped += 1  # neither the tables nor the prose hold a point
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
                                       work.ref.identifier.slug, work.ref.identifier)
            # What upload's freshness should follow. When the prose adds
            # nothing, the upload is the tables' upload, so it keeps the
            # fingerprint it already had.
            summary["basis"] = (tables_artifact.fingerprint
                                if not summary["prose_analyses"] and tables_artifact is not None else "")
            yield Outcome(article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                          fingerprint=work.fingerprint, payload=payload, summary=summary)

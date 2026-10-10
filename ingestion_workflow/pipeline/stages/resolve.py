"""Merge an article's table analyses with the results its prose adds."""

from __future__ import annotations

import collections
import re
import unicodedata
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import Analysis, AnalysisCollection, Coordinate
from ingestion_workflow.models.analysis import UNKNOWN_SPACES, CoordinateSpace
from ingestion_workflow.services.coordinate_flags import UPLOADED_PROSE_ROLES

from ..plan import StagePlan, Work
from ..stage import Context

RESOLVE_VERSION = 5

#: How close a prose coordinate may sit to a table's and still be the same
#: peak: papers round the same voxel differently between text and table.
SAME_PEAK_MM = 1.0

UNNAMED = "unnamed prose analysis"


def _norm(name: Optional[str]) -> str:
    """The name compared: NFKC, casefolded, every dash a '-', whitespace collapsed.

    study_schema's `keys.normalize_name`, which the pinned study_schema does not have yet.
    """
    text = unicodedata.normalize("NFKC", name or "").casefold()
    text = re.sub(r"[\u2010-\u2015\u2212\ufe58\ufe63\uff0d]", "-", text)
    return " ".join(re.sub(r"[\u200b-\u200d\u2060\ufeff]", "", text).split())


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

    A prose analysis is one passage's analysis: the same name in two passages
    is two analyses, and each unnamed one is its own, in the order the passage
    gives them. Every role is kept, one role per analysis. A point is dropped
    only as the text restating a table analysis of the same name at that peak,
    and each drop is listed in the summary's `restated_points`.
    A coordinate the prose reports under two contrasts stays under both.
    """
    table_points = [
        ((c["x"], c["y"], c["z"]), key, a.get("name") or "")
        for key, blob in (tables or {}).items()
        for a in (blob or {}).get("analyses", [])
        for c in a.get("coordinates", [])
    ]
    table_spaces = collections.Counter(
        (blob or {}).get("coordinate_space") for blob in (tables or {}).values()
        if (blob or {}).get("analyses")
        and (blob or {}).get("coordinate_space") not in UNKNOWN_SPACES)

    groups: Dict[Tuple, Analysis] = {}
    seen = set()
    kept = collections.Counter()
    restated: Dict[Tuple, Dict[str, Any]] = {}
    at_table_peak = 0
    spaces = collections.Counter()
    article_space = _space((prose or {}).get("space"))
    for index, passage in enumerate((prose or {}).get("passages", [])):
        space = _space(passage.get("space"))
        if space is None:
            space = article_space  # the space the article's Methods state
        unnamed = 0
        for ordinal, a in enumerate(passage.get("analyses", [])):
            name = (a.get("name") or "").strip()
            named = bool(name)
            if named:
                identity = (index, _norm(name))
            else:
                unnamed += 1
                name = UNNAMED if unnamed == 1 else f"{UNNAMED} {unnamed}"
                identity = (index, ordinal)
            points = a.get("points", [])
            roles = list(dict.fromkeys(p.get("role") or "other" for p in points))
            # A named analysis the passage reports no peak for is kept, empty:
            # whether it found nothing is decided later.
            if not roles and named:
                roles = ["result"]
            for role in roles:
                groups.setdefault((identity, role), Analysis(
                    name=name, table_id="prose",
                    metadata={"source": "prose", "role": role, "passages": [index],
                              "ordinal": ordinal, "unwritten": a.get("unwritten", 0)}))
            for p in points:
                role = p.get("role") or "other"
                xyz = (float(p["x"]), float(p["y"]), float(p["z"]))
                at_peak = [(key, n) for t, key, n in table_points
                           if all(abs(u - v) <= SAME_PEAK_MM for u, v in zip(xyz, t))]
                same = [(key, n) for key, n in at_peak if _norm(n) == _norm(name)]
                if same:
                    drop = restated.setdefault((identity, role), {
                        "passage": index, "analysis": name, "role": role, "points": 0,
                        "restates": []})
                    drop["points"] += 1
                    for key, n in same:
                        if {"table": key, "analysis": n} not in drop["restates"]:
                            drop["restates"].append({"table": key, "analysis": n})
                    continue
                at_table_peak += bool(at_peak)
                key = (identity, role, tuple(round(v) for v in xyz))
                if key in seen:
                    continue
                seen.add(key)
                analysis = groups[(identity, role)]
                kept[role] += 1
                size = p.get("cluster_size")
                analysis.coordinates.append(Coordinate(
                    x=xyz[0], y=xyz[1], z=xyz[2], space=space,
                    statistic_value=_number(p.get("value")), statistic_type=p.get("statistic"),
                    cluster_size=int(size) if isinstance(size, (int, float)) else None,
                    cluster_measure=a.get("measure") if size is not None else None,
                ))
                if space is not None and role in UPLOADED_PROSE_ROLES:
                    spaces[space] += 1  # another study's peak may be in another space
    # An analysis whose every point restates a table analysis is that analysis.
    for group in restated:
        if not groups[group].coordinates:
            del groups[group]

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
        "restated": sum(d["points"] for d in restated.values()),
        "restated_points": list(restated.values()),
        "at_table_peaks": at_table_peak,
        "kept": dict(kept),
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
        return fingerprint("resolve", RESOLVE_VERSION, SAME_PEAK_MM,
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
            read = prose.summary.get("coordinates") or prose.summary.get("analyses")
            if not read and not (tables and tables.summary.get("tables")):
                plan.skipped += 1  # neither the tables nor the prose hold a point to upload
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

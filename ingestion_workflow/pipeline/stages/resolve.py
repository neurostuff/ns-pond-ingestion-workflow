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


def _near(a: Sequence[float], b: Sequence[float]) -> bool:
    """Whether two points are one peak (`SAME_PEAK_MM`)."""
    return all(abs(u - v) <= SAME_PEAK_MM for u, v in zip(a, b))


def resolve(tables: Dict[str, Any], prose: Dict[str, Any], slug: str,
            identifier=None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The table collections, plus one collection of what the prose adds.

    A prose analysis is a passage's analysis, one role per analysis; each
    unnamed one is its own, in the order the passage gives them. The same
    name in another passage is the same analysis when the two share a peak,
    and another analysis when they share none. A point at a table peak
    restates that table, whatever either is named: the analysis lists it in
    `restated_points` instead of its coordinates. An analysis whose every
    point restates a table keeps them and is marked `restatement`, so it is
    recorded but not uploaded as a second result. A coordinate the prose
    reports under two contrasts stays under both.
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

    # Each passage's analyses, keyed by (passage, name or position, role).
    found: Dict[Tuple, Dict[str, Any]] = {}
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
                identity = _norm(name)
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
                found.setdefault((index, identity, role), {
                    "name": name, "role": role, "named": named, "passages": [index],
                    "ordinal": ordinal, "unwritten": a.get("unwritten", 0), "points": []})
            for p in points:
                xyz = (float(p["x"]), float(p["y"]), float(p["z"]))
                found[(index, identity, p.get("role") or "other")]["points"].append(
                    (xyz, p, space, a.get("measure")))

    groups: List[Dict[str, Any]] = []
    by_name: Dict[Tuple, List[Dict[str, Any]]] = collections.defaultdict(list)
    for (_, identity, role), entry in found.items():
        if entry["named"]:
            same = next((g for g in by_name[(identity, role)]
                         if any(_near(p[0], q[0]) for p in entry["points"] for q in g["points"])),
                        None)
            if same is not None:
                same["passages"].append(entry["passages"][0])
                same["unwritten"] += entry["unwritten"]
                same["points"] += entry["points"]
                continue
            by_name[(identity, role)].append(entry)
        groups.append(entry)

    analyses: List[Analysis] = []
    kept = collections.Counter()
    restated: List[Dict[str, Any]] = []
    spaces = collections.Counter()
    for entry in groups:
        role = entry["role"]
        analysis = Analysis(name=entry["name"], table_id="prose", metadata={
            "source": "prose", "role": role, "passages": entry["passages"],
            "ordinal": entry["ordinal"], "unwritten": entry["unwritten"]})
        seen, new, restating = set(), [], []
        for xyz, p, space, measure in entry["points"]:
            key = tuple(round(v) for v in xyz)
            if key in seen:
                continue  # the model repeating itself
            seen.add(key)
            if space is not None and role in UPLOADED_PROSE_ROLES:
                spaces[space] += 1  # another study's peak may be in another space
            size = p.get("cluster_size")
            coordinate = Coordinate(
                x=xyz[0], y=xyz[1], z=xyz[2], space=space,
                statistic_value=_number(p.get("value")), statistic_type=p.get("statistic"),
                cluster_size=int(size) if isinstance(size, (int, float)) else None,
                cluster_measure=measure if size is not None else None,
            )
            restates = []
            for t, table, n in table_points:
                if _near(xyz, t) and {"table": table, "analysis": n} not in restates:
                    restates.append({"table": table, "analysis": n})
            (restating if restates else new).append((coordinate, restates))
        if restating:
            analysis.metadata["restated_points"] = [
                {"x": c.x, "y": c.y, "z": c.z, "restates": r} for c, r in restating]
            tables_restated = []
            for _, r in restating:
                tables_restated += [t for t in r if t not in tables_restated]
            restated.append({"passages": entry["passages"], "analysis": entry["name"],
                             "role": role, "points": len(restating),
                             "restates": tables_restated, "restatement": not new})
        if restating and not new:
            analysis.metadata["restatement"] = True
            analysis.coordinates = [c for c, _ in restating]
        else:
            analysis.coordinates = [c for c, _ in new]
            kept[role] += len(new)
        analyses.append(analysis)

    out = dict(tables or {})
    if analyses:
        stated = _space((prose or {}).get("space"))
        if spaces:
            space = spaces.most_common(1)[0][0]
        elif stated is not None:
            space = stated
        elif table_spaces:
            space = CoordinateSpace(table_spaces.most_common(1)[0][0])
        else:
            space = None
        out["prose"] = AnalysisCollection(slug=slug, identifier=identifier, analyses=analyses,
                                          coordinate_space=space).to_dict()
    summary = {
        "tables": sum(1 for blob in out.values() if (blob or {}).get("analyses")),
        "prose_analyses": len(analyses),
        "restatements": sum(1 for a in analyses if a.metadata.get("restatement")),
        "prose_points": sum(kept.values()),
        "restated": sum(d["points"] for d in restated),
        "restated_points": restated,
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
            # nothing but restatements, the upload is the tables' upload, so
            # it keeps the fingerprint it already had.
            adds = summary["prose_analyses"] - summary["restatements"]
            summary["basis"] = (tables_artifact.fingerprint
                                if not adds and tables_artifact is not None else "")
            yield Outcome(article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                          fingerprint=work.fingerprint, payload=payload, summary=summary)

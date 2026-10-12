"""Merge an article's table analyses with the results its prose adds."""

from __future__ import annotations

import collections
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from study_schema.keys import normalize_name

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import Analysis, AnalysisCollection, Coordinate
from ingestion_workflow.models.analysis import UNKNOWN_SPACES, CoordinateSpace
from ingestion_workflow.models.statistics import normalize_statistic_kind

from ..plan import StagePlan, Work
from ..stage import Context, take_back_or_block, taking_back
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page

RESOLVE_VERSION = 10

#: How close a prose coordinate may sit to a table's and still be the same
#: peak: papers round the same voxel differently between text and table.
SAME_PEAK_MM = 1.0

#: How far a prose statistic may differ from the table row's and still be its
#: value: the text rounds to two decimals.
SAME_VALUE = 0.011

UNNAMED = "unnamed prose analysis"


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


def _agrees(kind: Any, value: Optional[float], table_kind: Any, table_value: Any) -> bool:
    """Whether a prose statistic is the table row's: the same kind and the
    same value to rounding, either being absent when it is not stated."""
    kind, table_kind = normalize_statistic_kind(kind), normalize_statistic_kind(table_kind)
    if kind and table_kind and kind != table_kind:
        return False
    table_value = _number(table_value)
    return value is None or table_value is None or abs(abs(value) - abs(table_value)) <= SAME_VALUE


def _same_statistic(p: Dict[str, Any], q: Dict[str, Any]) -> bool:
    """Whether two prose points carry one statistic: both none, or the same
    kind and the same value to rounding. A coordinate without a statistic (a
    seed) is not a result peak that has one."""
    kinds = [normalize_statistic_kind(r.get("statistic")) for r in (p, q)]
    values = [_number(r.get("value")) for r in (p, q)]
    stated = [bool(k) or v is not None for k, v in zip(kinds, values)]
    if stated[0] != stated[1]:
        return False
    if kinds[0] and kinds[1] and kinds[0] != kinds[1]:
        return False
    return None in values or abs(abs(values[0]) - abs(values[1])) <= SAME_VALUE


def _overlap(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Whether the sets share a point, at one statistic."""
    return any(_near(p[0], q[0]) and _same_statistic(p[1], q[1])
               for p in a["points"] for q in b["points"])


def _contradict(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Whether the sets have a point in common whose statistics differ."""
    return any(_near(p[0], q[0]) and not _same_statistic(p[1], q[1])
               for p in a["points"] for q in b["points"])


def _joined(found: List[Tuple[tuple, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """The passages' analyses, each name's copies in other passages joined into
    its first when they share a point (`SAME_PEAK_MM`) at one statistic, or a
    section heading with no point shared at a different one.

    Each join is listed in the first's `joined`: the two passages, and
    whether a point or the heading joined them. Different names and unnamed
    analyses never join.
    """
    into: Dict[int, int] = {}  # index in `found` -> the index it joined

    def root(i: int) -> int:
        while i in into:
            i = into[i]
        return i

    joins = []
    for j, ((_, identity, *_), entry) in enumerate(found):
        for i in range(j):
            (_, other, *_), first = found[i]
            named = first["named"] and entry["named"]
            if not named or other != identity or root(i) == root(j):
                continue
            by = ("shared point" if _overlap(first, entry)
                  else "heading" if (entry["heading"] and entry["heading"] == first["heading"]
                                     and not _contradict(first, entry))
                  else None)
            if by is not None:
                a, b = sorted((root(i), root(j)))
                into[b] = a
                joins.append((i, j, by))
    for i, j, by in joins:
        found[root(j)][1].setdefault("joined", []).append(
            {"passages": [found[i][1]["passages"][0], found[j][1]["passages"][0]], "by": by})
    groups = []
    for j, (_, entry) in enumerate(found):
        r = root(j)
        if r == j:
            groups.append(entry)
            continue
        first = found[r][1]
        first["passages"] = sorted(set(first["passages"] + entry["passages"]))
        first["unwritten"] += entry["unwritten"]
        first["points"] += entry["points"]
    return groups


def resolve(tables: Dict[str, Any], prose: Dict[str, Any], slug: str,
            identifier=None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The table collections, plus one collection of what the prose adds.

    One analysis per name within a passage. The same name in another passage
    is the same analysis seen again when the two share a point at one statistic
    or a section heading (`_joined`); with neither, such as the cue stage's and the pain
    stage's peaks of one contrast, they are different sets. An unnamed
    analysis is its own set, in the model's order. A named analysis the model
    gave no point is kept, empty.
    What a set is for is the roles stage's to decide, not the prose model's.

    A point at a table peak restates that table, whatever either is named,
    when its statistic is the table row's (`_agrees`); a different statistic
    at the peak, such as a correlation with behaviour, is a result of its own.
    The analysis lists a restated point in `metadata.restated_points`, with
    what it restates, instead of its coordinates. An analysis whose every
    point restates a table is the table's result again: it is left out. The
    summary's `restated_points` records every analysis with a restated point,
    with those points, for the parse to list among its omissions. A coordinate
    the prose reports under two contrasts stays under both. Within a set,
    points at the same place are one point, whatever statistic each gives: the
    first is kept, every repeat is listed in `metadata.dropped`, and each other
    statistic a repeat gives is kept as the point's `other_statistics`.
    """
    table_points = [
        ((c["x"], c["y"], c["z"]), key, a.get("name") or "",
         c.get("statistic_type"), c.get("statistic_value"))
        for key, blob in (tables or {}).items()
        for a in (blob or {}).get("analyses", [])
        for c in a.get("coordinates", [])
    ]
    table_spaces = collections.Counter(
        (blob or {}).get("coordinate_space") for blob in (tables or {}).values()
        if (blob or {}).get("analyses")
        and (blob or {}).get("coordinate_space") not in UNKNOWN_SPACES)

    # Each passage's analyses: (passage, normalized name), or (passage, "", position) unnamed.
    found: Dict[tuple, Dict[str, Any]] = {}
    article_space = _space((prose or {}).get("space"))
    for index, passage in enumerate((prose or {}).get("passages", [])):
        space = _space(passage.get("space"))
        if space is None:
            space = article_space  # the space the article's Methods state
        unnamed = 0
        for position, a in enumerate(passage.get("analyses", [])):
            name = (a.get("name") or "").strip()
            named = bool(name)
            if named:
                group = (index, normalize_name(name))
            else:
                unnamed += 1
                name = UNNAMED if unnamed == 1 else f"{UNNAMED} {unnamed}"
                group = (index, "", position)
            entry = found.setdefault(group, {
                "name": name, "named": named, "passages": [index],
                "heading": (passage.get("heading") or "").strip(),
                "ordinal": position, "unwritten": 0, "points": []})
            entry["unwritten"] += a.get("unwritten", 0)
            entry["points"] += [((float(p["x"]), float(p["y"]), float(p["z"])), p, space,
                                 a.get("measure")) for p in a.get("points", [])]

    groups = _joined(list(found.items()))
    joins = collections.Counter(j["by"] for g in groups for j in g.get("joined", ()))

    analyses: List[Analysis] = []
    restated: List[Dict[str, Any]] = []
    repeated = merged = 0
    spaces = collections.Counter()
    for entry in groups:
        analysis = Analysis(name=entry["name"], table_id="prose", metadata={
            "source": "prose", "passages": entry["passages"],
            "ordinal": entry["ordinal"], "unwritten": entry["unwritten"]})
        if entry.get("joined"):
            analysis.metadata["joined"] = entry["joined"]
        seen, new, restating, dropped = {}, [], [], []
        for xyz, p, space, measure in entry["points"]:
            key = tuple(round(v) for v in xyz)
            value = _number(p.get("value"))
            if key in seen:
                dropped.append({"x": xyz[0], "y": xyz[1], "z": xyz[2],
                                "reason": "repeats a point of this analysis"})
                kept = seen[key]
                stated = {"statistic_type": p.get("statistic"), "statistic_value": value}
                if value is not None and stated not in [
                        {"statistic_type": kept.statistic_type,
                         "statistic_value": kept.statistic_value}, *kept.other_statistics]:
                    kept.other_statistics.append(stated)
                continue
            size = p.get("cluster_size")
            coordinate = Coordinate(
                x=xyz[0], y=xyz[1], z=xyz[2], space=space,
                statistic_value=value, statistic_type=p.get("statistic"),
                cluster_size=int(size) if isinstance(size, (int, float)) else None,
                cluster_measure=measure if size is not None else None,
            )
            seen[key] = coordinate
            restates = []
            for t, table, n, kind, table_value in table_points:
                if (_near(xyz, t) and _agrees(p.get("statistic"), value, kind, table_value)
                        and {"table": table, "analysis": n} not in restates):
                    restates.append({"table": table, "analysis": n})
            if restates:
                restating.append((coordinate, restates))
            else:
                new.append(coordinate)
                if space is not None:
                    spaces[space] += 1
        if dropped:
            analysis.metadata["dropped"] = dropped
            repeated += len(dropped)
            merged += sum(1 for c in seen.values() if c.other_statistics)
        points = [{"x": c.x, "y": c.y, "z": c.z, "restates": r} for c, r in restating]
        if restating:
            restated.append({"passages": entry["passages"], "analysis": entry["name"],
                             "points": points, "restatement": not new})
        if restating and not new:
            continue
        if restating:
            analysis.metadata["restated_points"] = points
        analysis.coordinates = new
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
        "prose_points": sum(len(a.coordinates) for a in analyses),
        "restatements": sum(1 for d in restated if d["restatement"]),
        "restated": sum(len(d["points"]) for d in restated),
        "restated_points": restated,
        "repeated": repeated,
        "merged_statistics": merged,
        "joins": dict(sorted(joins.items())),
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
        return fingerprint("resolve", RESOLVE_VERSION, SAME_PEAK_MM, SAME_VALUE,
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
        downloads = ctx.catalog.artifacts(ids, "download")
        extracted = ctx.catalog.artifacts(ids, "extract")
        for ref in refs:
            prose = upstream.get(ref.id, {}).get("")
            if prose is None or prose.status is not Status.OK:
                take_back_or_block(plan, ref, prose, artifacts.get(ref.id, {}).get(""))
                continue
            if read_from_bad_page(prose, downloads.get(ref.id, {}), extracted.get(ref.id, {})):
                plan.block(f"prose {READ_FROM_BAD_PAGE}")  # an earlier resolve stays as it is
                continue
            # The tables' analyses when there are any. Nothing waits for them:
            # when they arrive the fingerprint below changes, and this re-runs.
            analyses = analysed.get(ref.id, {}).get("")
            tables = analyses if analyses is not None and analyses.status is Status.OK else None
            read = prose.summary.get("coordinates") or prose.summary.get("analyses")
            if not read and not (tables and tables.summary.get("tables")):
                plan.skipped += 1  # neither the tables nor the prose hold an analysis
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
        back, works = taking_back(self.name, works)
        yield from back
        analysed = ctx.catalog.artifacts([w.article_id for w in works], "analyses")
        for work in works:
            tables_artifact = analysed.get(work.article_id, {}).get("")
            tables_ok = tables_artifact is not None and tables_artifact.ok
            tables = ctx.payload(tables_artifact) if tables_ok else {}
            payload, summary = resolve(tables or {}, ctx.payload(work.upstream) or {},
                                       work.ref.identifier.slug, work.ref.identifier)
            # What upload's freshness should follow. When the prose adds
            # nothing, the upload is the tables' upload, so it keeps the
            # fingerprint it already had.
            summary["prose_read"] = work.upstream.fingerprint
            summary["basis"] = (tables_artifact.fingerprint
                                if not summary["prose_analyses"] and tables_artifact is not None
                                else "")
            yield Outcome(article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                          fingerprint=work.fingerprint, payload=payload, summary=summary)

"""Write the parsed paper and the coordinate parse: study_schema's paper-parse contract.

Two files per paper, beside `stage1/analyses.json` until pondie reads them:

    <study>/parse/parsed_paper.json      ParsedPaper: the text every offset points into,
                                         its tables row by row, the bibliography
    <study>/parse/coordinate_parse.json  CoordinateParse: every analysis, keyed by the
                                         cells it was read from

Both are built as study_schema's generated models, so a field the contract does not
name cannot be written. Nothing here decides anything new: the text is extract's,
the meta-analysis verdict triage's, the readings and the split the analyses stage's,
each set's role the roles stage's. What this adds is where each analysis sits in its
table -- the model's points carry no row -- because the key is made from the cells.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import re
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from study_schema import keys, layouts
from study_schema.models import paper_parse as pp

from ingestion_workflow.extractors.utils import normalize_minus
from ingestion_workflow.models import Analysis, AnalysisCollection, ArticleExtractionBundle
from ingestion_workflow.models.statistics import point_values
from ingestion_workflow.services import coordinate_text
from ingestion_workflow.services.coordinate_flags import PLACEHOLDER_NAME, SPLIT_RULE
from ingestion_workflow.services.coordinate_space import sectionize
from ingestion_workflow.services.create_analyses import table_reading
from ingestion_workflow.services.logging import get_logger
from ingestion_workflow.services.naming import sanitize_table_id
from ingestion_workflow.services.set_roles.labels import is_decided

logger = get_logger(__name__)

#: Bump when the same inputs would give different files.
PAPER_PARSE_VERSION = 4
PRODUCER = "ns-pond-ingestion-workflow"
PARSE_DIR = "parse"


_SECTIONS = {
    "methods": "methods",
    "results": "results",
    "discussion": "discussion",
    "abstract": "abstract",
    "intro": "introduction",
    "back": "references",
}
_MEASURES = {"voxels": "voxels", "voxel": "voxels", "mm3": "mm3", "cm3": "cm3"}
#: How far a model's copy of a number may sit from the cell it copied.
_SAME_NUMBER = 0.051


@dataclass
class ParseInputs:
    """What the sync stage holds beside the bundle that the two files need."""

    article_id: str
    base_study_id: Optional[str] = None
    #: The triage payload: `is_meta_analysis`, `publication_types`, `tables[]`.
    triage: Optional[Mapping[str, Any]] = None
    #: table_id -> {reason, note}: tables a person marked as holding no coordinates.
    excluded: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    #: The analyses stage's summary `readings` / `unread`; None when not recorded.
    readings: Optional[Mapping[str, str]] = None
    unread: Optional[Mapping[str, str]] = None
    #: The prose stage payload (its `passages[].text` locate a prose analysis).
    prose: Optional[Mapping[str, Any]] = None
    #: How many passages the passages stage kept, and resolve's restatement count.
    passages_kept: Optional[int] = None
    restated: Optional[int] = None
    #: artifact kind -> fingerprint of every catalog artifact read.
    fingerprints: Mapping[str, str] = field(default_factory=dict)


@dataclass
class _Table:
    headings: List[str]
    rows: List[List[Tuple[str, int]]]  # body rows: (cell text, resolved column)
    _index: Optional[list] = field(default=None, repr=False)

    def index(self) -> list:
        """Per body row, its numbers and the triples printed in one cell, read once."""
        if self._index is None:
            from nspond_tables.grid import as_number
            from nspond_tables.read import triples_in

            self._index = [
                (
                    [(as_number(text), col) for text, col in cells],
                    [(trio, col) for text, col in cells for trio in triples_in(text)],
                )
                for cells in self.rows
            ]
        return self._index


#: Characters a passage and the text print differently: superscript digits.
_FOLD = str.maketrans("\u2070\u00b9\u00b2\u00b3\u2074\u2075\u2076\u2077\u2078\u2079", "0123456789")
_ALNUM = re.compile(r"[^\W_]+")


def _squash(text: str) -> Tuple[str, array]:
    """`text`'s letters and digits only, lower case, and where each one sits in `text`."""
    runs, where = [], array("l")
    for match in _ALNUM.finditer(text.translate(_FOLD)):
        run = match.group().lower()
        if len(run) != match.end() - match.start():  # a letter whose lower case is longer
            run = match.group()
        runs.append(run)
        where.extend(range(match.start(), match.end()))
    return "".join(runs), where


class _Text:
    """The parsed paper's text, and its letters and digits alone, built on first use.

    The passages stage and the extractor print one sentence with different spaces,
    minus signs, punctuation and superscripts; their letters and digits agree.
    """

    def __init__(self, text: str) -> None:
        self.text = text
        self._squashed: Optional[Tuple[str, array]] = None

    def __len__(self) -> int:
        return len(self.text)

    def find(self, needle: str, start: int = 0) -> Optional[Tuple[int, int]]:
        """(start, end) in the text of the first copy of `needle`'s letters and digits
        that begins at or after `start`, or None."""
        wanted, _ = _squash(needle)
        if not wanted:
            return None
        if self._squashed is None:
            self._squashed = _squash(self.text)
        squashed, where = self._squashed
        at = squashed.find(wanted, _bisect(where, start))
        if at < 0:
            return None
        return where[at], where[at + len(wanted) - 1] + 1


def _bisect(where: array, offset: int) -> int:
    from bisect import bisect_left

    return bisect_left(where, offset)


# -- the parsed paper --------------------------------------------------------------------


def parsed_paper(root: Path, bundle: ArticleExtractionBundle, inputs: ParseInputs):
    """The ParsedPaper for an article already written under `root`, or None without text.

    `root` is the study's directory; its `processed/<source>/text*` is the text.
    """
    content, meta = bundle.article_data, bundle.article_metadata
    source = content.source.value
    text_file = _text_file(root, source)
    if text_file is None:
        return None, None, {}
    raw = text_file.read_bytes()
    text = _Text(raw.decode("utf-8", errors="replace"))

    from ingestion_workflow.pipeline.stages.triage import is_a_meta_analysis, publication_types

    triage = inputs.triage or {}
    title = meta.title or content.slug
    types = list(triage.get("publication_types") or publication_types(meta.to_dict()))
    is_meta = triage.get("is_meta_analysis")
    if is_meta is None:
        is_meta = is_a_meta_analysis(title, types)
    verdicts = {str(v.get("table_id")): v for v in triage.get("tables") or [] if v.get("table_id")}

    grids: Dict[str, _Table] = {t.table_id: _grid(t.raw_content_path) for t in content.tables}
    inlined = _inlined_tables(content.tables, grids, text)
    tables = [
        _parsed_table(
            root,
            index,
            table,
            grids[table.table_id],
            verdicts.get(table.table_id),
            inputs.excluded.get(table.table_id),
            spans,
        )
        for index, (table, spans) in enumerate(zip(content.tables, inlined))
    ]

    paper = pp.ParsedPaper(
        header=_header(
            "parsed_paper",
            inputs,
            _identifiers(content.identifier, inputs),
            "sync",
            [
                ("extraction", inputs.fingerprints.get("extract")),
                ("metadata", inputs.fingerprints.get("metadata")),
                ("triage", inputs.fingerprints.get("triage")),
            ],
        ),
        source=source,
        text_path=text_file.relative_to(root).as_posix(),
        text_sha256=hashlib.sha256(raw).hexdigest(),
        text_length=len(text.text),
        bibliography=_bibliography(meta, title, types),
        is_meta_analysis=bool(is_meta),
        is_meta_analysis_basis=_meta_basis(title, types) if is_meta else None,
        sections=_sections(text.text),
        tables=tables,
    )
    return paper, text, grids


def _text_file(root: Path, source: str) -> Optional[Path]:
    found = sorted((root / "processed" / source).glob("text.*"))
    return found[0] if found else None


def _bibliography(meta, title: str, types: Sequence[str]):
    raw = (((meta.raw_metadata or {}).get("pubmed") or {}).get("MedlineCitation") or {}).get(
        "Article"
    ) or {}
    language = raw.get("Language")
    language = (
        [language]
        if isinstance(language, str)
        else [x for x in language or [] if isinstance(x, str)]
    )
    return pp.Bibliography(
        title=title,
        authors=[a.name for a in meta.authors] or None,
        journal=meta.journal,
        publication_year=meta.publication_year,
        abstract=meta.abstract,
        language=language or None,
        publication_types=list(types) or None,
        keywords=list(meta.keywords) or None,
        license=meta.license,
        open_access=meta.open_access,
        provider=meta.source,
    )


def _meta_basis(title: str, types: Sequence[str]) -> Optional[str]:
    from ingestion_workflow.pipeline.stages.triage import is_a_meta_analysis

    by_type = is_a_meta_analysis("", types)
    by_title = is_a_meta_analysis(title, ())
    if by_type and by_title:
        return "publication_type_and_title"
    return "publication_type" if by_type else ("title" if by_title else None)


def _sections(text: str):
    out = []
    for start, end, label in sectionize(text):
        if label == "unknown":
            continue
        out.append(pp.Section(kind=_SECTIONS.get(label, "other"), start_char=start, end_char=end))
    return out or None


def _grid(raw_path) -> _Table:
    """The table's header cells and body rows, as nspond_tables reads them."""
    from nspond_tables import serialize
    from nspond_tables.grid import is_header_row

    try:
        markup = Path(raw_path).read_text(encoding="utf-8", errors="ignore")
        grid = serialize.from_source(normalize_minus(markup))
        resolved = grid.resolve()
    except Exception as exc:  # noqa: BLE001 - an unreadable table has no rows, not a failed paper
        logger.debug("could not read table %s: %s", raw_path, exc)
        return _Table([], [])
    headings: List[str] = []
    rows: List[List[Tuple[str, int]]] = []
    for row in resolved:
        if is_header_row(row):
            if not rows:
                headings = [p.cell.text for p in row]
            continue
        rows.append([(p.cell.text, p.col) for p in row])
    return _Table(headings, rows)


def _parsed_table(root: Path, index: int, table, grid: _Table, verdict, excluded, inlined):
    triage = None
    if verdict is not None or excluded is not None:
        verdict = verdict or {}
        triage = pp.TableTriage(
            passes=bool(verdict.get("passes", False)),
            score=verdict.get("score"),
            route=verdict.get("route"),
            located_by=verdict.get("located_by"),
            excluded_by_hand=True if excluded is not None else None,
            exclusion_reason=_exclusion_reason(excluded),
        )
    space = getattr(table.space, "value", table.space)
    raw = Path(table.raw_content_path) if table.raw_content_path else None
    return pp.ParsedTable(
        table_id=table.table_id,
        number=str(table.table_number) if table.table_number is not None else None,
        caption=table.caption or None,
        footer=table.footer or None,
        raw_path=_raw_path(root, index, table.table_id, raw),
        column_headings=grid.headings or None,
        rows=[
            pp.TableRow(row=i, cells=[text for text, _ in cells], text_span=_span(inlined.get(i)))
            for i, cells in enumerate(grid.rows)
        ],
        text_span=_span(inlined.get("table")),
        coordinate_space_hint=str(space) if space else None,
        triage=triage,
    )


#: Fewest letters and digits a row needs before its place in the text is trusted.
_ROW_CHARS = 8
#: Furthest one inlined row may sit from the one before it.
_ROW_GAP = 2000


#: How much of a caption's first line is looked for in the text.
_CAPTION_CHARS = 80


def _inlined_tables(tables, grids: Mapping[str, _Table], text: _Text) -> List[Dict]:
    """`_inlined` for each of the paper's tables, each searched only in its own block.

    Two tables can print the same row (a shared header), so a row is looked for only
    between where its table is printed and where the next one is. A table is printed
    where its caption, or failing that its "Table N" line, starts a line, in the
    order the tables come; one found in neither way (a second parser's copy of a
    table) is searched for after the table before it, up to the next one found.
    """
    anchors: List[Optional[Tuple[int, int]]] = []
    at = 0
    for table in tables:
        anchor = _anchor(table, text, at)
        anchors.append(anchor)
        if anchor is not None:
            at = anchor[1]
    out = []
    at = 0
    for i, table in enumerate(tables):
        lo = anchors[i][1] if anchors[i] is not None else at
        hi = next((a[0] for a in anchors[i + 1 :] if a is not None and a[0] >= lo), len(text))
        spans = _inlined(grids[table.table_id], text, lo, hi)
        out.append(spans)
        at = spans["table"][1] if "table" in spans else lo
    return out


def _anchor(table, text: _Text, start: int) -> Optional[Tuple[int, int]]:
    """Where the table's caption, or its "Table N" line, starts a line at or after `start`."""
    caption = (table.caption or "").strip().split("\n")[0][:_CAPTION_CHARS]
    if len(_squash(caption)[0]) >= _ROW_CHARS:
        found = _line_find(text, caption, start)
        if found is not None:
            return found
    if table.table_number is not None:
        label = f"Table {table.table_number}"
        found = _line_find(text, label, start, whole=True)
        if found is not None:
            return found
    return None


def _line_find(
    text: _Text, needle: str, start: int, end: Optional[int] = None, whole: bool = False
) -> Optional[Tuple[int, int]]:
    """The first copy of `needle` in [start, end) that starts a line (and, if `whole`,
    ends it)."""
    end = len(text) if end is None else end
    while True:
        span = text.find(needle, start)
        if span is None or span[1] > end:
            return None
        line_start = text.text.rfind("\n", 0, span[0]) + 1
        line_end = text.text.find("\n", span[1])
        line_end = len(text) if line_end < 0 else line_end
        if not _ALNUM.search(text.text, line_start, span[0]) and not (
            whole and _ALNUM.search(text.text, span[1], line_end)
        ):
            return span
        start = span[0] + 1


def _inlined(
    grid: _Table, text: _Text, lo: int = 0, hi: Optional[int] = None
) -> Dict[Any, Tuple[int, int]]:
    """Where the table's body rows are inlined in text[lo:hi], row by row, and the table.

    A source that inlines its tables (pubget, elsevier) prints each row on a line of
    its own, so a row is placed only where it starts a line, in order, near the row
    before it. The table's span runs from its first placed row to its last, and is
    given only when most of its rows were placed.
    """
    found: Dict[Any, Tuple[int, int]] = {}
    at = lo
    for i, cells in enumerate(grid.rows):
        line = " ".join(cell for cell, _ in cells)
        if len(_squash(line)[0]) < _ROW_CHARS:
            continue
        span = _line_find(text, line, at, hi)
        if span is not None and found and span[0] - at > _ROW_GAP:
            span = None
        if span is not None:
            found[i] = span
            at = span[1]
    rows = [v for k, v in found.items() if k != "table"]
    if rows and 2 * len(rows) > len(grid.rows):
        found["table"] = (rows[0][0], rows[-1][1])
    return found


def _span(span: Optional[Tuple[int, int]]):
    return pp.TextSpan(start_char=span[0], end_char=span[1]) if span else None


def _raw_path(root: Path, index: int, table_id: str, path: Optional[Path]) -> Optional[str]:
    """Where sync copied the raw table in the bundle (`nspond._write_sources`), if it did."""
    if path is None:
        return None
    name = f"{sanitize_table_id(table_id, index)}{path.suffix or '.html'}"
    found = sorted((root / "source").glob(f"*/tables/{name}"))
    return found[0].relative_to(root).as_posix() if found else None


def _exclusion_reason(excluded) -> Optional[str]:
    if not excluded:
        return None
    parts = [str(excluded.get(k)) for k in ("reason", "note") if excluded.get(k)]
    return "; ".join(parts) or None


def _identifiers(identifier, inputs: ParseInputs):
    return pp.ArticleIdentifiers(
        pmid=getattr(identifier, "pmid", None) or None,
        pmcid=getattr(identifier, "pmcid", None) or None,
        doi=getattr(identifier, "doi", None) or None,
        neurostore_base_study_id=inputs.base_study_id,
    )


def _header(kind: str, inputs: ParseInputs, ids, stage: str, refs):
    return pp.ArtifactHeader(
        artifact_kind=kind,
        schema_version=pp.version,
        article_id=inputs.article_id,
        identifiers=ids,
        producer=pp.Producer(
            name=PRODUCER, version=f"paper_parse.{PAPER_PARSE_VERSION}", stage=stage
        ),
        inputs=[pp.InputRef(artifact_kind=k, fingerprint=f) for k, f in refs if f] or None,
    )


# -- the coordinate parse ----------------------------------------------------------------


def coordinate_parse(
    paper,
    text,
    grids: Mapping[str, _Table],
    per_table: Mapping[str, AnalysisCollection],
    inputs: ParseInputs,
    splits: Optional[Dict[int, dict]] = None,
):
    """The CoordinateParse, and the analyses that could not be written into it.

    `splits`, when given, is filled with the SignSplit each split stage analysis
    was declared with in the parse, by `id()` of the stage analysis, so stage1
    carries the same declaration, keys included.

    An analysis whose cells or characters cannot be found has no key, so it is left
    out with its reason rather than given a made-up key. Two analyses with one key
    (the same cells or spans and normalized name) are one reported analysis: the
    first holds the points of both, and the second is noted as merged into it, or
    omitted as a repeat when it adds no point. The reasons of a table's analyses are also the `reason` of its
    TableReading; those of the text are the parse's `omitted_analyses`.
    """
    text = text if isinstance(text, _Text) else _Text(text)
    omitted: List[Omitted] = []
    roles = _Roles(text, omitted)
    analyses: List[pp.ParsedAnalysis] = []
    seen: Dict[str, pp.ParsedAnalysis] = {}
    for table_id, collection in per_table.items():
        table_analyses = []
        for a in collection.analyses:
            if a.name.strip().upper() == PLACEHOLDER_NAME and not a.coordinates:
                omitted.append(
                    Omitted(
                        a.name,
                        None if table_id == "prose" else table_id,
                        "a placeholder analysis with no points",
                    )
                )
            else:
                table_analyses.append(a)
        prose = table_id == "prose" or any(
            (a.metadata or {}).get("source") == "prose" for a in table_analyses
        )
        if prose:
            # Per name: two names never share a key, so one's copy of a repeated
            # point must not move the other's.
            taken: Dict[str, Set[Tuple[int, int]]] = {}
            built = (
                _prose_analysis(
                    a, collection, text, inputs, roles,
                    taken.setdefault(keys.normalize_name(a.name), set()),
                )
                for a in table_analyses
            )
        else:
            built = _table_analyses(
                table_id, table_analyses, collection, grids.get(table_id), roles
            )
            if splits is not None:
                splits.update(
                    (id(a), b.split.model_dump(mode="json", exclude_none=True))
                    for a, b in zip(table_analyses, built)
                    if b is not None and b.split is not None
                )
        for analysis in built:
            if analysis is None:
                continue
            first = seen.get(analysis.key)
            if first is not None:
                omitted.append(_merge(first, analysis))
                continue
            seen[analysis.key] = analysis
            analyses.append(analysis)

    payload = [a.model_dump(mode="json", exclude_none=True) for a in analyses]
    parse_id = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    parse = pp.CoordinateParse(
        header=_header(
            "coordinate_parse",
            inputs,
            paper.header.identifiers,
            "sync",
            [("parsed_paper", paper.text_sha256), ("analyses", inputs.fingerprints.get("space"))],
        ),
        parse_id=parse_id,
        text_sha256=paper.text_sha256,
        analyses=analyses,
        tables=_readings(paper, per_table, inputs, omitted) or None,
        omitted_analyses=[
            pp.OmittedAnalysis(
                name=o.name,
                text_spans=[pp.TextSpan(start_char=a, end_char=b) for a, b in o.spans] or None,
                reason=o.reason,
            )
            for o in omitted
            if o.table_id is None and not o.kept
        ]
        or None,
        text_sweep=_text_sweep(inputs),
    )
    return parse, omitted


def _role_of(analysis: pp.ParsedAnalysis) -> Dict[str, Any]:
    return {
        "role": analysis.role,
        "anchor_kind": analysis.anchor_kind,
        "from_prior_study": analysis.from_prior_study,
        "role_source": analysis.role_source,
    }


def _xyz(point: pp.ParsedPoint) -> Tuple[Any, ...]:
    return (*point.coordinates, point.space)


def _values(point: pp.ParsedPoint) -> List[Tuple[str, float]]:
    return sorted((str(v.kind), v.value) for v in point.values or [])


def _merge(first: pp.ParsedAnalysis, other: pp.ParsedAnalysis) -> Omitted:
    """Add `other`'s points to `first`, which holds its key, and the record saying so.

    A point at the same coordinates and space as one `first` has is the same point
    and is not added twice, even when only its statistic values differ: `first`'s
    stay, and the other values are written in the note. When no point is new,
    `other` is a repeat and is omitted as one.

    Entries of one key with different roles are still one analysis, but it must
    not silently take one of the roles: the note carries `role_conflict` (both
    roles and their sources) and the analysis is held for review.
    """
    what = "cells" if other.origin == "table" else "spans"
    have = {_xyz(p): p for p in first.points}
    new = [p for p in other.points if _xyz(p) not in have]
    differing = [
        f"{p.coordinates} kept {_values(have[_xyz(p)])}, dropped {_values(p)}"
        for p in other.points
        if _xyz(p) in have and _values(p) != _values(have[_xyz(p)])
    ]
    values = f"; statistic values differ at the same point: {'; '.join(differing)}" if differing else ""
    mine, theirs = _role_of(first), _role_of(other)
    conflict = None
    role = ""
    if (mine["role"], mine["anchor_kind"], mine["from_prior_study"]) != (
        theirs["role"], theirs["anchor_kind"], theirs["from_prior_study"],
    ):
        conflict = {"key": first.key, "roles": [mine, theirs]}
        role = (
            f"; role conflict, held for review: {mine['role']!r} ({mine['role_source']}) "
            f"and {theirs['role']!r} ({theirs['role_source']})"
        )
    if not new:
        reason = f"a repeat of {first.key} ({first.name!r}): the same {what}, name and points"
        return Omitted(
            other.name, other.table_id, reason + values + role, _span_pairs(other),
            role_conflict=conflict,
        )
    first.points = [*first.points, *new]
    first.statistic = _statistic(first.points)
    return Omitted(
        other.name,
        other.table_id,
        f"merged into {first.key} ({first.name!r}): the same {what} and name; "
        f"its {len(new)} other point(s) added{values}{role}",
        _span_pairs(other),
        kept=True,
        role_conflict=conflict,
    )


class MissingRole(ValueError):
    """A stage1 analysis without the role the roles stage decides: the article is not parsed."""


class _Roles:
    """Each analysis's role, as the roles stage wrote it under `metadata.set_role`.

    There is no default: an analysis without one raises `MissingRole`. Numbers the
    roles stage found are not brain coordinates are no coordinate set, so they are
    left out with that reason. A citing sentence the parsed paper's text does not
    hold cannot be a TextSpan; it is left out of the evidence and noted on the
    omission list instead.
    """

    def __init__(self, text: _Text, omitted: List[Omitted]) -> None:
        self.text = text
        self.omitted = omitted

    def of(self, analysis: Analysis, table_id: Optional[str]) -> Optional[Dict[str, Any]]:
        decided = (analysis.metadata or {}).get("set_role")
        if not is_decided(analysis.metadata or {}):
            raise MissingRole(
                f"analysis {analysis.name!r} of {table_id or 'the text'} has no role "
                "from the roles stage"
            )
        if decided.get("role") is None:
            self.omitted.append(
                Omitted(
                    analysis.name,
                    table_id,
                    f"not brain coordinates, as {decided['role_source']} decided",
                )
            )
            return None
        evidence = []
        for span in decided.get("prior_study_evidence") or []:
            found = self._place(span)
            if found is None:
                self.omitted.append(
                    Omitted(
                        analysis.name,
                        table_id,
                        f"kept; a citing sentence is not in the parsed paper's text: "
                        f"{(span.get('text') or '')[:80]!r}",
                        kept=True,
                    )
                )
            else:
                evidence.append(pp.TextSpan(start_char=found[0], end_char=found[1]))
        return {
            "role": decided["role"],
            "anchor_kind": decided.get("anchor_kind"),
            "from_prior_study": decided.get("from_prior_study"),
            "prior_study_evidence": evidence or None,
            "role_confidence": decided.get("role_confidence"),
            "role_source": decided["role_source"],
        }

    def _place(self, span: Mapping[str, Any]) -> Optional[Tuple[int, int]]:
        """The sentence's characters in the parsed paper's text, which may not be the text the roles stage read."""
        sentence = span.get("text") or ""
        start, end = span.get("start_char"), span.get("end_char")
        if sentence and isinstance(start, int) and self.text.text[start:end] == sentence:
            return start, end
        if not sentence.strip():
            return None
        at = self.text.text.find(sentence)
        return (at, at + len(sentence)) if at >= 0 else self.text.find(sentence)


@dataclass
class Omitted:
    """A stage1 analysis the parse could not hold, and why.

    A table's are written into its TableReading's `reason`, a text analysis's into
    the parse's `omitted_analyses`, and all of them into the sync summary.
    """

    name: str
    table_id: Optional[str]
    reason: str
    spans: Sequence[Tuple[int, int]] = ()
    #: A note on a kept analysis: in the sync summary, not among the parse's omissions.
    kept: bool = False
    #: Both roles of entries that share a key: the merged analysis is held for review.
    role_conflict: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        return f"{self.table_id or 'text'}: {self.name!r}: {self.reason}"


def _span_pairs(analysis: pp.ParsedAnalysis) -> List[Tuple[int, int]]:
    return [(s.start_char, s.end_char) for s in analysis.text_spans or []]


def _table_analyses(
    table_id: str,
    analyses: Sequence[Analysis],
    collection,
    grid: Optional[_Table],
    roles: _Roles,
) -> List[Optional[pp.ParsedAnalysis]]:
    grid = grid or _Table([], [])
    placed = _place_all(analyses, grid)
    # A column group is a block of coordinate columns: rank the x columns the
    # table's points were found in, so a table of one block is all 0.
    columns = sorted({col for points in placed for found in points if found for col in [found[1]]})
    group = {col: i for i, col in enumerate(columns)}

    out: List[Tuple[Optional[pp.ParsedAnalysis], Optional[dict]]] = []
    for analysis, found in zip(analyses, placed):
        cells = {(row, group[col]) for row, col in filter(None, found)}
        if not analysis.coordinates:
            row = _row_naming(grid, analysis.name)
            if row is not None:
                cells.add((row, 0))
        if not cells:
            roles.omitted.append(
                Omitted(
                    analysis.name,
                    table_id,
                    "no row of the table prints its points"
                    if analysis.coordinates
                    else "no row of the table names it",
                )
            )
            out.append((None, None))
            continue
        points = [
            _point(c, collection, found=f and (f[0], group[f[1]]))
            for c, f in zip(analysis.coordinates, found)
        ]
        out.append(
            _analysis(
                analysis,
                collection,
                points,
                roles,
                origin="table",
                table_id=table_id,
                key=keys.table_key(table_id, cells, analysis.name),
                cells=[pp.CellRef(row=r, column_group=g) for r, g in sorted(cells)],
            )
        )
    _declare_splits(out)
    return [analysis for analysis, _ in out]


def _candidates(coordinate, grid: _Table) -> List[Tuple[int, int]]:
    """Every (body row, x column) printing this point's x, y, z; one per row."""
    xyz = (coordinate.x, coordinate.y, coordinate.z)

    def same(trio) -> bool:
        return all(v is not None and abs(a - v) <= _SAME_NUMBER for a, v in zip(xyz, trio))

    found = []
    for index, (numbers, triples) in enumerate(grid.index()):
        hit = next(
            (
                numbers[i][1]
                for i in range(len(numbers) - 2)
                if same([n for n, _ in numbers[i : i + 3]])
            ),
            None,
        )
        if hit is None:
            hit = next((col for trio, col in triples if same(trio)), None)
        if hit is not None:
            found.append((index, hit))
    return found


def _place_all(
    analyses: Sequence[Analysis], grid: _Table
) -> List[List[Optional[Tuple[int, int]]]]:
    """(body row, x column) of each point of each analysis of one table.

    A point printed in one row is placed there. A point printed in several -- two
    contrasts reporting one peak -- goes to the row that fits its own analysis:
    in the column block its other points are in, not a row another analysis
    already holds, nearest its other rows, then nearest below the row naming it.
    """
    candidates = [[_candidates(c, grid) for c in a.coordinates] for a in analyses]
    sure = [{found[0] for found in points if len(found) == 1} for points in candidates]
    taken: Dict[Tuple[int, int], int] = {}
    for i, rows in enumerate(sure):
        for cell in rows:
            taken.setdefault(cell, i)
    placed = []
    for i, (analysis, points) in enumerate(zip(analyses, candidates)):
        own_rows = [r for r, _ in sure[i]]
        own_cols = {c for _, c in sure[i]}
        named = _row_naming(grid, analysis.name) if any(len(p) > 1 for p in points) else None

        def fit(cell, i=i, own_rows=own_rows, own_cols=own_cols, named=named):
            row, col = cell
            return (
                bool(own_cols) and col not in own_cols,
                taken.get(cell, i) != i,
                min((abs(row - r) for r in own_rows), default=0),
                row - named if named is not None and row > named else len(grid.rows),
                row,
            )

        chosen = []
        for found in points:
            cell = min(found, key=fit) if found else None
            if cell is not None:
                taken.setdefault(cell, i)
            chosen.append(cell)
        placed.append(chosen)
    return placed


def _row_naming(grid: _Table, name: str) -> Optional[int]:
    """The body row a contrast reported without coordinates is named in."""
    wanted = _norm(name)
    if not wanted:
        return None
    for index, cells in enumerate(grid.rows):
        if any(_norm(text) == wanted for text, _ in cells):
            return index
    for index, cells in enumerate(grid.rows):
        if wanted in _norm(" ".join(text for text, _ in cells)):
            return index
    return None


def _norm(text: str) -> str:
    return re.sub(r"\W+", " ", (text or "").lower()).strip()


def _mirror(xyz, text: str, windows) -> Optional[Tuple[int, int]]:
    """A copy in `windows` printing the point with one or more signs reversed.

    The matcher never takes "-42" for 42 (the other hemisphere), so a point whose
    only printed copy is its mirror has no span; this finds that copy, so the
    parse can say so.
    """
    for signs in itertools.product((1, -1), repeat=3):
        flipped = tuple(sign * v for sign, v in zip(signs, xyz))
        if flipped == tuple(xyz):
            continue
        for a, b in windows:
            found = coordinate_text.find_all(flipped, text, a, b)
            if found:
                return found[0]
    return None


def _prose_analysis(
    analysis: Analysis,
    collection,
    text: _Text,
    inputs: ParseInputs,
    roles: _Roles,
    taken: Optional[Set[Tuple[int, int]]] = None,
) -> Optional[pp.ParsedAnalysis]:
    """A text analysis, keyed by where its points are printed and by its name.

    One sentence can state several analyses, and two of them can share their
    points, so the name is part of the key. The passage is the fallback for an
    analysis none of whose points can be found. `taken` holds the characters the
    passage's earlier analyses' points were found at: a point printed again in
    the passage goes to its next copy, as a repeated table point goes to the row
    no other analysis took.
    """
    taken = set() if taken is None else taken
    passages = (inputs.prose or {}).get("passages") or []
    windows: List[Tuple[int, int]] = []
    for index in (analysis.metadata or {}).get("passages") or []:
        passage = passages[index].get("text") if 0 <= index < len(passages) else None
        if passage:
            windows += _locate(passage, text)
    point_spans = []
    for c in analysis.coordinates:
        xyz = coordinate_text.triple(c)
        point_spans.append(
            coordinate_text.find_point(xyz, text.text, windows, taken) if xyz else None
        )
        if point_spans[-1]:
            taken.add(point_spans[-1])
        elif xyz and (mirror := _mirror(xyz, text.text, windows)):
            roles.omitted.append(
                Omitted(
                    analysis.name,
                    None,
                    f"kept; point ({', '.join(f'{v:g}' for v in xyz)}) has no text_span: "
                    f"its passage prints it only with a sign reversed, "
                    f"{text.text[mirror[0]:mirror[1]]!r} at {mirror[0]}-{mirror[1]}",
                    kept=True,
                )
            )
    spans = sorted({s for s in point_spans if s}) or sorted(set(windows))
    if not spans:
        roles.omitted.append(
            Omitted(
                analysis.name,
                None,
                "neither its passages nor its points are in the parsed paper's text",
            )
        )
        return None
    key = keys.span_key("text", spans, analysis.name)
    points = [
        _point(c, collection, found=None, span=s)
        for c, s in zip(analysis.coordinates, point_spans)
    ]
    return _analysis(
        analysis,
        collection,
        points,
        roles,
        origin="text",
        table_id=None,
        key=key,
        text_spans=[pp.TextSpan(start_char=a, end_char=b) for a, b in spans],
    )[0]


#: Fewest letters and digits a piece of a passage needs to be placed on its own.
_PIECE = 24


def _locate(passage: str, text: _Text) -> List[Tuple[int, int]]:
    """Where a prose passage sits in the parsed paper's text, as (start, end) pieces.

    The passages stage cuts passages from its own reading of the download, joined
    sentence by sentence, so a passage is rarely a verbatim slice of text.txt: its
    spaces, minus signs and punctuation differ, a figure legend may be joined to the
    sentence after it, and a table inlined in text.txt may split a sentence. Exact
    first, then by its letters and digits, then sentence by sentence; a sentence the
    text does not hold is left out.
    """
    start = text.text.find(passage)
    if start >= 0:
        return [(start, start + len(passage))]
    whole = text.find(passage)
    if whole is not None:
        return [whole]
    pieces, at = [], 0
    for sentence in re.split(r"(?<=[.;!?])\s+|\n+", passage):
        if len(_squash(sentence)[0]) < _PIECE:
            continue
        found = text.find(sentence, at) or text.find(sentence)
        if found is not None:
            pieces.append(found)
            at = found[1]
    return pieces


def _analysis(
    analysis: Analysis,
    collection,
    points,
    roles: _Roles,
    *,
    origin,
    table_id,
    key,
    cells=None,
    text_spans=None,
):
    role = roles.of(analysis, table_id)
    if role is None:
        return None, False
    name = analysis.name
    built = pp.ParsedAnalysis(
        key=key,
        cells=cells,
        text_spans=text_spans,
        origin=origin,
        table_id=table_id,
        name=name,
        name_is_printed=False if name.strip().upper() == PLACEHOLDER_NAME else None,
        description=analysis.description or None,
        coordinate_space=_space_value(collection.coordinate_space),
        **role,
        statistic=_statistic(points),
        points=points,
    )
    return built, (analysis.metadata or {}).get("split")


def _statistic(points) -> Optional[str]:
    """The statistic every point's values share, or None."""
    kinds = {v.kind for p in points for v in p.values or [] if v.kind != "p"}
    return kinds.pop() if len(kinds) == 1 else None


def _declare_splits(analyses: List[Tuple[Optional[pp.ParsedAnalysis], Optional[dict]]]) -> None:
    """Copy the analyses stage's sign split into `split{}` on both halves.

    The stage declares it as `metadata["split"]`: `{"half": "original", "index": i}`
    on the analysis as named, and `{"half": "inverse", "original_index": i}` on the
    reversed contrast, `i` being the original's place among the stage's analyses
    when it split. The halves pair by that number, so an analysis dropped in
    between, or two originals with one name, cannot mispair them. The inverse half
    carries the name the stage gave the reversed contrast. An inverse half whose original is not in the parse
    (`original_index` is None, or names one that was not placed) is declared on
    its own; so is an original whose inverse is not in the parse.
    """
    rule = SPLIT_RULE
    originals = {
        split.get("index"): built
        for built, split in analyses
        if built is not None and split and split["half"] == "original"
    }
    for original in originals.values():
        original.split = pp.SignSplit(half="original", rule=rule)
    for built, split in analyses:
        if built is None or not split or split["half"] != "inverse":
            continue
        original = originals.get(split.get("original_index"))
        # The analyses stage composed its name from the original's (`inverse_name`).
        built.name_is_printed = False
        if original is None:
            built.split = pp.SignSplit(half="inverse", rule=rule)
            continue
        built.split = pp.SignSplit(half="inverse", original_analysis=original.key, rule=rule)


def _space_value(space) -> Optional[str]:
    """The space's string, or None when the table states none (null space)."""
    return space.value if space else None


def _point(coordinate, collection, found: Optional[Tuple[int, int]], span=None):
    space = coordinate.space.value if coordinate.space else None
    values = [
        pp.PointValue(**v)
        for v in point_values(coordinate.statistic_value, coordinate.statistic_type)
    ] or None
    measure = (
        str(coordinate.cluster_measure or "").strip().lower().replace("³", "3").replace("^", "")
    )
    return pp.ParsedPoint(
        coordinates=[coordinate.x, coordinate.y, coordinate.z],
        space=space if space and space != _space_value(collection.coordinate_space) else None,
        row=found[0] if found else None,
        column_group=found[1] if found else None,
        text_span=pp.TextSpan(start_char=span[0], end_char=span[1]) if span else None,
        values=values,
        cluster_size=coordinate.cluster_size,
        cluster_measure=(
            _MEASURES.get(measure, "other")
            if coordinate.cluster_size is not None and measure
            else None
        ),
        is_subpeak=coordinate.is_subpeak,
    )


def _readings(
    paper,
    per_table: Mapping[str, AnalysisCollection],
    inputs: ParseInputs,
    omitted: Sequence[Omitted] = (),
):
    """One TableReading per table of the paper whose reading is known.

    A hand exclusion wins, then a triage rejection, then what the analyses stage
    recorded. An article analysed before readings were recorded has them only
    for the tables it made a collection for; the rest are not recorded. A table
    some of whose analyses were left out names them in `reason`.
    """
    left_out: Dict[str, List[str]] = {}
    for o in omitted:
        if o.table_id and not o.kept:
            left_out.setdefault(o.table_id, []).append(f"omitted {o.name!r}: {o.reason}")
    out = []
    for table in paper.tables or []:
        tid = table.table_id
        if tid in inputs.excluded:
            out.append(
                pp.TableReading(
                    table_id=tid,
                    reading="excluded_by_hand",
                    reason=_exclusion_reason(inputs.excluded[tid]),
                )
            )
        elif table.triage is not None and not table.triage.passes:
            out.append(
                pp.TableReading(
                    table_id=tid, reading="rejected_by_triage", reason=table.triage.route
                )
            )
        elif inputs.readings is not None and tid in inputs.readings:
            out.append(pp.TableReading(table_id=tid, reading=inputs.readings[tid]))
        elif tid in per_table:
            out.append(pp.TableReading(table_id=tid, reading=table_reading(per_table[tid])))
        else:
            continue
        if tid in left_out and out[-1].reason is None:
            out[-1].reason = "; ".join(left_out[tid])
    return out


def _text_sweep(inputs: ParseInputs):
    if inputs.passages_kept is None:
        return None
    from ingestion_workflow.pipeline.stages.passages import MAX_PASSAGES

    read = len((inputs.prose or {}).get("passages") or [])
    return pp.TextSweep(
        passages_found=inputs.passages_kept,
        passages_read=read,
        # The passages stage keeps the first MAX_PASSAGES and does not record how
        # many it found, so a full list is the only sign the cap cut it.
        truncated=inputs.passages_kept >= MAX_PASSAGES or read < inputs.passages_kept,
        points_dropped_as_table_restatements=inputs.restated,
    )


# -- writing -----------------------------------------------------------------------------


def write(
    root: Path,
    bundle: ArticleExtractionBundle,
    per_table: Mapping[str, AnalysisCollection],
    inputs: ParseInputs,
    *,
    overwrite: bool = True,
    splits: Optional[Dict[int, dict]] = None,
) -> Dict[str, Any]:
    """Write both files under `root/parse/`; what was written, for the stage summary.

    Raises when there is no text to parse or the two files disagree, so the sync
    stage records a failure and tries again rather than calling the paper parsed.
    """
    from pyarty import write_bundle

    out = root / PARSE_DIR
    if not overwrite and (out / "coordinate_parse.json").exists():
        return {}
    paper, text, grids = parsed_paper(root, bundle, inputs)
    if paper is None:
        raise LookupError(f"no text under processed/{bundle.article_data.source.value}")
    parse, omitted = coordinate_parse(paper, text, grids, per_table, inputs, splits)
    files = layouts.PaperParse(parsed_paper=paper, coordinate_parse=parse)
    problems = layouts.check_paper(files, root)
    if problems:
        raise ValueError("; ".join(problems))
    write_bundle(files, out, overwrite=True)
    return {
        "parse_id": parse.parse_id,
        "analyses": len(parse.analyses),
        **({"parse_omitted": left} if (left := [str(o) for o in omitted if not o.kept]) else {}),
        **({"parse_notes": notes} if (notes := [str(o) for o in omitted if o.kept]) else {}),
        **(
            {"held_for_review": held}
            if (held := [o.role_conflict for o in omitted if o.role_conflict])
            else {}
        ),
    }


__all__ = [
    "PAPER_PARSE_VERSION",
    "MissingRole",
    "Omitted",
    "ParseInputs",
    "coordinate_parse",
    "parsed_paper",
    "write",
]

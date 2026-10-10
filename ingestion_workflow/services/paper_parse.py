"""Write the parsed paper and the coordinate parse: study_schema's paper-parse contract.

Two files per paper, beside `stage1/analyses.json` until pondie reads them:

    <study>/parse/parsed_paper.json      ParsedPaper: the text every offset points into,
                                         its tables row by row, the bibliography
    <study>/parse/coordinate_parse.json  CoordinateParse: every analysis, keyed by the
                                         cells it was read from

Both are built as study_schema's generated models, so a field the contract does not
name cannot be written. Nothing here decides anything new: the text is extract's,
the meta-analysis verdict triage's, the readings the analyses stage's, the split the
analyses stage's. What this adds is where each analysis sits in its table -- the
model's points carry no row -- because the key is made from the cells.
"""

from __future__ import annotations

import hashlib
import json
import re
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from study_schema import keys, layouts
from study_schema.models import paper_parse as pp

from ingestion_workflow.extractors.utils import normalize_minus
from ingestion_workflow.models import Analysis, AnalysisCollection, ArticleExtractionBundle
from ingestion_workflow.services.coordinate_flags import PLACEHOLDER_NAME
from ingestion_workflow.services.coordinate_space import sectionize
from ingestion_workflow.services.create_analyses import NEGATIVE_SUFFIX, table_reading
from ingestion_workflow.services.logging import get_logger
from ingestion_workflow.services.naming import sanitize_table_id

logger = get_logger(__name__)

#: Bump when the same inputs would give different files.
PAPER_PARSE_VERSION = 2
PRODUCER = "ns-pond-ingestion-workflow"
PARSE_DIR = "parse"

#: The analyses stage's statistic letters, one to one onto study_schema's
#: StatisticKind (its description lists them; study_schema has no such map to
#: import). Anything else is `other`.
STATISTIC_KINDS = {
    "T": "t",
    "Z": "z",
    "F": "f",
    "D": "d",
    "G": "g",
    "R": "r",
    "B": "beta",
    "P": "p",
}

#: The prose model's roles, as CoordinateParse.role describes the mapping.
PROSE_ROLES = {
    "result": ("result", None),
    "roi": ("anchor", "roi"),
    "seed": ("anchor", "seed"),
    "target": ("anchor", "stimulation_target"),
    "prior_study": ("reference", None),
    "figure": ("display", None),
}

_SPACES = {"MNI": "MNI", "TAL": "TAL", "OTHER": "other"}
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


def statistic_kind(letter: Optional[str]) -> str:
    """study_schema's StatisticKind for one of the analyses stage's letters."""
    return STATISTIC_KINDS.get(str(letter or "").strip().upper(), "other")


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

    grids: Dict[str, _Table] = {}
    tables = []
    for index, table in enumerate(content.tables):
        grid = _grid(table.raw_content_path)
        grids[table.table_id] = grid
        tables.append(
            _parsed_table(
                root,
                index,
                table,
                grid,
                verdicts.get(table.table_id),
                inputs.excluded.get(table.table_id),
                _inlined(grid, text),
            )
        )

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
        coordinate_space_hint=_SPACES.get(str(space)) if space else None,
        triage=triage,
    )


#: Fewest letters and digits a row needs before its place in the text is trusted.
_ROW_CHARS = 8
#: Furthest one inlined row may sit from the one before it.
_ROW_GAP = 2000


def _inlined(grid: _Table, text: _Text) -> Dict[Any, Tuple[int, int]]:
    """Where the table's body rows are inlined in the text, row by row, and the table.

    A source that inlines its tables (pubget, elsevier) prints each row on a line of
    its own, so a row is placed only where it starts a line, in order, near the row
    before it. The table's span runs from its first placed row to its last, and is
    given only when most of its rows were placed.
    """
    found: Dict[Any, Tuple[int, int]] = {}
    at = 0
    for i, cells in enumerate(grid.rows):
        line = " ".join(cell for cell, _ in cells)
        if len(_squash(line)[0]) < _ROW_CHARS:
            continue
        start = at
        while True:
            span = text.find(line, start)
            if span is None or (found and span[0] - at > _ROW_GAP):
                span = None
                break
            line_start = text.text.rfind("\n", 0, span[0]) + 1
            if not _ALNUM.search(text.text, line_start, span[0]):
                break
            start = span[0] + 1
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
):
    """The CoordinateParse, and the analyses that could not be written into it.

    An analysis whose cells or characters cannot be found has no key, and one whose
    key another analysis already holds would overwrite it, so each is left out with
    its reason rather than given a made-up key. The reasons of a table's analyses are
    also the `reason` of its TableReading; the contract has no slot for those of the
    text (see `Omitted`).
    """
    text = text if isinstance(text, _Text) else _Text(text)
    omitted: List[Omitted] = []
    analyses: List[pp.ParsedAnalysis] = []
    seen: Dict[str, str] = {}
    for table_id, collection in per_table.items():
        table_analyses = [
            a
            for a in collection.analyses
            if not (a.name.strip().upper() == PLACEHOLDER_NAME and not a.coordinates)
        ]
        prose = table_id == "prose" or any(
            (a.metadata or {}).get("source") == "prose" for a in table_analyses
        )
        if prose:
            # One at a time, so each sees the keys of those before it.
            built = (
                _prose_analysis(a, collection, text, inputs, omitted, seen) for a in table_analyses
            )
        else:
            built = _table_analyses(
                table_id, table_analyses, collection, grids.get(table_id), omitted
            )
        for analysis in built:
            if analysis is None:
                continue
            if analysis.key in seen:
                omitted.append(
                    Omitted(
                        analysis.name,
                        analysis.table_id,
                        f"the same cells as {analysis.key} ({seen[analysis.key]!r})",
                    )
                )
                continue
            seen[analysis.key] = analysis.name
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
        text_sweep=_text_sweep(inputs),
    )
    return parse, omitted


@dataclass
class Omitted:
    """A stage1 analysis the parse could not hold, and why.

    study_schema's CoordinateParse has no slot for these yet (an `omitted[]` of
    name, table and reason would be one); until it does, a table's are written into
    its TableReading's `reason`, and all of them into the sync summary.
    """

    name: str
    table_id: Optional[str]
    reason: str

    def __str__(self) -> str:
        return f"{self.table_id or 'text'}: {self.name!r}: {self.reason}"


def _table_analyses(
    table_id: str,
    analyses: Sequence[Analysis],
    collection,
    grid: Optional[_Table],
    omitted: List[Omitted],
) -> List[Optional[pp.ParsedAnalysis]]:
    grid = grid or _Table([], [])
    placed = _place_all(analyses, grid)
    # A column group is a block of coordinate columns: rank the x columns the
    # table's points were found in, so a table of one block is all 0.
    columns = sorted({col for points in placed for found in points if found for col in [found[1]]})
    group = {col: i for i, col in enumerate(columns)}

    out: List[Tuple[Optional[pp.ParsedAnalysis], bool]] = []
    for analysis, found in zip(analyses, placed):
        cells = {(row, group[col]) for row, col in filter(None, found)}
        if not analysis.coordinates:
            row = _row_naming(grid, analysis.name)
            if row is not None:
                cells.add((row, 0))
        if not cells:
            omitted.append(
                Omitted(
                    analysis.name,
                    table_id,
                    "no row of the table prints its points"
                    if analysis.coordinates
                    else "no row of the table names it",
                )
            )
            out.append((None, False))
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
                origin="table",
                table_id=table_id,
                key=keys.table_key(table_id, cells),
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


def _prose_analysis(
    analysis: Analysis,
    collection,
    text: _Text,
    inputs: ParseInputs,
    omitted: List[Omitted],
    seen: Optional[Mapping[str, str]] = None,
) -> Optional[pp.ParsedAnalysis]:
    """A text analysis, keyed by where its points are printed.

    Several analyses can come from one passage, so the passage alone would give
    them one key; the characters of each point are what tell them apart. When
    another analysis already holds those characters (one peak reported for two
    contrasts) the place its own name is printed is added; failing that, it is
    omitted as the same points. The passage is the fallback for an analysis none
    of whose points can be found.
    """
    passages = (inputs.prose or {}).get("passages") or []
    windows: List[Tuple[int, int]] = []
    for index in (analysis.metadata or {}).get("passages") or []:
        passage = passages[index].get("text") if 0 <= index < len(passages) else None
        if passage:
            windows += _locate(passage, text)
    point_spans = [_find_point(c, text.text, windows) for c in analysis.coordinates]
    spans = sorted({s for s in point_spans if s}) or sorted(set(windows))
    if not spans:
        omitted.append(
            Omitted(
                analysis.name,
                None,
                "neither its passages nor its points are in the parsed paper's text",
            )
        )
        return None
    key = keys.span_key("text", spans)
    if key in (seen or {}):
        named = next(
            (s for w in windows for s in [text.find(analysis.name, w[0])] if s and s[1] <= w[1]),
            None,
        )
        if named is None or named in spans:
            omitted.append(
                Omitted(analysis.name, None, f"the same points as {key} ({seen[key]!r})")
            )
            return None
        spans = sorted(spans + [named])
        key = keys.span_key("text", spans)
    role, anchor = PROSE_ROLES.get(
        (analysis.metadata or {}).get("role") or "result", ("result", None)
    )
    points = [
        _point(c, collection, found=None, span=s)
        for c, s in zip(analysis.coordinates, point_spans)
    ]
    return _analysis(
        analysis,
        collection,
        points,
        origin="text",
        table_id=None,
        key=key,
        text_spans=[pp.TextSpan(start_char=a, end_char=b) for a, b in spans],
        role=role,
        anchor_kind=anchor,
        from_prior_study=True if role == "reference" else None,
    )[0]


_MINUS = "-−–‐"


def _number(value: float) -> str:
    digits = re.escape(f"{abs(value):g}") + (r"(?:\.0+)?" if float(value).is_integer() else r"\d*")
    if value < 0:
        return rf"[{_MINUS}]\s?{digits}"
    return rf"(?<![{_MINUS}\d.])\+?{digits}"


#: Furthest a point found outside its passages may sit from one of them.
_NEAR = 3000


def _find_point(coordinate, text: str, windows) -> Optional[Tuple[int, int]]:
    """The characters printing a point's x, y and z.

    Within one of its passages first. A passage cut differently from the text may
    not hold it, so then anywhere in the text: where it is printed once, or the
    copy nearest one of its passages.
    """
    sep = r"[^\d\n]{1,12}?"
    pattern = re.compile(
        sep.join(_number(v) for v in (coordinate.x, coordinate.y, coordinate.z)) + r"(?![\d.])"
    )
    for start, end in windows:
        match = pattern.search(text, start, end)
        if match:
            return match.start(), match.end()
    found = [m.span() for m in pattern.finditer(text)]
    if len(found) == 1:
        return found[0]
    if found and windows:
        near = min(
            ((min(abs(s - a), abs(s - b)), (s, e)) for s, e in found for a, b in windows),
        )
        if near[0] <= _NEAR:
            return near[1]
    return None


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
    *,
    origin,
    table_id,
    key,
    cells=None,
    text_spans=None,
    role="result",
    anchor_kind=None,
    from_prior_study=None,
):
    name = analysis.name
    kinds = {v.kind for p in points for v in p.values or [] if v.kind != "p"}
    built = pp.ParsedAnalysis(
        key=key,
        cells=cells,
        text_spans=text_spans,
        origin=origin,
        table_id=table_id,
        name=name,
        name_is_printed=False if name.strip().upper() == PLACEHOLDER_NAME else None,
        description=analysis.description or None,
        coordinate_space=_SPACES.get(collection.coordinate_space.value, "unknown"),
        role=role,
        anchor_kind=anchor_kind,
        from_prior_study=from_prior_study,
        role_source="proposal",
        statistic=kinds.pop() if len(kinds) == 1 else None,
        points=points,
    )
    return built, name.endswith(NEGATIVE_SUFFIX)


def _declare_splits(analyses: List[Tuple[Optional[pp.ParsedAnalysis], bool]]) -> None:
    """Declare the analyses stage's sign split as `split{}` on both halves.

    The stage names the negative half `<name> (negative)` and emits it right
    after the positive one; that adjacency and the name are the only record of
    the split, so this is where it becomes a field, and only then is the suffix
    dropped from the name. A `(negative)` name with no such primary keeps it.
    """
    for i, (half, negative) in enumerate(analyses):
        if half is None or not negative or i == 0:
            continue
        primary, primary_negative = analyses[i - 1]
        name = half.name[: -len(NEGATIVE_SUFFIX)]
        if primary is None or primary_negative or primary.name != name:
            continue
        rule = "sign_of_directional_statistic"
        primary.split = pp.SignSplit(
            group=primary.key, direction="positive", rule=rule, primary=True
        )
        half.split = pp.SignSplit(
            group=primary.key, direction="negative", rule=rule, primary=False
        )
        half.name = name


def _point(coordinate, collection, found: Optional[Tuple[int, int]], span=None):
    space = coordinate.space.value if coordinate.space else None
    values = None
    if isinstance(coordinate.statistic_value, (int, float)):
        values = [
            pp.PointValue(
                kind=statistic_kind(coordinate.statistic_type),
                value=float(coordinate.statistic_value),
            )
        ]
    measure = (
        str(coordinate.cluster_measure or "").strip().lower().replace("³", "3").replace("^", "")
    )
    return pp.ParsedPoint(
        coordinates=[coordinate.x, coordinate.y, coordinate.z],
        space=_SPACES.get(space) if space and space != collection.coordinate_space.value else None,
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
        if o.table_id:
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
    parse, omitted = coordinate_parse(paper, text, grids, per_table, inputs)
    files = layouts.PaperParse(parsed_paper=paper, coordinate_parse=parse)
    problems = layouts.check_paper(files, root)
    if problems:
        raise ValueError("; ".join(problems))
    write_bundle(files, out, overwrite=True)
    return {
        "parse_id": parse.parse_id,
        "analyses": len(parse.analyses),
        **({"parse_omitted": [str(o) for o in omitted]} if omitted else {}),
    }


__all__ = [
    "PAPER_PARSE_VERSION",
    "Omitted",
    "ParseInputs",
    "coordinate_parse",
    "parsed_paper",
    "statistic_kind",
    "write",
]

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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from study_schema import keys
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
PAPER_PARSE_VERSION = 1
PRODUCER = "ns-pond-ingestion-workflow"
PARSE_DIR = "parse"

#: The analyses stage's statistic letters, one to one onto study_schema's
#: StatisticKind (its description lists them). Anything else is `other`.
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
    text = raw.decode("utf-8", errors="replace")

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
        text_length=len(text),
        bibliography=_bibliography(meta, title, types),
        is_meta_analysis=bool(is_meta),
        is_meta_analysis_basis=_meta_basis(title, types) if is_meta else None,
        sections=_sections(text),
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


def _parsed_table(root: Path, index: int, table, grid: _Table, verdict, excluded):
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
            pp.TableRow(row=i, cells=[text for text, _ in cells])
            for i, cells in enumerate(grid.rows)
        ],
        coordinate_space_hint=_SPACES.get(str(space)) if space else None,
        triage=triage,
    )


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
    text: str,
    grids: Mapping[str, _Table],
    per_table: Mapping[str, AnalysisCollection],
    inputs: ParseInputs,
):
    """The CoordinateParse, and what could not be written into it.

    An analysis whose cells or passages cannot be found has no key, so it is left
    out and named in the returned problems rather than given a made-up one.
    """
    problems: List[str] = []
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
            built = [
                _prose_analysis(a, collection, text, inputs, problems)[0] for a in table_analyses
            ]
        else:
            built = _table_analyses(
                table_id, table_analyses, collection, grids.get(table_id), problems
            )
        for analysis in built:
            if analysis is None:
                continue
            if analysis.key in seen:
                problems.append(
                    f"{analysis.key}: {analysis.name!r} covers the same cells as "
                    f"{seen[analysis.key]!r}"
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
        tables=_readings(paper, per_table, inputs) or None,
        text_sweep=_text_sweep(inputs),
    )
    return parse, problems


def _table_analyses(
    table_id: str,
    analyses: Sequence[Analysis],
    collection,
    grid: Optional[_Table],
    problems: List[str],
) -> List[Optional[pp.ParsedAnalysis]]:
    grid = grid or _Table([], [])
    placed = [[_place(c, grid) for c in a.coordinates] for a in analyses]
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
            problems.append(f"{table_id}: no row of the table holds {analysis.name!r}")
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


def _place(coordinate, grid: _Table) -> Optional[Tuple[int, int]]:
    """(body row, x column) of the first row printing this point's x, y, z."""
    from nspond_tables.grid import as_number
    from nspond_tables.read import triples_in

    xyz = (coordinate.x, coordinate.y, coordinate.z)

    def same(trio) -> bool:
        return all(v is not None and abs(a - v) <= _SAME_NUMBER for a, v in zip(xyz, trio))

    for index, cells in enumerate(grid.rows):
        numbers = [(as_number(text), col) for text, col in cells]
        for i in range(len(numbers) - 2):
            if same([n for n, _ in numbers[i : i + 3]]):
                return index, numbers[i][1]
        for text, col in cells:
            if any(same(trio) for trio in triples_in(text)):
                return index, col
    return None


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
    analysis: Analysis, collection, text: str, inputs: ParseInputs, problems: List[str]
) -> Tuple[Optional[pp.ParsedAnalysis], bool]:
    """A text analysis, keyed by where its points are printed.

    Several analyses can come from one passage, so the passage alone would give
    them one key; the characters of each point are what tell them apart. The
    passage is the fallback for an analysis none of whose points can be found.
    """
    passages = (inputs.prose or {}).get("passages") or []
    windows, located = [], []
    for index in (analysis.metadata or {}).get("passages") or []:
        passage = passages[index].get("text") if 0 <= index < len(passages) else None
        if not passage:
            continue
        found = _locate(passage, text)
        if found:
            located.append(found)
            windows.append(found)
        elif text.find(passage[:_ANCHOR]) >= 0:
            at = text.find(passage[:_ANCHOR])
            windows.append((at, min(len(text), at + 2 * len(passage))))
    point_spans = [_find_point(c, text, windows) for c in analysis.coordinates]
    spans = sorted({s for s in point_spans if s}) or located
    if not spans:
        problems.append(
            f"text: the passages of {analysis.name!r} are not in the parsed paper's text"
        )
        return None, False
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
        key=keys.span_key("text", spans),
        text_spans=[pp.TextSpan(start_char=a, end_char=b) for a, b in spans],
        role=role,
        anchor_kind=anchor,
        from_prior_study=True if role == "reference" else None,
    )


_MINUS = "-\u2212\u2013\u2010"


def _number(value: float) -> str:
    digits = re.escape(f"{abs(value):g}") + (r"(?:\.0+)?" if float(value).is_integer() else r"\d*")
    if value < 0:
        return rf"[{_MINUS}]\s?{digits}"
    return rf"(?<![{_MINUS}\d.])\+?{digits}"


def _find_point(coordinate, text: str, windows) -> Optional[Tuple[int, int]]:
    """The characters printing a point's x, y and z, within one of its passages."""
    sep = r"[^\d\n]{1,12}?"
    pattern = re.compile(
        sep.join(_number(v) for v in (coordinate.x, coordinate.y, coordinate.z)) + r"(?![\d.])"
    )
    for start, end in windows:
        match = pattern.search(text, start, end)
        if match:
            return match.start(), match.end()
    return None


#: How much of a passage's head and tail must match the text to place it there.
_ANCHOR = 40


def _locate(passage: str, text: str) -> Optional[Tuple[int, int]]:
    """Where a prose passage sits in the parsed paper's text, as (start, end).

    The passages stage cuts passages from its own copy of the text, joined
    sentence by sentence, so a passage is rarely a verbatim slice of text.txt:
    its whitespace differs and an inlined table or heading may sit inside it.
    Exact first, then with whitespace collapsed, then by its head and tail,
    accepted only when what lies between is no more than twice its length.
    """
    start = text.find(passage)
    if start >= 0:
        return start, start + len(passage)
    squeezed, where = [], []
    for i, ch in enumerate(text):
        if ch.isspace():
            if squeezed and squeezed[-1] == " ":
                continue
            ch = " "
        squeezed.append(ch)
        where.append(i)
    wanted = re.sub(r"\s+", " ", passage).strip()
    at = "".join(squeezed).find(wanted)
    if at >= 0 and wanted:
        return where[at], where[at + len(wanted) - 1] + 1
    head, tail = passage[:_ANCHOR], passage[-_ANCHOR:]
    start = text.find(head)
    end = text.find(tail, start + 1) if start >= 0 else -1
    if start >= 0 and end >= 0 and end + len(tail) - start <= 2 * len(passage):
        return start, end + len(tail)
    return None


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
    negative = name.endswith(NEGATIVE_SUFFIX)
    if negative:
        name = name[: -len(NEGATIVE_SUFFIX)]
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
    return built, negative


def _declare_splits(analyses: List[Tuple[Optional[pp.ParsedAnalysis], bool]]) -> None:
    """Declare the analyses stage's sign split as `split{}` on both halves.

    The stage names the negative half `<name> (negative)` and emits it right
    after the positive one; that adjacency and the name are the only record of
    the split, so this is where it becomes a field.
    """
    for i, (half, negative) in enumerate(analyses):
        if half is None or not negative:
            continue
        primary = next(
            (
                a
                for a, neg in reversed(analyses[:i])
                if a is not None and not neg and a.name == half.name
            ),
            None,
        )
        if primary is None:
            continue
        rule = "sign_of_directional_statistic"
        primary.split = pp.SignSplit(
            group=primary.key, direction="positive", rule=rule, primary=True
        )
        half.split = pp.SignSplit(
            group=primary.key, direction="negative", rule=rule, primary=False
        )


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


def _readings(paper, per_table: Mapping[str, AnalysisCollection], inputs: ParseInputs):
    """One TableReading per table of the paper whose reading is known.

    A hand exclusion wins, then a triage rejection, then what the analyses stage
    recorded. An article analysed before readings were recorded has them only
    for the tables it made a collection for; the rest are not recorded.
    """
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
    """Write both files under `root/parse/`; what was written, for the stage summary."""
    out = root / PARSE_DIR
    if not overwrite and (out / "coordinate_parse.json").exists():
        return {}
    paper, text, grids = parsed_paper(root, bundle, inputs)
    if paper is None:
        return {"parse": "no text"}
    parse, problems = coordinate_parse(paper, text, grids, per_table, inputs)
    out.mkdir(parents=True, exist_ok=True)
    for name, model in (("parsed_paper.json", paper), ("coordinate_parse.json", parse)):
        body = json.dumps(
            model.model_dump(mode="json", exclude_none=True), indent=1, ensure_ascii=False
        )
        (out / name).write_text(body, encoding="utf-8")
    return {
        "parse_id": parse.parse_id,
        "analyses": len(parse.analyses),
        **({"parse_problems": problems} if problems else {}),
    }


__all__ = [
    "PAPER_PARSE_VERSION",
    "ParseInputs",
    "coordinate_parse",
    "parsed_paper",
    "statistic_kind",
    "write",
]

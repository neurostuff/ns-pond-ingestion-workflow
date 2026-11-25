"""Service responsible for creating analyses from extracted tables."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import replace
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from ingestion_workflow.clients import CoordinateParsingClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.analysis import UNKNOWN_SPACES
from ingestion_workflow.models.statistics import inverted
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleExtractionBundle,
    Coordinate,
    CoordinatePoint,
    CoordinateSpace,
    ExtractedTable,
    ParseAnalysesOutput,
)
from ingestion_workflow.extractors.utils import normalize_minus, table_figures
from ingestion_workflow.prompts.coordinate_parsing import ANALYSIS_BOUNDARY_RULES
from ingestion_workflow.services.coordinate_flags import (
    PLACEHOLDER_NAME,
    inverse_name,
    is_placeholder,
    subpeak_flags,
)
from ingestion_workflow.services import column_shift
from ingestion_workflow.services.naming import sanitize_table_id
from ingestion_workflow.services.nuextract_payload import UnreadableAnswer
from ingestion_workflow.utils.progress import emit_progress

logger = logging.getLogger(__name__)

_SCHEMA_TEMPLATE = """{
  "analyses": [
    {
      "name": "<analysis name exactly as in table>",
      "coordinates": [
        {
          "x": <float>,
          "y": <float>,
          "z": <float>,
          "space"?: "MNI" | "TAL" | null,
          "statistic_value"?: <float> | null,
          "statistic_type"?: "T" | "Z" | "D" | "G" | "F" | "R" | "B" | "P" | null,
          "cluster_size"?: <int> | null,
          "cluster_measure"?: "voxels" | "mm^3" | null,
          "is_subpeak"?: true | false
        }, ...
      ],
      "contrasts"?: [
        {
          "name": ...,
          "conditions": [...],
          "weights": [...],
          "description"?: ...
        },
        ...
      ]
    },
    ...
  ]
}"""


#: Tag boundaries that carry table structure. Everything else is decoration.
_CELL_END = re.compile(r"(?i)</t[dh]>")
_ROW_END = re.compile(r"(?i)</tr>|</row>")
_ANY_TAG = re.compile(r"<[^>]+>")
_RUNS = re.compile(r"[ \t]{2,}")


#: A numeric footnote mark straight after a number is never more digits of it.
#: Stripped like any inline tag, `46<sup>1</sup>` read as `461`: a seed table
#: citing its sources by footnote put every z out of the brain (PMC3985019,
#: Table 2). Such a mark is dropped. Every other superscript is left to the
#: serialiser as before -- a letter mark, a unit's power (`mm<sup>3</sup>`), a
#: minus set as a superscript -- so the extractor's input changes only where a
#: number was wrong; the fine-tune never saw any other rendering of them.
#: `10<sup>5</sup>` is a power of ten, not a footnote, and is not touched.
_MARK = r"\s*\d{1,3}(?:\s*[,–-]\s*\d{1,3})*\s*"
_LINK = r"(?:xref|(?:ce:)?cross-ref)"
_FOOTNOTE_AFTER_NUMBER = (
    # `<sup>1</sup>`, bare or inside a footnote link
    re.compile(rf"(?<=\d)(?<!10)\s*(<{_LINK}\b[^>]*>\s*)?<((?:[\w-]+:)?sup)\b[^>]*>{_MARK}"
               rf"</\2\s*>(?(1)\s*</{_LINK}\s*>)", re.I),
    # a footnote link holding the number itself, `<xref ref-type="table-fn">1</xref>`
    re.compile(rf"(?<=\d)(?<!10)\s*<({_LINK})\b[^>]*>{_MARK}</\1\s*>", re.I),
)


def _footnote_marks(markup: str) -> str:
    """`markup` without the numeric footnote marks that follow a number."""
    for rule in _FOOTNOTE_AFTER_NUMBER:
        markup = rule.sub("", markup)
    return markup


#: A coordinate printed with its direction instead of its sign: `32R | 92P | 4I`
#: (Talairach, left/right, anterior/posterior, superior/inferior). The extractor
#: kept the digits and dropped the letter, so every left, posterior and inferior
#: focus was stored on the other side (PMC-less PDF of doi:10.1111/j.1530-0277.
#: 2001.tb02204.x, Table 2). Only three cells in a row, in x, y, z order, are
#: read so: a lone `6L` may be anything.
_DIRECTED = (
    re.compile(r"^(\d+(?:\.\d+)?)\s?([RL])$"),
    re.compile(r"^(\d+(?:\.\d+)?)\s?([AP])$"),
    re.compile(r"^(\d+(?:\.\d+)?)\s?([SI])$"),
)
_NEGATIVE_DIRECTIONS = {"L", "P", "I"}


def _signed_by_direction(line: str) -> str:
    """One serialised row with each `x R | y P | z I` triple written as signed numbers."""
    if " | " not in line:
        return line
    cells = line.split(" | ")
    i = 0
    while i + 2 < len(cells):
        found = [rule.match(cells[i + k].strip()) for k, rule in enumerate(_DIRECTED)]
        if all(found):
            for k, m in enumerate(found):
                sign = "-" if m.group(2) in _NEGATIVE_DIRECTIONS and float(m.group(1)) else ""
                cells[i + k] = sign + m.group(1)
            i += 3
        else:
            i += 1
    return " | ".join(cells)


#: A data row whose first cell spans columns: `<2:Caudate (L/R) | 12 | 6 | 16 | 8.34`.
_LEADING_SPAN = re.compile(r"^<(\d+):(.*)$", re.S)
_BARE_NUMBER = re.compile(r"^[-+−]?\d+(?:\.\d+)?$")
#: A region over its BA or cluster-size column spans two or three. Wider, the span is a block's
#: banner set beside another block's rows (vk5rtp22s3da), and v23 read those worse spelt out.
_WIDEST_SPAN = 3


def _leading_span_rows(lines: Sequence[str]) -> List[str]:
    """The table's lines, each data row with a leading span spelt out where the table itself
    keeps those columns apart: another data row, with no span of its own, prints a value in one
    of the columns the span covers (PMC5622290's `Cluster #1b | 9,727` beside
    `<2:Caudate (L/R) | 12`). Where every row spans them, the table is consistent as written,
    and spelt out v23 read it worse (3odyz2h6hh7b, every row `<2:` under an unmarked `<2:`
    header)."""
    kept_apart = set()
    for line in lines:
        if line.startswith("#") or " | " not in line:
            continue
        cells = line.split(" | ")
        if _LEADING_SPAN.match(cells[0]) or not any(_BARE_NUMBER.match(c.strip()) for c in cells[1:]):
            continue
        column = 0
        for cell in cells:
            span = _LEADING_SPAN.match(cell)
            if column and cell.strip() not in ("", "~"):
                kept_apart.add(column)
            column += int(span.group(1)) if span else 1
    head = _header_first_span(lines)
    return [_leading_span_cells(line, head, kept_apart) for line in lines]


def _leading_span_cells(line: str, header_span: int = 1, kept_apart=None) -> str:
    """One serialised row with a column-spanning first cell written as that cell and empty cells.

    A row-group banner is a line `<N:text`; a data row whose first cell spans columns begins the
    same way. nu-v23 read such a row's cell as one column, so every value after it moved one
    column left: x became the cluster size, y the x (PMC5622290 Table 2, where the region spans
    the cluster-size column of each sub-peak). As plain cells the row lines up with its header.

    Only a data row is rewritten: one whose span holds text and whose later cells print a bare
    number. Left as they are, because v23 read them worse spelt out: banners, side by side or
    with empty cells after them; a header row the source did not mark (`<3:Coordinate |
    Accuracy | p`); an empty spanned cell (`<8:^6:`); a row with a second span, whose cells
    don't follow the header's boundaries (`<2:Mid calcarine | <2:1660` under `#Region |
    #<3:Cluster extent`); and a row whose span is no wider than the header's own first cell
    (`header_span`), which already lines up with `#<2:Region`."""
    if line.startswith("#") or " | " not in line:
        return line
    cells = line.split(" | ")
    span = _LEADING_SPAN.match(cells[0])
    if (not span or int(span.group(1)) <= header_span or int(span.group(1)) > _WIDEST_SPAN
            or not re.sub(r"^\^\d+:", "", span.group(2)).strip()
            or not any(_BARE_NUMBER.match(c.strip()) for c in cells[1:])
            or any(_LEADING_SPAN.match(c) for c in cells[1:])
            or (kept_apart is not None
                and not any(c in kept_apart for c in range(1, int(span.group(1)))))):
        return line
    return " | ".join([span.group(2)] + [""] * (int(span.group(1)) - 1) + cells[1:])


def _header_first_span(lines: Sequence[str]) -> int:
    """The widest first cell of the table's header rows (`#<2:Region` is 2), else 1."""
    widest = 1
    for line in lines:
        if not line.startswith("#"):
            continue
        span = re.match(r"^#(?:\^\d+:)?<(\d+):", line)
        widest = max(widest, int(span.group(1)) if span else 1)
    return widest


def _text_of(markup: str) -> str:
    """The words in a table, with its grid kept and its tags dropped.

    Only used when the serialiser could not read the table. The tags are what
    made the raw form large -- 14.7x on average -- and a model trained on
    ` | `-separated cells gains nothing from `<td class="...">`. Cell and row
    boundaries survive as ` | ` and newlines, because which column a number
    sits in is the whole answer.
    """
    text = _ROW_END.sub("\n", _CELL_END.sub(" | ", markup))
    text = _ANY_TAG.sub("", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    lines = [_RUNS.sub(" ", line).strip(" |\t") for line in text.split("\n")]
    return "\n".join(line for line in lines if line.strip())


def split_by_sign(name, coordinates, taken=()):
    """Split an analysis that reports both directions under one name.

    A positive and a negative statistic are different directions, and pooling
    them pools an increase with a decrease -- 2,120 table analyses in the
    53,273 ns-pond papers with a stage1 hold both.

    The direction normally lives in the contrast name, which is why a point's
    side reads the statistic rather than the name. Where one contrast reports
    both, the sign is the only thing that separates them. The rule is study_schema's
    (`SplitRule.sign_of_directional_statistic`): points with a negative value,
    of any kind, form the inverse half, every other point, unsigned ones
    included, the original half.

    The original half is the analysis as named. The inverse half is the reversed
    contrast: it is named by `inverse_name`, given `taken`, the table's printed
    names, so it is not named like another of them; and its signed values are negated
    (`statistics.inverted`), so a t of -4.1 for "A > B" is a t of 4.1 for "B > A".
    `split` is what says so.

    Yields `(name, coordinates, split)` in table order, the original half
    first. `split` is `{"half": "original"}` on the original and
    `{"half": "inverse"}` on the inverse; `_build_collection` adds the original's
    index among the analyses, so the halves pair by it and not by name.
    `split` is None for an analysis whose statistics are all
    one side or which has none, which is returned unchanged.
    """
    original = [c for c in coordinates if c.sign != "negative"]
    inverse = [c for c in coordinates if c.sign == "negative"]
    if not (original and inverse):
        return [(name, coordinates, None)]
    flipped = [
        replace(c, statistic_value=inverted(c.statistic_value, c.statistic_type))
        for c in inverse
    ]
    return [
        (name, original, {"half": "original"}),
        (inverse_name(name, taken), flipped, {"half": "inverse"}),
    ]


def _points(parsed: ParseAnalysesOutput):
    """Each parsed point as (x, y, z, statistic, cluster size)."""
    out = []
    for analysis in parsed.analyses:
        for point in analysis.points:
            values = point.values or []
            try:
                statistic = float(values[0].value) if values else None
            except (TypeError, ValueError):
                statistic = None  # a printed "<0.001" is not a number to place
            out.append((*point.coordinates, statistic, point.cluster_size))
    return out


def _coordinates(parsed: ParseAnalysesOutput):
    return sorted(tuple(round(v, 3) for v in p[:3]) for p in _points(parsed))


def _misplaced(parsed: ParseAnalysesOutput, rows) -> list:
    """The `extraction_signs.MISPLACED` signs an answer carries against its table's rows.

    A greedy answer sometimes gives a point the statistic or cluster size printed in
    another point's row: the next row's p, the next cluster's t on subpeaks, the
    first row's value copied down. Asked again under the grammar with the loop
    penalty, all of 4 such tables read every value from its own row (2026-11-23).
    """
    from ingestion_workflow.services import extraction_signs

    signs = extraction_signs.suspect_signs(_points(parsed), rows)
    return [s for s in signs if s in extraction_signs.MISPLACED]


def table_reading(collection: AnalysisCollection) -> str:
    """What reading one table found, in the coordinate parse's vocabulary.

    `TableReadingKind` in study_schema's paper parse: `coordinates` when any
    analysis has points, `contrasts_without_coordinates` when it only names
    contrasts with none, `no_coordinates` when nothing was read from it.
    """
    if any(analysis.coordinates for analysis in collection.analyses):
        return "coordinates"
    if collection.analyses:
        return "contrasts_without_coordinates"
    return "no_coordinates"



def build_document(
    *,
    title: Optional[str],
    abstract: Optional[str],
    caption: Optional[str],
    footer: Optional[str],
    table_text: str,
) -> str:
    """The document the fine-tuned extractor was trained on.

    Field order and labels are load-bearing -- this is reproduced from the
    trainer, not designed here, and a reordering is an input the model has
    not seen. Absent fields are omitted rather than sent empty, because
    that is how the training rows were built.

    The abstract earns its place: a paper often states its normalisation
    space there and nowhere in the table, and it is where the table's
    abbreviations are spelled out. It is also most of the prompt's tokens,
    which is the first thing to measure if cost ever matters.

    There is no `Space:` line. That was an input in earlier versions and
    is a *target* now -- feeding it back would tell the model the answer.

    A module function, not a method, so an offline evaluation can build the
    same document from a bare row without a bundle. Rebuilt by hand in a
    benchmark it drifts, and a benchmark on a document production never
    sends measures nothing.
    """
    parts = [f"Title: {title or ''}"]
    for label, value in (
        ("Abstract", abstract),
        ("Caption", caption),
        ("Footer", footer),
    ):
        if value:
            parts.append(f"{label}: {value}")
    parts.append("")
    parts.append(table_text)
    return "\n".join(parts)


class CreateAnalysesService:
    """Create AnalysisCollection objects from extracted tables."""

    def __init__(
        self,
        settings: Settings,
        *,
        extractor_name: Optional[str] = None,
    ) -> None:
        self.settings = settings
        self.extractor_name = extractor_name
        self.client = CoordinateParsingClient(settings)

    def run(
        self,
        bundle: ArticleExtractionBundle,
        progress_hook: Callable[[int], None] | None = None,
        readings: Optional[Dict[str, str]] = None,
        unread: Optional[Dict[str, str]] = None,
        retried: Optional[Dict[str, str]] = None,
        realigned: Optional[Dict[str, str]] = None,
    ) -> Dict[str, AnalysisCollection]:
        """Create analyses for every table in the bundle.

        `readings`, when given, is filled with what reading each table found
        (`table_reading`), including the tables that yield no collection;
        `unread` with why a table was not read; and `retried` with the tables
        asked again because values sat on the wrong point (`_misplaced`), and
        which answer was kept; `realigned` with the tables whose points were read
        off the headed x, y and z columns and read back (`column_shift.realign`).
        Passed in rather than kept on the service, which is shared across threads.
        """
        readings = {} if readings is None else readings
        unread = {} if unread is None else unread
        retried = {} if retried is None else retried
        realigned = {} if realigned is None else realigned
        if not bundle.article_data.tables:
            return {}

        results: Dict[str, AnalysisCollection] = {}
        article_slug = bundle.article_data.slug
        identifier = bundle.article_data.identifier

        # Which tables are worth a call is triage's decision, and it is made
        # before this runs. Re-testing `contains_coordinates` here applied the
        # old reader-based filter a second time and silently dropped every
        # table triage passed on the residual route -- where the reader found
        # nothing by definition, which is the whole reason that route exists.
        # It cost 1,438 of 1,461 articles in the first corpus run.
        redundant = self._redundant(bundle.article_data.tables, article_slug)
        unreadable: Optional[UnreadableAnswer] = None
        for index, table in enumerate(bundle.article_data.tables):
            sanitized_table_id = sanitize_table_id(table.table_id, index)
            table_key = table.table_id or sanitized_table_id
            if table_key in redundant:
                unread[table_key] = "repeats another table's numbers"
                continue

            try:
                table_text = self._read_table_content(table)
            except FileNotFoundError as exc:
                logger.warning(
                    "Skipping table %s for article %s: %s",
                    table_key,
                    article_slug,
                    exc,
                )
                unread[table_key] = "raw content missing"
                continue
            model_space = None
            if getattr(self.settings, "llm_native_schema", False):
                serialised = self._serialise(table_text, table_key)
                document = self._build_document(bundle, table, serialised)
                try:
                    parsed_output, model_space = self.client.parse_analyses_native(document)
                except UnreadableAnswer as exc:
                    # One table's answer does not lose the article's others.
                    unread[table_key] = f"unreadable answer: {exc}"
                    unreadable = unreadable or exc
                    continue
                # Before the misplaced check, which finds each point's row by its x, y and z.
                parsed_output, moved = column_shift.realign(
                    parsed_output, _footnote_marks(normalize_minus(table_text))
                )
                if moved:
                    realigned[table_key] = moved
                rows = [*serialised.splitlines(), table.caption or "", table.footer or ""]
                misplaced = _misplaced(parsed_output, rows)
                if misplaced:
                    parsed_output, model_space, retried[table_key] = self._retry(
                        document, rows, misplaced, parsed_output, model_space
                    )
            else:
                prompt = self._build_prompt(bundle, table, table_text, table_key)
                parsed_output = self.client.parse_analyses(prompt)
            if not parsed_output.analyses:
                # Expected on the native path: the fine-tune is trained to
                # return nothing for a table that holds no coordinates, and
                # roughly a third of what reaches it does not.
                logger.debug(
                    "no analyses for article %s table %s",
                    article_slug,
                    table_key,
                )

            collection = self._build_collection(
                parsed_output,
                table,
                identifier,
                sanitized_table_id,
                table_key,
                article_slug,
                model_space=model_space,
            )
            # A table the extractor found nothing in is recorded as processed
            # by the stage's artifact, but it does not become a collection: an
            # empty one carries no result and would be counted as a table with
            # analyses by everything downstream.
            readings[table_key] = table_reading(collection)
            if collection.analyses:
                results[table_key] = collection
            emit_progress(progress_hook)

        if unreadable is not None and not readings:
            # Nothing was read: recorded as done, the article would pass for
            # one with no coordinates and never be asked again.
            raise unreadable
        return results

    def _retry(self, document, rows, misplaced, first, first_space):
        """Ask again constrained; keep that answer when it reads the same coordinates
        and puts no value on the wrong point, else the first. Returns the answer kept,
        its space, and what was retried and kept."""
        what = ", ".join(misplaced)
        try:
            again, space = self.client.parse_analyses_native(document, constrained=True)
        except UnreadableAnswer:
            return first, first_space, f"{what}; kept the first answer: the retry was unreadable"
        if _coordinates(again) != _coordinates(first):
            return first, first_space, f"{what}; kept the first answer: the retry read other coordinates"
        if _misplaced(again, rows):
            return first, first_space, f"{what}; kept the first answer: the retry misplaces values too"
        return again, space, f"{what}; kept the constrained retry"

    def _build_collection(
        self,
        parsed_output: ParseAnalysesOutput,
        table: ExtractedTable,
        identifier,
        sanitized_table_id: str,
        table_key: str,
        article_slug: str,
        model_space: Optional[str] = None,
    ) -> AnalysisCollection:
        # A space the extraction actually read wins: it came from the article,
        # not from this table. Unknown is not an answer, so it defers to the
        # model; with neither, it is None. A stated `OTHER` is kept.
        read = table.space if table.space not in UNKNOWN_SPACES else None
        table_space = read or self._coerce_space(model_space, None)
        collection = AnalysisCollection(
            slug=f"{article_slug}::{sanitized_table_id}",
            coordinate_space=table_space,
            identifier=identifier,
        )
        printed = [parsed.name for parsed in parsed_output.analyses if parsed.name]
        for idx, parsed in enumerate(parsed_output.analyses, start=1):
            if is_placeholder(parsed.name, parsed.points):
                continue
            coordinates = self._convert_points(
                parsed.points,
                table_space,
            )
            analysis_name = parsed.name or f"{table_key} analysis {idx}"
            for name, subset, split in split_by_sign(analysis_name, coordinates, printed):
                # Where the original sits among the analyses now; later filtering
                # moves positions, so the halves are paired by this number.
                if split and split["half"] == "original":
                    original_index = len(collection.analyses)
                    split = {**split, "index": original_index}
                elif split:
                    split = {**split, "original_index": original_index}
                collection.add_analysis(Analysis(
                    name=name,
                    description=parsed.description,
                    coordinates=subset,
                    table_id=table_key,
                    table_number=table.table_number,
                    table_caption=table.caption or "",
                    table_footer=table.footer or "",
                    metadata={
                        "table_metadata": dict(table.metadata),
                        "sanitized_table_id": sanitized_table_id,
                        **({"split": split} if split else {}),
                    },
                ))
        return collection

    def _convert_points(
        self,
        points: List[CoordinatePoint],
        default_space: Optional[CoordinateSpace],
    ) -> List[Coordinate]:
        # Two passes: `is_subpeak` is a property of the analysis, not of a row.
        # A blank extent means nothing until the other rows are known to have
        # one, so every row's numbers are read first and the flags derived
        # from the whole set.
        rows = [self._read_point(point, default_space) for point in points]
        subpeaks = subpeak_flags([row["cluster_size"] for row in rows])
        return [
            Coordinate(
                x=row["x"],
                y=row["y"],
                z=row["z"],
                space=row["space"],
                statistic_value=row["statistic_value"],
                statistic_type=row["statistic_type"],
                cluster_size=row["cluster_size"],
                cluster_measure=row["cluster_measure"],
                is_subpeak=subpeak,
            )
            for row, subpeak in zip(rows, subpeaks)
        ]

    def _read_point(
        self,
        point: CoordinatePoint,
        default_space: Optional[CoordinateSpace],
    ) -> Dict[str, object]:
        """Normalise one point's numbers, without deciding any flag."""
        cluster_size = point.cluster_size
        cluster_measure = point.cluster_measure
        statistic_value = None
        statistic_type = None
        if point.values:
            primary_value = point.values[0]
            statistic_type = primary_value.kind
            try:
                statistic_value = (
                    float(primary_value.value) if primary_value.value is not None else None
                )
            except (TypeError, ValueError):
                statistic_value = None
        return {
            "x": point.coordinates[0],
            "y": point.coordinates[1],
            "z": point.coordinates[2],
            "space": self._coerce_space(point.space, default_space),
            "statistic_value": statistic_value,
            "statistic_type": statistic_type,
            "cluster_size": cluster_size,
            "cluster_measure": cluster_measure,
        }

    def _coerce_space(
        self, space_label: Optional[str], fallback: Optional[CoordinateSpace]
    ) -> Optional[CoordinateSpace]:
        return CoordinateSpace.from_label(space_label) or fallback

    def _build_document(
        self,
        bundle: ArticleExtractionBundle,
        table: ExtractedTable,
        table_text: str,
    ) -> str:
        """The document the fine-tuned extractor was trained on."""
        return build_document(
            title=bundle.article_metadata.title,
            abstract=bundle.article_metadata.abstract,
            caption=table.caption,
            footer=table.footer,
            table_text=table_text,
        )

    def _build_prompt(
        self,
        bundle: ArticleExtractionBundle,
        table: ExtractedTable,
        table_text: str,
        table_key: str,
    ) -> str:
        article_title = bundle.article_metadata.title
        article_abstract = bundle.article_metadata.abstract or ""
        table_metadata = json.dumps(table.metadata, indent=2, sort_keys=True)
        prompt = f"""
You are a neuroimaging table curation assistant. Your job is to parse raw HTML/XML activation/summary tables (from neuroimaging papers) and
produce exactly one JSON object that strictly conforms to the AnalysisCollection schema below.
Follow these rules exactly and conservatively — do not invent, infer beyond the rules, or emit any
extra text.

Required output schema (must match exactly; do NOT add fields):
{_SCHEMA_TEMPLATE}

Top-level and formatting constraints (enforce every time)
- Output ONLY a single JSON object and nothing else.
- Do NOT add, rename, or omit top-level fields beyond the schema above.
- Do NOT add any other fields anywhere in the JSON.
- analysis "name" must be copied verbatim from the table (trim only leading/trailing whitespace;
  preserve punctuation and case).
- If the table contains no coordinates at all and names no contrast: return "analyses": [].
  Never invent a placeholder analysis for such a table.
- If coordinates exist but you cannot confidently assign them to any explicit analysis/contrast
  label, group those coordinates into one analysis named "UNKNOWN".
- If an analysis or contrast header is explicitly present in the table but has no coordinate rows
  under it, include it with coordinates: [].
- Only include contrasts[] if the table explicitly lists contrasts (names, conditions, weights,
  descriptions). If none are present, omit contrasts entirely.

Parsing sources and coordinate identification
- Only treat numeric triplets explicitly from X/Y/Z columns or from inline coordinate cells
  (e.g., "−6 −94 27", "10 20 30") as coordinates.
  - A valid coordinate MUST contain three numeric values mapping clearly to X, Y and Z.
  - Inline triplets must be split into three numeric components.
- Do NOT pull coordinates from other numbers in the row (e.g., cluster counts, indices,
  row numbers).
- If a row lacks a complete numeric triplet, skip that row's coordinate (do not fabricate
  missing numbers).
- If many or all rows lack parseable triplets and no coordinates can be obtained, return
  "analyses": [] -- except for contrasts the table names, each kept with coordinates: [].

Cleaning and normalization (apply before parsing)
- Remove HTML/XML formatting artifacts (tags like <i>, <sup>, <hsp/>, <ce:...>, &nbsp;, invisible
  spacing wrappers, etc.).
- Normalize minus signs: convert Unicode minus (U+2212) and broken sequences to ASCII "-" before
  numeric parsing.
- Trim only leading/trailing whitespace for analysis names; preserve inner whitespace/punctuation
  exactly as in the table header.

Header, layout, and grouping semantics
- Use explicit table section headers, row group headers, contrast label cells, or repeated
  column-block headers as the analysis.name. Use the exact header text (trim
  leading/trailing whitespace).
- If a contrast label spans multiple rows (rowspan/morerows), propagate that name to all rows in
  that row block.
- Respect colspan/rowspan/morerows semantics to determine which numeric columns map to X/Y/Z,
  statistic, cluster_size, region, etc.
- Reading order and duplicates:
  - Within a single analysis, include each unique triplet only once.

{ANALYSIS_BOUNDARY_RULES}

Statistic type, value, and cluster size inference rules
- statistic_type: read the COLUMN HEADER, and match a whole word or a header
  that is the bare letter by itself. A letter inside another word is not a
  statistic: "Extent", "Cluster" and "Talairach" all contain a "t", and the "z"
  of an "x | y | z" run is a COORDINATE column, never a Z statistic.
  - "z score", "Z-value", "Zmax", "Peak Z", or a column headed exactly "Z" => "Z".
  - "t-value", "T score", "t(38)", "Peak t", or a column headed exactly "T" => "T".
  - "Cohen's d", "effect size (d)", or a column headed exactly "d" => "D".
  - "Hedges' g" => "G".
  - "F-value", "F(2,38)", or a column headed exactly "F" => "F".
  - "correlation coefficient", "Pearson's r", "r value" => "R".
  - "beta", "regression coefficient", "parameter estimate" => "B".
  - "p-value", "p(FWE)", "p(unc.)", "pFDR" => "P".
  - If the table names MORE THAN ONE, report the first of these that appears:
    T, Z, D, G, F, R, B, P. A p-value is a significance level rather than a
    test statistic, so a table printing "#t | #p(FWE)" reports the t, and
    statistic_value is the number in THAT column.
  - If a numeric statistic value appears but no type can be read from the
    header, legend or caption, set statistic_type = null. Do not guess, and do
    not default to "T".
- statistic_value:
  - Parse the numeric statistic value as a float. If the cell contains extra text (e.g.,
    "0.32 (p<0.05)"), parse the leading numeric token only.
  - If statistic_value is non-numeric or missing, set statistic_value = null.
  - Keep the sign the table prints on statistic_value. Never flip it, and never take a sign
    from negative x/y/z coordinate components.
- cluster_size and cluster_measure:
  - Map cluster-count headers to cluster_measure = "voxels" when header text says "Voxels",
    "# voxels", "vox", "k", "kE", "extent", "Cluster extent", "No. of voxels" or similar.
  - Map cluster volume headers to "mm^3" only when the units are explicitly stated as "mm^3"
    (or "mm^3" present in header/legend).
  - If cluster size cannot be parsed or unit cannot be confidently inferred, set cluster_size =
    null and cluster_measure = null.
  - If parsed cluster size contains a spurious negative sign, take absolute value and store a
    non-negative integer.
  - cluster_size must be integer or null.
- space:
  - If legend/header/caption contains "MNI", "MNI coordinates", or "MNI space" => space = "MNI".
  - If legend/header/caption contains "Talairach", "Talairach coordinates", or notes Talairach conversion
    => space = "TAL".
  - If not stated/confident, set space = null.

Subpeak flag (boolean)
- is_subpeak:
  - true only if table explicitly denotes "subpeak", "submaxima", "submaxima", "subpeak",
    or legend states "submaxima"/"subpeaks" OR the table structure clearly indicates submaxima:
    - e.g., a multi-row cluster where the first/top row contains cluster size/extent/voxel count
      and the following rows in the same cluster block omit cluster size (morerows/rowspan
      semantics): treat those subsequent rows as subpeaks and set is_subpeak = true.
  - Otherwise set is_subpeak = false.
  - When is_subpeak = true, set cluster_size = null and cluster_measure = null for that coordinate
    unless cluster size is explicitly provided for the specific subpeak row.

Cleaning numeric parsing rules
- Accept integers or floats for x, y, z.
- statistic_value must be numeric float or null.
- For statistic_value cells with parenthetical notes/trailing text, parse the leading numeric
  token.
- If coordinate or statistic fields are missing or non-numeric, set the corresponding JSON fields
  to null (or skip coordinate if x/y/z incomplete).
- Normalize and remove any non-numeric characters surrounding numbers except leading "-" sign and
  digits.
- Do NOT use other numbers in a row (e.g., cluster count or Broadmann area) as coordinate components.

Subrow / cluster block handling
- If the first row of a multirow cluster includes a cluster extent/voxel count and subsequent rows
  omit it, and if legend/table structure supports submaxima interpretation, mark subsequent rows
  is_subpeak = true and set their cluster_size and cluster_measure to null (unless explicit
  cluster_size is present for that subrow).
- If table legend explicitly states "submaxima" or "subpeak", use it to mark subrows accordingly.

Contrasts field
- Only include contrasts[] when the table explicitly lists contrasts definitions (names,
  conditions, weights, descriptions). Fill name, conditions[], weights[] exactly as provided. If
  none are present, omit contrasts entirely (prefer omission to adding an empty array).

Ambiguities and conservative fallbacks
- When mapping is ambiguous, be conservative: place ambiguous coordinates under an analysis named
  "UNKNOWN" rather than guessing an analysis name.
- For any ambiguous numeric parsing (missing units, ambiguous statistic type), prefer null for that
  specific field rather than guessing.
- If you must make a non-trivial assumption, favor null or "UNKNOWN".

Final validation checks (required before emitting JSON)
- Every coordinate object must have numeric x, y, z values.
- statistic_value must be numeric float or null.
- statistic_type must be one of {"Z","T","P","R","F","B"} or null.
- cluster_size must be integer or null.
- cluster_measure must be "voxels", "mm^3" or null.
- space must be "MNI", "TAL" or null.
- is_subpeak must be an explicit boolean for each coordinate.
- No coordinate triplet may appear more than once across different analyses (apply
  first-occurrence reading-order rule).
- analysis.name values must be copied verbatim from the table (trim only leading/trailing
  whitespace).
- If the table contains no coordinates and names no contrast, "analyses" is []. An analysis
  with coordinates: [] is only ever a contrast the table names.

Processing workflow (recommended, must be followed)
1. Read the entire raw table content first. Remove XML/HTML artifacts and normalize whitespace and
   minus signs.
2. Inspect header/legend/caption for cues: statistic type ("T"/"Z"), coordinate space ("MNI"/"Talairach"),
   cluster units ("voxels"/"mm^3") and any "subpeak"/"submaxima" notation.
3. Identify the column mapping for X/Y/Z, statistic, cluster extent, and the column containing
   contrast/analysis headers. Respect colspan/rowspan/morerows.
4. Walk rows in reading order (top-to-bottom, left-to-right). Propagate any spanning
   contrast/analysis name(s) to subsequent rows as indicated by rowspan/morerows.
5. For each row block: parse a triplet only from the X/Y/Z columns or inline coordinate cells. If a
   valid triplet parsed, parse cluster size and statistic_value per the rules above.
6. Set is_subpeak according to explicit indicators and structural cues.
7. Deduplicate coordinates: if an identical triplet was already included earlier (in reading order)
   under any analysis, skip the later occurrence.
8. If coordinates are present but cannot be confidently assigned to an explicit analysis/contrast
   (no header/label), place them under a single analysis named "UNKNOWN".
9. If contrasts definitions are explicitly present, include contrasts[]. Otherwise omit contrasts.

Examples of header cues (use these heuristics)
- "MNI", "MNI coordinates" => space = "MNI"
- "Talairach", "Talairach coordinates" or footnote mentioning Talairach => space = "TAL"
- Header containing "<i>z</i>", "Zmax", "z score" => statistic_type = "Z"
- Header containing "<i>t</i>", "t-value", "T score" => statistic_type = "T"
- "Voxels", "# voxels", "k", "kE", "extent", "Cluster extent" => cluster_measure = "voxels"
- Units "mm^3" in header/legend => cluster_measure = "mm^3"

Failure modes to avoid
- Do NOT take a statistic's sign from negative coordinate components.
- Do NOT invent analysis or contrast names.
- Do NOT output anything other than the single JSON object described.
- Do NOT add explanatory text, logs, or extraneous fields.
- Do NOT assign statistic_type unless header/legend supports it — use null if ambiguous.

Edge cases
- If cluster size is shown only for the first line of a cluster and subsequent rows omit it, treat
  subsequent rows as subpeaks (if structure/legend supports it).
- If a cluster size has a negative sign due to parsing artifact, take absolute value.
- If a statistic cell contains a leading numeric token followed by text, parse only the numeric
  part (e.g., "3.21 (p<0.05)" => 3.21).
- If you encounter repeated column-block headers (multiple analyses sharing same subcolumns), treat
  each block header as a separate analysis and map that block's rows to that analysis.

Strict output rules summary
- Produce exactly one JSON object matching the schema above and nothing else.
- analysis.name text must be taken verbatim from table headings/contrast labels (trim only).
- Include coordinates arrays for each named analysis; use "UNKNOWN" when necessary (see rules).
- Omit contrasts unless explicitly provided in the table.
- All numeric and boolean fields must conform to the validation checks above.

If any rule conflicts with table content, follow the conservative fallback rules above (prefer
UNKNOWN and nulls). Always prefer missing/null/UNKNOWN over inventing values.


Article Title: {article_title}
Article Abstract: {article_abstract}
Table ID: {table_key}
Table Number: {table.table_number}
Table Caption: {table.caption}
Table Footer: {table.footer}
Table Metadata: {table_metadata}

Raw Table Content:
{table_text}
"""
        return prompt.strip()

    def _redundant(self, tables: Sequence[ExtractedTable], article_slug: str) -> set:
        """Table ids that repeat another table's numbers, and can be skipped.

        ACE renders the same physical table twice -- once from the CSV it
        parsed and once from the article HTML -- under different ids (`2` and
        `html-table-2`). Both pass triage, so both would be extracted and both
        uploaded, and the paper's coordinates counted twice. Measured over 800
        articles with more than one passing table, **72.6%** carry the same
        numbers twice and **36%** of extractions are redundant.

        Identity is the numbers in order, not the bytes: the two renderings
        differ in markup and whitespace but agree on every figure, and the
        figures are what gets stored (`table_figures`). The main text's copy wins
        over a supplement's (`services.supplements`); then a copy with a caption
        wins over one without, since ACE's own copy carries none; then the
        longer serialisation, because the two renderings are not always
        equally complete.
        """
        best: Dict[str, tuple] = {}
        found = []
        for index, table in enumerate(tables):
            key = table.table_id or sanitize_table_id(table.table_id, index)
            try:
                text = self._serialise(self._read_table_content(table), key)
            except (FileNotFoundError, OSError):
                continue
            numbers = table_figures(text)
            if len(numbers) < 3:
                continue                    # nothing numeric to collide on
            digest = hashlib.sha1(",".join(numbers).encode()).hexdigest()
            found.append((digest, key))
            # A supplement repeating a main-text table loses to it: the main text's is the
            # copy the paper's other stages and its readers point at.
            rank = ("supplement" not in (getattr(table, "metadata", None) or {}),
                    bool((table.caption or "").strip()), len(text))
            if digest not in best or rank > best[digest][1]:
                best[digest] = (key, rank)

        winners = {key for key, _ in best.values()}
        drop = {key for _, key in found if key not in winners}
        if drop:
            logger.info(
                "article %s: skipping %d table(s) repeating another's numbers: %s",
                article_slug, len(drop), sorted(drop),
            )
        return drop

    @staticmethod
    def _serialise(markup: str, table_key: str) -> str:
        """The table in the form the fine-tune was trained on.

        `_read_table_content` returns the raw HTML, which is what the prompted
        models are given and what their prompt describes. The fine-tune has
        never seen it: it was trained, and every number was measured, on
        `nspond_tables`' serialisation -- ` | ` between cells, `#` on a header
        cell, `<N` a colspan and `^N` a rowspan.

        The difference is not cosmetic. Measured over the tables that
        overflowed the context window, the raw HTML is **14.7x** larger, up to
        27.8x; one 23,692 token table serialises to 1,499. Sending the raw
        form put the model off-distribution and spent most of the context on
        markup -- and prefill is two thirds of the corpus's cost.

        A table the serialiser cannot read falls back to its **text**, not its
        markup: the tags are what made the raw form large, and a model that
        has never seen HTML gains nothing from `<td class="...">`. Cell and
        row boundaries are kept as ` | ` and newlines so the grid survives,
        which is the part the answer depends on.
        """
        from nspond_tables import serialize

        markup = _footnote_marks(normalize_minus(markup))
        try:
            out = serialize.serialize(markup)
        except Exception as exc:                       # noqa: BLE001 - any parse failure
            logger.warning("could not serialise table %s (%s); sending text", table_key, exc)
            out = ""
        if out.strip():
            lines = out.split("\n")
            return "\n".join(_signed_by_direction(line) for line in _leading_span_rows(lines))
        # Neither form read as a table. Text beats markup, and markup beats
        # nothing at all -- the model can only answer about what it is sent.
        return _text_of(markup) or markup

    def _read_table_content(self, table: ExtractedTable) -> str:
        path = Path(table.raw_content_path)
        if not path.exists():
            raise FileNotFoundError(f"Table raw content missing: {path}")
        try:
            return path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return path.read_text(encoding="utf-8", errors="ignore")


__all__ = ["CreateAnalysesService", "PLACEHOLDER_NAME", "sanitize_table_id", "table_reading"]

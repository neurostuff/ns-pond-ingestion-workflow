"""Service responsible for creating analyses from extracted tables."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Callable, Dict, List, Optional

from ingestion_workflow.clients import CoordinateParsingClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleExtractionBundle,
    Coordinate,
    CoordinateSpace,
    ExtractedTable,
    CoordinatePoint,
    ParseAnalysesOutput,
)
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
          "statistic_type"?: "t" | "Z" | "T" | null,
          "cluster_size"?: <int> | null,
          "cluster_measure"?: "voxels" | "mm^3" | null,
          "is_subpeak"?: true | false,
          "is_deactivation"?: true | false,
          "is_seed"?: true | false
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


def sanitize_table_id(table_id: str | None, index: int) -> str:
    """Sanitize table identifiers for filesystem-safe usage."""
    if table_id:
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", table_id).strip("-")
        if normalized:
            return normalized.lower()
    return f"table-{index + 1}"


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
    ) -> Dict[str, AnalysisCollection]:
        """Create analyses for every table in the bundle."""
        if not bundle.article_data.tables:
            return {}

        results: Dict[str, AnalysisCollection] = {}
        article_slug = bundle.article_data.slug
        identifier = bundle.article_data.identifier

        for index, table in enumerate(bundle.article_data.tables):
            if not table.contains_coordinates and not table.coordinates:
                logger.debug(
                    "Skipping table %s for article %s (no coordinates detected).",
                    table.table_id,
                    bundle.article_data.slug,
                )
                continue
            sanitized_table_id = sanitize_table_id(table.table_id, index)
            table_key = table.table_id or sanitized_table_id

            try:
                table_text = self._read_table_content(table)
            except FileNotFoundError as exc:
                logger.warning(
                    "Skipping table %s for article %s: %s",
                    table_key,
                    article_slug,
                    exc,
                )
                continue
            prompt = self._build_prompt(bundle, table, table_text, table_key)
            parsed_output = self.client.parse_analyses(prompt)
            if not parsed_output.analyses:
                logger.warning(
                    "LLM returned no analyses for article %s table %s",
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
            )
            results[table_key] = collection
            emit_progress(progress_hook)

        return results

    def _build_collection(
        self,
        parsed_output: ParseAnalysesOutput,
        table: ExtractedTable,
        identifier,
        sanitized_table_id: str,
        table_key: str,
        article_slug: str,
    ) -> AnalysisCollection:
        table_space = table.space or CoordinateSpace.OTHER
        collection = AnalysisCollection(
            slug=f"{article_slug}::{sanitized_table_id}",
            coordinate_space=table_space,
            identifier=identifier,
        )
        for idx, parsed in enumerate(parsed_output.analyses, start=1):
            coordinates = self._convert_points(
                parsed.points,
                table_space,
            )
            analysis_name = parsed.name or f"{table_key} analysis {idx}"
            analysis = Analysis(
                name=analysis_name,
                description=parsed.description,
                coordinates=coordinates,
                table_id=table_key,
                table_number=table.table_number,
                table_caption=table.caption or "",
                table_footer=table.footer or "",
                metadata={
                    "table_metadata": dict(table.metadata),
                    "sanitized_table_id": sanitized_table_id,
                },
            )
            collection.add_analysis(analysis)
        return collection

    def _convert_points(
        self,
        points: List[CoordinatePoint],
        default_space: CoordinateSpace,
    ) -> List[Coordinate]:
        coordinates: List[Coordinate] = []
        for point in points:
            space = self._coerce_space(point.space, default_space)
            statistic_value = None
            statistic_type = None
            cluster_size = point.cluster_size
            cluster_measure = point.cluster_measure
            is_subpeak = bool(point.is_subpeak)
            is_deactivation = bool(point.is_deactivation)
            is_seed = bool(point.is_seed)
            if cluster_size is not None:
                try:
                    cluster_size = abs(int(cluster_size))
                except (TypeError, ValueError):
                    cluster_size = None
            if cluster_measure is not None:
                normalized_measure = str(cluster_measure).strip().lower()
                if normalized_measure not in {"voxels", "mm^3", "mm3"}:
                    cluster_measure = None
                elif normalized_measure in {"mm^3", "mm3"}:
                    cluster_measure = "mm^3"
                else:
                    cluster_measure = "voxels"
            if point.values:
                primary_value = point.values[0]
                statistic_type = primary_value.kind
                try:
                    statistic_value = (
                        float(primary_value.value) if primary_value.value is not None else None
                    )
                except (TypeError, ValueError):
                    statistic_value = None
            coordinates.append(
                Coordinate(
                    x=point.coordinates[0],
                    y=point.coordinates[1],
                    z=point.coordinates[2],
                    space=space,
                    statistic_value=statistic_value,
                    statistic_type=statistic_type,
                    cluster_size=cluster_size,
                    cluster_measure=cluster_measure,
                    is_subpeak=is_subpeak,
                    is_deactivation=is_deactivation,
                    is_seed=is_seed,
                )
            )
        return coordinates

    def _coerce_space(
        self, space_label: Optional[str], fallback: CoordinateSpace
    ) -> CoordinateSpace:
        if not space_label:
            return fallback
        normalized = str(space_label).strip().upper()
        if normalized == "MNI":
            return CoordinateSpace.MNI
        if normalized in {"TAL", "TALAIRACH"}:
            return CoordinateSpace.TALAIRACH
        return fallback

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
- If the table contains no coordinates at all: return exactly one analysis object with name
  "UNKNOWN" and coordinates: [] and do NOT include contrasts.
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
  "UNKNOWN" with coordinates: [].

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
- Repeated column-block headers (e.g., "Pattern identification n=15",
  "Pattern validation n=32") are treated as distinct analyses; use the block header verbatim as
  analysis "name".
- Do NOT create separate analyses for hemispheres. If a hemisphere column is present (e.g.,
  "L"/"R"), keep the single analysis name and include both hemisphere coordinates under it.
- Respect colspan/rowspan/morerows semantics to determine which numeric columns map to X/Y/Z,
  statistic, cluster_size, region, etc.
- Reading order and duplicates:
  - Within a single analysis, include each unique triplet only once.

Statistic type, value, and cluster size inference rules
- statistic_type:
  - If any header/legend contains "z", "Z", "z score", or "Zmax" => statistic_type = "Z".
  - If header/legend contains "t", "T", "t-value", "T-value", "T score" => statistic_type = "t"
    (lowercase) by default. Only set "T" (uppercase) if the table explicitly uses "T" as the
    statistic label and context unambiguously suggests uppercase.
  - If a numeric statistic value appears but no explicit type can be inferred from headers/legend/caption,
    set statistic_type = null.
- statistic_value:
  - Parse the numeric statistic value as a float. If the cell contains extra text (e.g.,
    "0.32 (p<0.05)"), parse the leading numeric token only.
  - If statistic_value is non-numeric or missing, set statistic_value = null.
  - If statistic_value is explicitly negative, set is_deactivation = true. Otherwise
    is_deactivation = false unless the table explicitly labels the coordinate as a "deactivation"
    or "negative".
  - Do NOT infer deactivation from negative x/y/z coordinate components.
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

Subpeak, seed, and deactivation flags (booleans)
- is_deactivation:
  - true only if statistic_value is explicitly negative OR the table explicitly labels that
    coordinate as a deactivation/negative.
  - Otherwise set is_deactivation = false.
- is_subpeak:
  - true only if table explicitly denotes "subpeak", "submaxima", "submaxima", "subpeak",
    or legend states "submaxima"/"subpeaks" OR the table structure clearly indicates submaxima:
    - e.g., a multi-row cluster where the first/top row contains cluster size/extent/voxel count
      and the following rows in the same cluster block omit cluster size (morerows/rowspan
      semantics): treat those subsequent rows as subpeaks and set is_subpeak = true.
  - Otherwise set is_subpeak = false.
  - When is_subpeak = true, set cluster_size = null and cluster_measure = null for that coordinate
    unless cluster size is explicitly provided for the specific subpeak row.
- is_seed:
  - true only if the row/coordinate is explicitly labeled as a "seed", "seed region",
    "seed location", or similar explicit seed label, such as Region of Interest or ROI.
  - Otherwise set is_seed = false.

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
- statistic_type must be one of {"t","Z","T"} or null.
- cluster_size must be integer or null.
- cluster_measure must be "voxels", "mm^3" or null.
- space must be "MNI", "TAL" or null.
- is_subpeak, is_deactivation, is_seed must be explicit booleans for each coordinate.
- No coordinate triplet may appear more than once across different analyses (apply
  first-occurrence reading-order rule).
- analysis.name values must be copied verbatim from the table (trim only leading/trailing
  whitespace).
- If the table contains no coordinates, return exactly one analysis with name "UNKNOWN" and
  coordinates: [].

Processing workflow (recommended, must be followed)
1. Read the entire raw table content first. Remove XML/HTML artifacts and normalize whitespace and
   minus signs.
2. Inspect header/legend/caption for cues: statistic type ("t"/"Z"), coordinate space ("MNI"/"Talairach"),
   cluster units ("voxels"/"mm^3") and any "subpeak"/"submaxima" notation.
3. Identify the column mapping for X/Y/Z, statistic, cluster extent, and the column containing
   contrast/analysis headers. Respect colspan/rowspan/morerows.
4. Walk rows in reading order (top-to-bottom, left-to-right). Propagate any spanning
   contrast/analysis name to subsequent rows as indicated by rowspan/morerows.
5. For each row block: parse a triplet only from the X/Y/Z columns or inline coordinate cells. If a
   valid triplet parsed, parse cluster size and statistic_value per the rules above.
6. Set flags (is_subpeak, is_deactivation, is_seed) according to explicit indicators and structural
   cues.
7. Deduplicate coordinates: if an identical triplet was already included earlier (in reading order)
   under any analysis, skip the later occurrence.
8. If coordinates are present but cannot be confidently assigned to an explicit analysis/contrast
   (no header/label), place them under a single analysis named "UNKNOWN".
9. If contrasts definitions are explicitly present, include contrasts[]. Otherwise omit contrasts.

Examples of header cues (use these heuristics)
- "MNI", "MNI coordinates" => space = "MNI"
- "Talairach", "Talairach coordinates" or footnote mentioning Talairach => space = "TAL"
- Header containing "<i>z</i>", "Zmax", "z score" => statistic_type = "Z"
- Header containing "<i>t</i>", "t-value", "T score" => statistic_type = "t"
- "Voxels", "# voxels", "k", "kE", "extent", "Cluster extent" => cluster_measure = "voxels"
- Units "mm^3" in header/legend => cluster_measure = "mm^3"

Failure modes to avoid
- Do NOT infer deactivation from negative coordinate components.
- Do NOT invent analysis or contrast names.
- Do NOT output anything other than the single JSON object described.
- Do NOT add explanatory text, logs, or extraneous fields.
- Do NOT create separate analyses for hemispheres.
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

    def _read_table_content(self, table: ExtractedTable) -> str:
        path = Path(table.raw_content_path)
        if not path.exists():
            raise FileNotFoundError(f"Table raw content missing: {path}")
        try:
            return path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return path.read_text(encoding="utf-8", errors="ignore")


__all__ = ["CreateAnalysesService", "sanitize_table_id"]

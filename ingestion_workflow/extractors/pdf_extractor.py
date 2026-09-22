"""Download and extract article content from open-access PDFs.

Fallback source for articles that neither PubMed Central (via pubget) nor
Elsevier serve as XML. PDF locations come from Semantic Scholar's
``openAccessPdf`` and OpenAlex's ``best_oa_location``; parsing is Docling's.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.extractors.docling_convert import usable_cuda_devices
from ingestion_workflow.extractors.utils import safe_hash_stem, sanitize_table_id
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    ExtractedTable,
    ExtractionResult,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.utils.progress import emit_progress

logger = logging.getLogger(__name__)

PDF_MAGIC = b"%PDF-"
DOWNLOAD_TIMEOUT = 60

# Docling renders some glyphs it cannot map as GLYPH<n>; a decimal point read
# as a colon is the other artifact seen in practice. Cells matching this are
# flagged rather than rewritten -- "1 : 2" is a legitimate ratio, so guessing
# would silently corrupt real values.
_SUSPECT_DECIMAL = re.compile(r"^\s*\d+\s*:\s*\d+\s*$")


class PdfExtractor(BaseExtractor):
    """Extractor that downloads open-access PDFs and parses them with Docling."""

    _SUPPORTED_IDS = {"doi", "pmid"}

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self._session = session or requests.Session()

    # ----------------------------------------------------------------- download

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        if not identifiers:
            return []

        identifier_list = list(identifiers)
        pdf_urls = self._resolve_pdf_urls(identifiers)

        base_dir = Path(self.settings.pdf_cache_root or self.settings.get_cache_dir("pdf"))
        base_dir.mkdir(parents=True, exist_ok=True)

        worker_count = max(1, self.settings.max_workers)
        ordered: List[Optional[DownloadResult]] = [None] * len(identifier_list)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(
                    self._download_single,
                    identifier,
                    pdf_urls.get(identifier.slug),
                    base_dir,
                ): index
                for index, identifier in enumerate(identifier_list)
            }
            for future in as_completed(future_map):
                index = future_map[future]
                identifier = identifier_list[index]
                try:
                    ordered[index] = future.result()
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.exception("PDF download raised for %s", identifier.slug)
                    ordered[index] = self._failure(identifier, f"PDF download failed: {exc}")
                emit_progress(progress_hook)

        return [
            result or self._failure(identifier, "PDF download did not return a result.")
            for identifier, result in zip(identifier_list, ordered)
        ]

    def _resolve_pdf_urls(self, identifiers: Identifiers) -> Dict[str, str]:
        """Ask each configured provider, in order, for a PDF URL per identifier."""
        pdf_urls: Dict[str, str] = {}

        for provider in self.settings.pdf_url_providers:
            pending = Identifiers(
                [
                    identifier
                    for identifier in identifiers
                    if identifier.slug not in pdf_urls
                ]
            )
            if not pending.identifiers:
                break

            try:
                if provider == "semantic_scholar":
                    pdf_urls.update(self._semantic_scholar_pdf_urls(pending))
                elif provider == "openalex":
                    pdf_urls.update(self._openalex_pdf_urls(pending))
                else:
                    logger.warning("Unknown pdf_url provider: %s", provider)
            except Exception:
                logger.exception("PDF URL lookup failed for provider %s", provider)

        return pdf_urls

    def _semantic_scholar_pdf_urls(self, identifiers: Identifiers) -> Dict[str, str]:
        api_key = self.settings.semantic_scholar_api_key
        if not api_key:
            return {}
        client = SemanticScholarClient(api_key)
        metadata = client.get_metadata(list(identifiers))
        return {slug: meta.pdf_url for slug, meta in metadata.items() if meta.pdf_url}

    def _openalex_pdf_urls(self, identifiers: Identifiers) -> Dict[str, str]:
        email = self.settings.openalex_email
        if not email:
            return {}
        return OpenAlexClient(email).get_pdf_urls(identifiers)

    def _download_single(
        self,
        identifier: Identifier,
        pdf_url: Optional[str],
        base_dir: Path,
    ) -> DownloadResult:
        if not pdf_url:
            return self._failure(identifier, "No open-access PDF URL found.")

        try:
            response = self._session.get(pdf_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()
            payload = response.content
        except Exception as exc:
            return self._failure(identifier, f"PDF fetch failed: {exc}")

        if not payload.startswith(PDF_MAGIC):
            # Landing pages and paywall interstitials return HTML with a 200.
            return self._failure(identifier, f"Response from {pdf_url} is not a PDF.")

        article_dir = base_dir / safe_hash_stem(identifier.slug)
        article_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = article_dir / "article.pdf"
        pdf_path.write_bytes(payload)

        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PDF,
            success=True,
            files=[
                DownloadedFile(
                    file_path=pdf_path,
                    file_type=FileType.PDF,
                    content_type="application/pdf",
                    source=DownloadSource.PDF,
                )
            ],
        )

    @staticmethod
    def _failure(identifier: Identifier, message: str) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PDF,
            success=False,
            error_message=message,
        )

    # ------------------------------------------------------------------ extract

    def extract(
        self,
        download_results: List[DownloadResult],
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[ExtractionResult]:
        extraction_root = Path(self.settings.data_root) / "extractions" / "pdf"
        gpus = usable_cuda_devices()
        worker_count = self._worker_count(gpus)
        logger.info(
            "PDF extraction: %d worker(s) over %s",
            worker_count,
            f"GPU(s) {gpus}" if gpus else "CPU",
        )
        return self._run_extraction_pipeline(
            download_results,
            extraction_root=extraction_root,
            worker=_extract_pdf_article,
            worker_count=worker_count,
            worker_initializer=_pin_worker_to_gpu if gpus else None,
            worker_initargs=(
                (multiprocessing.get_context("spawn").Value("i", 0), gpus) if gpus else ()
            ),
            source_name="PDF",
            failure_message="PDF extraction did not produce a result.",
            failure_builder=_build_failure_content,
            progress_hook=progress_hook,
        )


    def _worker_count(self, gpus: List[int]) -> int:
        """How many Docling processes to run at once.

        Docling holds a model per process, so this is bounded by memory rather
        than by cores: one worker per usable GPU, and one on CPU, where a second
        worker would contend for the cores the pipeline already threads across.
        """
        configured = self.settings.pdf_extract_workers
        if configured:
            return max(1, configured)
        return max(1, len(gpus))


_COORD_COLUMN = re.compile(
    r"coordinate|\bcoords?\b|talairach|\btal\b|\bmni\b|location", re.IGNORECASE
)
_TRIPLET_CELL = re.compile(
    r"^\s*[-+]?\d+(?:\.\d+)?\s*[,;\s]\s*[-+]?\d+(?:\.\d+)?\s*[,;\s]\s*[-+]?\d+(?:\.\d+)?\s*$"
)
_MIN_MERGED_TRIPLETS = 3


def _merge_split_coordinate_columns(frame: Any) -> Any:
    """Rejoin an x/y/z triplet that Docling split across coordinate columns.

    Docling sometimes cuts a single "42, 30, 45" cell down the middle, leaving
    "42," in one column and "30, 45" in the next. Downstream detection needs
    either three columns headed x/y/z or one column holding whole triplets, so a
    split like this yields nothing at all.

    Only runs of adjacent columns whose headers already say coordinate/MNI/
    Talairach are considered, and the merge is kept only if it produces whole
    triplets for at least a few rows. A demographics table cannot qualify: its
    headers do not match, and pasting its columns together does not make
    triplets.
    """
    columns = [str(c) for c in frame.columns]
    runs = []
    start = None
    for index, name in enumerate(columns + [""]):
        if index < len(columns) and _COORD_COLUMN.search(name):
            start = index if start is None else start
            continue
        if start is not None:
            if index - start >= 2:
                runs.append((start, index))
            start = None

    if not runs:
        return frame

    result = frame
    # right to left, so earlier column positions stay valid as columns collapse
    for begin, end in reversed(runs):
        block = result.iloc[:, begin:end]
        joined = block.apply(
            lambda row: " ".join(str(v).strip() for v in row if str(v).strip()), axis=1
        )
        whole = joined.apply(lambda v: bool(_TRIPLET_CELL.match(v))).sum()
        if whole < _MIN_MERGED_TRIPLETS:
            continue
        kept = [i for i in range(result.shape[1]) if not begin <= i < end]
        rebuilt = result.iloc[:, kept]
        rebuilt.insert(begin, "Coordinates", joined.values, allow_duplicates=True)
        result = rebuilt
        logger.debug(
            "merged %d coordinate columns into one, %d whole triplets",
            end - begin,
            int(whole),
        )
    return result


def _pin_worker_to_gpu(counter: Any, gpus: List[int]) -> None:
    """Give this worker process exactly one GPU, before torch is imported.

    Each worker takes the next index off a shared counter, so two workers never
    land on the same card. Setting CUDA_VISIBLE_DEVICES rather than choosing a
    device later means the worker's torch only ever sees the one it owns.
    """
    with counter.get_lock():
        ordinal = counter.value
        counter.value += 1
    device = gpus[ordinal % len(gpus)]
    os.environ["CUDA_VISIBLE_DEVICES"] = str(device)


def _build_failure_content(
    download_result: DownloadResult, message: str
) -> ExtractedContent:
    return ExtractedContent(
        slug=download_result.identifier.slug,
        source=DownloadSource.PDF,
        identifier=download_result.identifier,
        full_text_path=None,
        tables=[],
        has_coordinates=False,
        error_message=message,
    )


def _extract_pdf_article(
    download_result: DownloadResult,
    extraction_root: Path | str,
) -> ExtractedContent:
    """Convert one downloaded PDF into text plus per-table CSV files."""
    from pubget._coordinate_space import _neurosynth_guess_space
    from pubget._coordinates import _extract_coordinates_from_table

    from ingestion_workflow.extractors.docling_convert import (
        convert_pdf,
        normalize_text_tokens,
    )
    from ingestion_workflow.extractors.utils import (
        coordinate_from_row,
        coordinate_space_from_guess,
    )

    slug = download_result.identifier.slug
    pdf_file = next(
        (f for f in download_result.files if f.file_type is FileType.PDF),
        None,
    )
    if pdf_file is None:
        return _build_failure_content(download_result, "No PDF file in download result.")

    output_dir = Path(extraction_root) / safe_hash_stem(slug)
    tables_dir = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    try:
        document = convert_pdf(pdf_file.file_path)
    except Exception as exc:
        return _build_failure_content(download_result, f"Docling conversion failed: {exc}")

    full_text = normalize_text_tokens(document.export_to_text())
    full_text_path = output_dir / "article.txt"
    full_text_path.write_text(full_text, encoding="utf-8")

    article_space = coordinate_space_from_guess(_neurosynth_guess_space(full_text))

    extracted_tables: List[ExtractedTable] = []
    for index, table in enumerate(document.tables):
        caption = " ".join(_caption_texts(table, document)).strip()
        table_id = sanitize_table_id(None, _table_label(caption), index)

        frame = table.export_to_dataframe(doc=document).map(normalize_text_tokens)
        frame = _merge_split_coordinate_columns(frame)
        csv_path = tables_dir / f"{table_id}.csv"
        frame.to_csv(csv_path, index=False)

        try:
            coordinates_frame = _extract_coordinates_from_table(frame)
        except Exception as exc:
            logger.warning("%s: coordinate parsing failed for %s: %s", slug, table_id, exc)
            coordinates_frame = None

        coordinates = []
        if coordinates_frame is not None and not coordinates_frame.empty:
            for _, row in coordinates_frame.iterrows():
                coord = coordinate_from_row(row, article_space)
                if coord is not None:
                    coordinates.append(coord)

        extracted_tables.append(
            ExtractedTable(
                table_id=table_id,
                raw_content_path=csv_path,
                table_number=index + 1,
                caption=caption,
                footer="",
                coordinates=coordinates,
                space=article_space,
                metadata={
                    "suspect_numeric_glyphs": _has_suspect_numerics(frame),
                },
            )
        )

    return ExtractedContent(
        slug=slug,
        source=DownloadSource.PDF,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=extracted_tables,
        has_coordinates=any(table.coordinates for table in extracted_tables),
    )


_TABLE_LABEL = re.compile(r"\b(table\s*[0-9ivx]+)", re.IGNORECASE)


def _table_label(caption: str) -> Optional[str]:
    """Pull a 'Table 3'-style label out of a caption, for use as a filename."""
    match = _TABLE_LABEL.search(caption)
    return match.group(1) if match else None


def _caption_texts(table: Any, document: Any) -> List[str]:
    captions: List[str] = []
    for caption_ref in getattr(table, "captions", []) or []:
        try:
            resolved = caption_ref.resolve(doc=document)
        except Exception:
            continue
        text = getattr(resolved, "text", None) or getattr(resolved, "content", None)
        if text:
            captions.append(str(text))
    return captions


def _has_suspect_numerics(frame: Any) -> bool:
    """True when any cell looks like a decimal point Docling read as a colon."""
    return bool(
        frame.map(lambda cell: bool(_SUSPECT_DECIMAL.match(str(cell)))).to_numpy().any()
    )


__all__ = ["PdfExtractor"]

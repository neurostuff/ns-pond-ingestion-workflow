"""Download and Extract tables from articles using ACE."""

from __future__ import annotations

import logging
import os
import re
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from ace.config import update_config
from ace.scrape import Scraper
from ace.sources import SourceManager, table_text
from bs4 import BeautifulSoup
from ace import extract as ace_extract

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.extractors.utils import (
    build_downloaded_file,
    build_failure_extraction,
    normalize_minus,
)
from ingestion_workflow.models import (
    Identifier,
    Identifiers,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    ExtractionResult,
    FileType,
    ExtractedContent,
    ExtractedTable,
    Coordinate,
    CoordinateSpace,
)

from ingestion_workflow.utils import slugify
from ingestion_workflow.patches import apply_ace_patch
from ingestion_workflow.patches.ace_patch import set_skip_remote_tables
from ingestion_workflow.utils.progress import emit_progress


apply_ace_patch()


logger = logging.getLogger(__name__)

_HTML_INVALID_MARKERS: list[tuple[str, str]] = [
    ("<title>new tab</title>", "HTML payload captured a browser new-tab page."),
    ("api rate limit exceeded", "HTML payload contains an API rate limit error."),
    ("access to this page has been denied", "HTML payload was an access-denied page."),
]
_MIN_HTML_LENGTH = 500

# A challenge page names itself in its title, or carries little but the
# challenge. "captcha" alone is no sign: publishers put a reCAPTCHA login
# widget on article pages. Of 261 pages rejected for containing it, 242 were
# articles with 7,000-128,000 characters of text; the one real challenge
# ("Checking your browser - reCAPTCHA") had 167.
_CHALLENGE_TITLE = re.compile(
    r"captcha|checking your browser|are you a robot|just a moment|verify you are human",
    re.I,
)
_CHALLENGE_MAX_TEXT = 3000
_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)
_INVISIBLE = re.compile(r"<script\b.*?</script>|<style\b.*?</style>|<[^>]+>", re.S | re.I)


def _is_challenge(html_text: str) -> bool:
    title = _TITLE.search(html_text)
    if title and _CHALLENGE_TITLE.search(title.group(1)):
        return True
    visible = _HTML_WS.sub(" ", _INVISIBLE.sub(" ", html_text)).strip()
    return len(visible) < _CHALLENGE_MAX_TEXT


def _sanitize_table_id(candidate: Optional[str], index: int) -> str:
    fallback = f"table-{index}"
    if not candidate:
        return fallback
    sanitized = slugify(candidate).strip("-")
    return sanitized.lower() or fallback


def _coordinate_space_from_guess(guess: Optional[str]) -> CoordinateSpace:
    if not guess:
        return CoordinateSpace.OTHER
    normalized = str(guess).strip().upper()
    if normalized == "MNI":
        return CoordinateSpace.MNI
    if normalized in {"TAL", "TALAIRACH"}:
        return CoordinateSpace.TALAIRACH
    if normalized == "UNKNOWN":
        return CoordinateSpace.OTHER
    return CoordinateSpace.OTHER


def _resolve_table_space(table: Any, article: Any) -> CoordinateSpace:
    parts = [
        getattr(table, "caption", None),
        getattr(table, "label", None),
        getattr(table, "notes", None),
    ]
    metadata_text = " ".join(part for part in parts if part)
    guess = ace_extract.guess_space(metadata_text)
    if guess == "UNKNOWN":
        guess = getattr(article, "space", None)
    return _coordinate_space_from_guess(guess)


def _coordinate_from_activation(activation: Any, space: CoordinateSpace) -> Optional[Coordinate]:
    if activation is None:
        return None
    coords = (
        getattr(activation, "x", None),
        getattr(activation, "y", None),
        getattr(activation, "z", None),
    )
    if any(value is None for value in coords):
        return None
    try:
        x_val = float(activation.x)
        y_val = float(activation.y)
        z_val = float(activation.z)
    except (TypeError, ValueError):
        return None

    statistic_value = None
    raw_stat = getattr(activation, "statistic", None)
    if raw_stat not in (None, ""):
        try:
            statistic_value = float(raw_stat)
        except (TypeError, ValueError):
            statistic_value = None

    cluster_size = None
    raw_size = getattr(activation, "size", None)
    if raw_size not in (None, ""):
        try:
            cluster_size = int(float(str(raw_size)))
        except (TypeError, ValueError):
            cluster_size = None

    return Coordinate(
        x=x_val,
        y=y_val,
        z=z_val,
        space=space,
        statistic_value=statistic_value,
        cluster_size=cluster_size,
    )


def _select_html_file(
    download_result: DownloadResult,
) -> Optional[DownloadedFile]:
    for downloaded in download_result.files:
        if downloaded.file_type is FileType.HTML:
            return downloaded
    return None


def _validate_downloaded_html(file_path: Path) -> tuple[bool, Optional[str]]:
    """Validate downloaded HTML content to catch placeholders or errors."""
    try:
        html_text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        html_text = file_path.read_text(errors="ignore")

    normalized = html_text.strip()
    if not normalized:
        return False, "HTML payload was empty."

    lowered = normalized.lower()
    if "<html" not in lowered:
        return False, "HTML payload is missing an <html> tag."

    for marker, reason in _HTML_INVALID_MARKERS:
        if marker in lowered:
            return False, reason
    if "captcha" in lowered and _is_challenge(normalized):
        return False, "HTML payload appears to be a CAPTCHA challenge."

    if len(normalized) < _MIN_HTML_LENGTH:
        return False, (f"HTML payload is unexpectedly small ({len(normalized)} characters).")

    return True, None


_HTML_TABLE = re.compile(r"<table\b[^>]*>.*?</table>", re.S | re.I)
_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_WS = re.compile(r"\s+")


#: A table's figures, free of thousands separators. Two renderings of one
#: table disagree about markup and whitespace but not about its numbers.
_FP_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")
_FP_THOUSANDS = re.compile(r"(?<=\d),(?=\d{3}\b)")


def _table_fingerprint(html: str) -> str:
    """The figures a table states, in order, for matching one to another.

    ACE rewrites the markup it keeps in `input_html`, and the rewrite changes
    the *text* as well: entities are decoded differently and `Empty Cell`
    placeholders appear, so the tag-free text of the two renderings differs by
    tens of characters -- 497 against 557 for one measured table. Matching on
    that text therefore missed the duplicates it was written to catch, and the
    same table entered the corpus twice under two ids. Over articles with more
    than one table that triage passed, 72.6% carried the same numbers twice.

    Numbers survive the rewrite. A table with fewer than three is not
    identified this way at all, so captions and layout tables cannot collide.
    """
    text = _HTML_TAG.sub(" ", normalize_minus(html))
    numbers = _FP_NUMBER.findall(_FP_THOUSANDS.sub("", text))
    if len(numbers) >= 3:
        return "n:" + ",".join(numbers)
    # Nothing numeric to match on; fall back to the visible text, which is
    # still enough to spot a byte-for-byte repeat.
    return _HTML_WS.sub("", text).lower()


_WHITESPACE = re.compile(r"[\s\u00a0]+")
_ENTITIES = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"'}


def _plain(raw: str) -> str:
    """Tags out, entities decoded, whitespace collapsed."""
    text = _HTML_TAG.sub(" ", raw or "")
    for k, v in _ENTITIES.items():
        text = text.replace(k, v)
    text = re.sub(r"&[a-z#0-9]+;", " ", text)
    return _WHITESPACE.sub(" ", text).strip()


_CAPTION_TAG = re.compile(r"<caption\b[^>]*>(.*?)</caption>", re.S | re.I)
# A caption usually sits immediately before the table in a block whose class or
# id says so, or begins "Table 3." A footnote sits immediately after.
_CAPTION_BLOCK = re.compile(
    r"<(?:div|p|span|h\d)\b[^>]*(?:class|id)=\"[^\"]*(?:caption|tblCaption|table-title"
    r"|label)[^\"]*\"[^>]*>(.*?)</(?:div|p|span|h\d)>", re.S | re.I)
_TABLE_LABEL = re.compile(r"(?:^|>)\s*(Table\s+[IVXLC\d]+[.:]?\s[^<]{0,300})", re.I)
_FOOTER_BLOCK = re.compile(
    r"<(?:div|p|span)\b[^>]*(?:class|id)=\"[^\"]*(?:foot|note|legend|tblFn)[^\"]*\""
    r"[^>]*>(.*?)</(?:div|p|span)>", re.S | re.I)


def _caption_and_footer(html_text: str, start: int, end: int, block: str) -> tuple[str, str]:
    """Text belonging to a table that the markup keeps outside it.

    A caption names the contrast and often the coordinate space, and a footnote
    carries the threshold and the statistic -- 15.7% of real tables state their
    space only in that surrounding text. Scanning the table element alone drops
    it, which left every rescued table with nothing but its cells.

    Looks inside the table for <caption>, then in a window just before it for a
    captioned block or a "Table N." line, and in a window just after for a
    footnote block.
    """
    inside = _CAPTION_TAG.search(block)
    caption = _plain(inside.group(1)) if inside else ""

    # Never look past another table. A caption sitting before a previous
    # </table> belongs to that table, and inheriting it is worse than having
    # none -- a wrong caption feeds the space rule a wrong answer.
    before = html_text[max(0, start - 2500):start]
    cut = before.lower().rfind("</table>")
    if cut != -1:
        before = before[cut + len("</table>"):]
    if not caption:
        hits = _CAPTION_BLOCK.findall(before)
        if hits:
            caption = _plain(hits[-1])
    if not caption:
        labels = _TABLE_LABEL.findall(_HTML_TAG.sub(" ", before))
        if labels:
            caption = _plain(labels[-1])

    after = html_text[end:end + 2500]
    stop = after.lower().find("<table")
    if stop != -1:
        after = after[:stop]
    notes = _FOOTER_BLOCK.findall(after)
    footer = _plain(notes[0]) if notes else ""
    return caption[:1200], footer[:1200]


def _unparsed_html_tables(
    html_text: str,
    ace_tables: Sequence[ExtractedTable],
    tables_dir: Path,
    space: CoordinateSpace,
) -> list[ExtractedTable]:
    """Every `<table>` in the document that ACE did not return.

    ACE yields only the tables its own parser identified as activation tables,
    so a table it missed was absent from the artifact entirely -- not merely
    unlabelled, but unavailable to any later detector, and the article's
    coordinates lost with it. Measured over 500 sampled ace tables, 100% carried
    a non-empty coordinate list, which is what a filtered set looks like; the
    negative class had no representation at all.

    Keeping them costs storage and nothing else: `coordinates` stays empty, so
    `create_analyses` still skips them until something says otherwise.
    """
    seen = set()
    for extracted in ace_tables:
        try:
            seen.add(_table_fingerprint(
                extracted.raw_content_path.read_text(encoding="utf-8")))
        except OSError:
            continue

    out: list[ExtractedTable] = []
    for index, match in enumerate(_HTML_TABLE.finditer(html_text or "")):
        block = match.group(0)
        fingerprint = _table_fingerprint(block)
        if not fingerprint or fingerprint in seen:
            continue
        seen.add(fingerprint)
        table_id = f"html-table-{index + 1}"
        path = tables_dir / f"{table_id}.html"
        # Decoded on the way to disk: everything downstream reads this
        # file, and an undecoded minus is a coordinate in the wrong
        # hemisphere.
        path.write_text(normalize_minus(block), encoding="utf-8")
        caption, footer = _caption_and_footer(
            html_text, match.start(), match.end(), block)
        out.append(ExtractedTable(
            table_id=table_id,
            raw_content_path=path,
            table_number=None,
            caption=caption,
            footer=footer,
            metadata={"origin": "html-scan", "document_index": index},
            coordinates=[],
            space=space,
        ))
    return out


def _translate_ace_table(
    table: Any,
    article: Any,
    tables_dir: Path,
    table_index: int,
) -> ExtractedTable:
    table_id = _sanitize_table_id(
        getattr(table, "number", None),
        table_index + 1,
    )
    raw_filename = tables_dir / f"{table_id}.html"
    raw_html = getattr(table, "input_html", None) or ""
    raw_filename.parent.mkdir(parents=True, exist_ok=True)
    if raw_html:
        raw_filename.write_text(raw_html, encoding="utf-8")
    else:
        raw_filename.write_text(
            "<!-- ACE did not retain raw table HTML -->",
            encoding="utf-8",
        )

    space = _resolve_table_space(table, article)
    coordinates = [
        coord
        for coord in (
            _coordinate_from_activation(activation, space)
            for activation in getattr(table, "activations", [])
        )
        if coord is not None
    ]

    metadata = {
        "label": getattr(table, "label", None),
        "notes": getattr(table, "notes", None),
        "position": getattr(table, "position", None),
        "n_activations": getattr(table, "n_activations", None),
        "n_columns": getattr(table, "n_columns", None),
    }

    table_number = None
    raw_number = getattr(table, "number", None)
    if raw_number is not None:
        try:
            table_number = int(str(raw_number))
        except ValueError:
            table_number = None

    return ExtractedTable(
        table_id=table_id,
        raw_content_path=raw_filename,
        table_number=table_number,
        caption=getattr(table, "caption", "") or "",
        footer=getattr(table, "notes", "") or "",
        metadata={k: v for k, v in metadata.items() if v is not None},
        coordinates=coordinates,
        space=space,
    )


MIN_NODE = (20, 19)


def _node_dir(node_path: Path | str) -> Path:
    path = Path(node_path).expanduser()
    return path.parent if path.is_file() else path


def prepare_node(settings: Settings) -> None:
    """Put `node_path` first on PATH, then refuse a node too old for readabilipy.

    Call once at setup, before readabilipy first runs. The PATH change reaches
    the extraction worker processes because they are started afterwards.
    """
    if settings.node_path:
        node_dir = str(_node_dir(settings.node_path))
        parts = os.environ.get("PATH", "").split(os.pathsep)
        if parts[0] != node_dir:
            os.environ["PATH"] = os.pathsep.join([node_dir] + parts)
    required = ".".join(map(str, MIN_NODE))
    fix = "set node_path in the settings file or INGEST_NODE_PATH to a directory with a newer node"
    try:
        out = subprocess.run(
            ["node", "--version"], capture_output=True, text=True, timeout=30
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError(
            f"ACE needs node >= {required} for readabilipy, but no working node "
            f"was found on PATH; {fix}."
        ) from None
    match = re.match(r"v?(\d+)\.(\d+)", out)
    if not match or (int(match[1]), int(match[2])) < MIN_NODE:
        raise RuntimeError(
            f"ACE needs node >= {required} for readabilipy, but found "
            f"{out or 'unknown'} on PATH; {fix}."
        )


def _require_readability() -> None:
    """Refuse to extract when readabilipy cannot run.

    When its node step fails, ACE falls back to a cruder cleaner and says so
    only in a per-article warning, so a whole run's text changes silently.
    readabilipy's bundled jsdom needs node >= 20.19; beast's /usr/bin/node is
    16, and nvm's newer node is on PATH only in interactive shells.
    """
    global _READABILITY_OK
    if _READABILITY_OK:
        return
    from readabilipy import simple_json_from_html_string

    try:
        simple_json_from_html_string(
            "<html><body><p>probe</p></body></html>", use_readability=True
        )
    except Exception as exc:
        raise RuntimeError(
            "readabilipy cannot run, so ACE would extract text with its "
            "fallback cleaner. Put node >= 20.19 first on PATH "
            "(settings node_path or INGEST_NODE_PATH)."
        ) from exc
    _READABILITY_OK = True


_READABILITY_OK = False


def _with_fetched_tables(text: str, tables: Sequence[Any]) -> str:
    """Append the tables ACE fetched from pages other than the article's.

    `keep_tables` places the tables the article page holds. Some publishers
    serve tables on separate pages, which ACE downloads after the text is
    built; those are added here, rendered the same way.
    """
    for table in tables:
        markup = getattr(table, "input_html", None)
        element = BeautifulSoup(markup, "lxml").find("table") if markup else None
        rendered = table_text(element) if element is not None else ""
        if rendered and rendered not in text:
            text = f"{text.rstrip()}\n\n{rendered}"
    return text


def _extract_ace_article(
    download_result: DownloadResult,
    extraction_root: Path,
) -> ExtractedContent:
    update_config(SAVE_ORIGINAL_HTML=True)
    html_file = _select_html_file(download_result)
    if html_file is None:
        raise ValueError("ACE extraction requires an HTML payload.")

    slug = download_result.identifier.slug
    article_dir = extraction_root / slug
    tables_dir = article_dir / "tables"
    source_tables_dir = article_dir / "downloaded_tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    source_tables_dir.mkdir(parents=True, exist_ok=True)

    html_text = html_file.file_path.read_text(encoding="utf-8")
    manager = SourceManager(table_dir=str(source_tables_dir))
    # A page no publisher's identifiers match goes to ACE's generic parser, as
    # ACE's own ingest does with force_ingest. Raising instead lost every table
    # on such a page: 11 of 43 articles whose coordinate tables autonima had,
    # each found by DefaultSource.
    source = manager.identify_source(html_text) or manager.default_source
    if source is None:
        raise ValueError("ACE could not identify an article source.")

    article = source.parse_article(
        html_text,
        pmid=download_result.identifier.pmid,
        metadata_dir=None,
        skip_metadata=True,
        keep_tables=True,
    )
    if not article:
        raise ValueError("ACE failed to parse the article content.")

    article_text = _with_fetched_tables(
        getattr(article, "text", "") or "", getattr(article, "tables", [])
    )
    full_text_path = article_dir / "article.txt"
    full_text_path.write_text(article_text, encoding="utf-8")

    extracted_tables = [
        _translate_ace_table(table, article, tables_dir, index)
        for index, table in enumerate(getattr(article, "tables", []))
    ]
    # ACE returns only the tables it recognised as activation tables. Keep the
    # rest of the document's tables too: one ACE missed used to be absent from
    # the artifact, so no later detector could find it and the article's
    # coordinates were lost. They arrive with an empty coordinate list, so
    # nothing downstream treats them as results.
    extracted_tables.extend(_unparsed_html_tables(
        html_text,
        extracted_tables,
        tables_dir,
        _coordinate_space_from_guess(getattr(article, "space", None)),
    ))

    has_coordinates = any(table.coordinates for table in extracted_tables)

    return ExtractedContent(
        slug=slug,
        source=DownloadSource.ACE,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=extracted_tables,
        has_coordinates=has_coordinates,
        error_message=None,
    )


def _run_ace_extraction_task(
    download_result: DownloadResult, extraction_root: Path | str
) -> ExtractedContent:
    root_path = Path(extraction_root)
    try:
        return _extract_ace_article(download_result, root_path)
    except Exception as exc:  # pragma: no cover - worker failure logging
        logger.exception("ACE extraction failed for %s", download_result.identifier.slug)
        return build_failure_extraction(download_result, DownloadSource.ACE, str(exc))


class ACEExtractor(BaseExtractor):
    """Extractor that uses ACE to download and extract tables from articles."""

    _SUPPORTED_IDS = {"pmid"}
    _HTML_CONTENT_TYPE = "text/html"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        download_mode: str = "browser",
    ) -> None:
        self.settings = settings or load_settings()
        self.settings.ensure_directories()
        prepare_node(self.settings)

        # Into the environment, not a module flag: extraction runs in a process
        # pool and a spawned worker does not inherit the parent's globals.
        set_skip_remote_tables(bool(self.settings.ace_skip_remote_tables))

        self._cache_root = self._resolve_cache_root()
        self._extraction_root = self._resolve_extraction_root()

        update_config(SAVE_ORIGINAL_HTML=True)
        self._download_mode = download_mode

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> list[DownloadResult]:
        if not identifiers:
            return []

        worker_count = self.settings.ace_max_workers
        if worker_count <= 0:
            worker_count = self.settings.max_workers
        worker_count = max(1, worker_count)

        identifiers_list = identifiers.identifiers

        if worker_count == 1 or len(identifiers_list) <= 1:
            scraper = self._build_scraper()
            results = []
            for identifier in identifiers_list:
                result = self._download_single(identifier, scraper=scraper)
                results.append(result)
                emit_progress(progress_hook)
            return results

        ordered_results: list[Optional[DownloadResult]] = [None] * len(identifiers_list)

        thread_local = threading.local()

        def _run(identifier: Identifier) -> DownloadResult:
            scraper = getattr(thread_local, "scraper", None)
            if scraper is None:
                scraper = self._build_scraper()
                thread_local.scraper = scraper
            return self._download_single(identifier, scraper=scraper)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run, identifier): index
                for index, identifier in enumerate(identifiers_list)
            }
            for future in as_completed(future_map):
                index = future_map[future]
                identifier = identifiers_list[index]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.exception(
                        "ACE download raised exception for PMID %s",
                        identifier.pmid,
                    )
                    result = self._failure(
                        identifier,
                        f"ACE download raised an exception: {exc}",
                    )
                ordered_results[index] = result
                emit_progress(progress_hook)

        results: list[DownloadResult] = []
        for index, identifier in enumerate(identifiers_list):
            result = ordered_results[index]
            if result is None:
                result = self._failure(
                    identifier,
                    "ACE download did not return a result.",
                )
            results.append(result)

        return results

    def extract(
        self,
        download_results: list[DownloadResult],
        progress_hook: Callable[[int], None] | None = None,
    ) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using ACE."""
        if download_results:
            _require_readability()
        return self._run_extraction_pipeline(
            download_results,
            extraction_root=self._extraction_root,
            worker=_run_ace_extraction_task,
            worker_count=self.settings.max_workers,
            source_name="ACE",
            failure_message="ACE extraction did not produce a result.",
            failure_builder=lambda download_result, message: build_failure_extraction(
                download_result,
                DownloadSource.ACE,
                message,
            ),
            progress_hook=progress_hook,
        )

    def _download_single(
        self,
        identifier: Identifier,
        *,
        scraper: Scraper | None = None,
    ) -> DownloadResult:
        pmid_value = identifier.pmid
        if not pmid_value:
            message = "ACE download requires a PMID."
            logger.warning(message)
            return self._failure(identifier, message)

        pmid = str(pmid_value).strip()

        active_scraper = scraper or self._build_scraper()

        journal = "IngestionWorkflow"

        try:
            file_path, valid = active_scraper.process_article(
                pmid,
                journal,
                delay=None,
                mode=self._download_mode,
                # Whether to fetch again is the catalog's call (`--refresh download:ace`);
                # by the time this runs it has decided to, so a file already on disk is reused.
                overwrite=False,
                prefer_pmc_source=False,
            )
        except Exception as exc:  # pragma: no cover - surfaced to caller
            logger.exception("ACE scraper failed for PMID %s", pmid)
            return self._failure(identifier, f"ACE scrape failed: {exc}")

        resolved_path = self._resolve_file_path(file_path, journal, pmid)

        if valid is False:
            message = f"ACE validation failed for PMID {pmid}."
            logger.warning(message)
            return self._failure(identifier, message)

        if resolved_path is None:
            message = "ACE scrape did not produce HTML content."
            logger.warning("%s PMID=%s", message, pmid)
            return self._failure(identifier, message)

        html_valid, invalid_reason = _validate_downloaded_html(resolved_path)
        if not html_valid:
            try:
                resolved_path.unlink()
            except OSError as exc:  # pragma: no cover - best-effort cleanup
                logger.debug(
                    "Failed to delete invalid ACE HTML payload %s: %s",
                    resolved_path,
                    exc,
                )
            reason = invalid_reason or "HTML payload failed validation."
            message = f"ACE scrape produced invalid HTML: {reason}"
            logger.warning("%s PMID=%s", message, pmid)
            return self._failure(identifier, message)

        downloaded_file = self._build_downloaded_file(resolved_path)
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.ACE,
            success=True,
            files=[downloaded_file],
            error_message=None,
        )

    def _build_scraper(self) -> Scraper:
        return Scraper(
            str(self._cache_root),
            api_key=self.settings.pubmed_api_key,
        )

    def _resolve_file_path(
        self,
        file_path: Path | str | None,
        journal: str,
        pmid: str,
    ) -> Optional[Path]:
        candidate = Path(file_path) if file_path else None
        if candidate and candidate.exists():
            return candidate

        journal_dir = self._cache_root / "html" / journal
        fallback = journal_dir / f"{pmid}.html"
        if fallback.exists():
            return fallback

        matches = list(self._cache_root.glob(f"html/**/{pmid}.html"))
        return matches[0] if matches else None

    def _build_downloaded_file(self, file_path: Path) -> DownloadedFile:
        return build_downloaded_file(
            file_path,
            FileType.HTML,
            source=DownloadSource.ACE,
            content_type=self._HTML_CONTENT_TYPE,
        )

    def _failure(self, identifier: Identifier, message: str) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.ACE,
            success=False,
            files=[],
            error_message=message,
        )

    def _resolve_cache_root(self) -> Path:
        base = self.settings.ace_cache_root or self.settings.get_cache_dir("ace")
        base.mkdir(parents=True, exist_ok=True)
        (base / "html").mkdir(parents=True, exist_ok=True)
        return base

    def _resolve_extraction_root(self) -> Path:
        base = self._cache_root / "extracted"
        base.mkdir(parents=True, exist_ok=True)
        return base

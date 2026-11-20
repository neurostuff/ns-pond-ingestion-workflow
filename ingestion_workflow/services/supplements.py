"""An article's supplementary material: listed, fetched, read, and appended to its extraction.

Routes (`ROUTES`), each from what ns-pond already holds for the article:

* `pmc`: the article's package in PMC's Cloud Service bucket, by PMCID, matched to the
  JATS `supplementary-material` hrefs by file name.
* `elsevier`: the `mmc` objects of a stored Elsevier XML, from Elsevier's CDN.
* `springer`: the ESM files a stored Springer JATS names.
* `publisher`: links to supplementary files on a stored publisher page (ACE's HTML).

The routes, the file types worth fetching and reading DOCX with Docling follow
anthonyangg/supplementary-extraction (see NOTICE).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse

from lxml import etree

from ingestion_workflow.models import ExtractedContent, ExtractedTable

logger = logging.getLogger(__name__)

ROUTES = ("pmc", "elsevier", "springer", "publisher")

#: Extensions fetched and read. Images, video, archives and code are listed and passed over.
READ = {"pdf", "docx", "doc", "xlsx", "xls", "csv", "tsv", "txt"}
SHEETS = {"xlsx", "xls", "csv", "tsv"}

#: Supplements that are about the paper's review or reporting, not its results.
NOT_RESULTS = re.compile(
    r"peer\s+review|reporting\s+summary|review\s+history|author\s+checklist|"
    r"\bconsort\b|\bprisma\b|\barrive\b|\bstrobe\b|checklist", re.I)

#: The heading the supplementary material sits under, after the figure legends.
HEADING = "Supplementary material"

#: Rows of a sheet written into the text; the table itself keeps every row.
SHEET_TEXT_ROWS = 400

#: Rows of each table a supplement's text keeps when it is appended, after the table's first
#: row; the table itself keeps every row. Data sheets and long Word tables were 69% of the
#: text of the supplements over the text cap (2026-11-20, 1,500 articles); 92% of the tables
#: captioned as demographics or participants have no more rows than this.
APPENDED_TABLE_ROWS = 60

#: Bumped when what `append` writes of a supplement changes: an extraction names it with the
#: supplements' read digest, so the articles with supplements are joined again.
APPEND_VERSION = 2

_PIPE_RULE = re.compile(r"^\|[\s|:-]*-[\s|:-]*\|?$")

PMC_CLOUD = "https://pmc-oa-opendata.s3.amazonaws.com"
ELSEVIER_CDN = "https://ars.els-cdn.com/content/image"
SPRINGER_ESM = "https://static-content.springer.com/esm/art%3A{doi}/MediaObjects/{name}"
XLINK = "{http://www.w3.org/1999/xlink}href"
USER_AGENT = "ns-pond-ingestion-workflow (supplementary material)"


@dataclass
class Supplement:
    """One item of supplementary material, as listed, then as fetched and read."""

    route: str
    url: Optional[str] = None
    name: str = ""
    label: str = ""
    caption: str = ""
    #: Why it was not fetched, or not read; None once read.
    reason: Optional[str] = None
    path: Optional[str] = None
    sha256: Optional[str] = None
    size: Optional[int] = None
    #: What reading it made: its text file and its tables (`ExtractedTable.to_dict`).
    text_path: Optional[str] = None
    tables: List[Dict[str, Any]] = field(default_factory=list)
    #: How it was read: "docling", "mineru", "sheet", "text".
    read_by: Optional[str] = None

    @property
    def extension(self) -> str:
        return extension(self.name)

    @property
    def read(self) -> bool:
        return self.reason is None and self.text_path is not None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v not in (None, "", [])}

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Supplement":
        return cls(**{k: payload[k] for k in cls.__dataclass_fields__ if k in payload})


def extension(name: str) -> str:
    stem = unquote(urlparse(name).path or name).rsplit("/", 1)[-1]
    return stem.rsplit(".", 1)[-1].lower() if "." in stem else ""


def _squash(text: str) -> str:
    return " ".join((text or "").split())


def _text(el) -> str:
    return _squash("".join(el.itertext())) if el is not None else ""


def _local(el) -> str:
    tag = el.tag if isinstance(el.tag, str) else ""
    return tag.rsplit("}", 1)[-1].split(":")[-1]


def passed_over(item: Supplement) -> Optional[str]:
    """Why a listed item is not fetched, if it is not."""
    if not item.url:
        return "no address"
    if item.extension not in READ:
        return f"not read: .{item.extension or '?'}"
    if NOT_RESULTS.search(f"{item.label} {item.caption}"):
        return "about the paper's review or reporting"
    return None


# -- listing ---------------------------------------------------------------------


def jats_items(root) -> List[Tuple[str, str, str]]:
    """`(href, label, caption)` of each file a JATS article lists as supplementary material."""
    out, seen = [], set()
    for supp in root.iter():
        name = _local(supp)
        if name not in ("supplementary-material", "inline-supplementary-material"):
            continue
        label = _text(next((c for c in supp if _local(c) == "label"), None))
        caption = _text(next((c for c in supp if _local(c) == "caption"), None))
        hrefs = [supp.get(XLINK)] if name == "inline-supplementary-material" else []
        hrefs += [el.get(XLINK) for el in supp.iter() if _local(el) in ("media", "ext-link")]
        for href in filter(None, (h.strip() for h in hrefs if h)):
            if href not in seen:
                seen.add(href)
                out.append((href, label, caption))
    return out


def pmc_items(jats, keys: Sequence[str], prefix: str) -> List[Supplement]:
    """The supplementary files of a PMC package: each JATS href the package holds by name.

    `keys` are the package's object names under `prefix` (`PMC<n>.<v>/`). An href the
    package does not hold is listed with that reason; nothing is matched by position."""
    held = {key[len(prefix):] for key in keys if key.startswith(prefix)}
    out = []
    for href, label, caption in jats_items(jats):
        name = unquote(urlparse(href).path).rsplit("/", 1)[-1]
        url = f"{PMC_CLOUD}/{prefix}{quote(name)}" if name in held else None
        item = Supplement("pmc", url, name, label, caption)
        if url is None:
            item.reason = "not in PMC's Cloud Service package"
        out.append(item)
    return out


def elsevier_items(root) -> List[Supplement]:
    """The `mmc` attachments an Elsevier full-text response lists, at Elsevier's CDN.

    Each one's label and caption come from the `e-component` with its locator."""
    described: Dict[str, Tuple[str, str]] = {}
    for comp in root.iter():
        if _local(comp) != "e-component":
            continue
        label = _text(next((c for c in comp if _local(c) == "label"), None))
        caption = _text(next((c for c in comp if _local(c) == "caption"), None))
        for link in comp.iter():
            if _local(link) == "link" and link.get("locator"):
                described[link.get("locator")] = (label, caption)
        if comp.get("id"):
            described.setdefault(comp.get("id"), (label, caption))
    out, seen = [], set()
    for obj in root.iter():
        if _local(obj) != "object":
            continue
        ref = (obj.get("ref") or "").strip()
        mime = (obj.get("mimetype") or "").lower()
        api_url = (obj.text or "").strip()
        if not re.match(r"mmc\d+$", ref, re.I) or ref in seen or not api_url:
            continue
        if mime.startswith(("image/", "video/", "audio/")):
            continue
        seen.add(ref)
        name = unquote(urlparse(api_url).path).rsplit("/", 1)[-1]
        label, caption = described.get(ref, ("", ""))
        out.append(Supplement("elsevier", f"{ELSEVIER_CDN}/{quote(name)}", name, label, caption))
    return out


def springer_items(jats, doi: str) -> List[Supplement]:
    """The ESM files a Springer JATS lists, each at Springer's static-content host."""
    out = []
    for href, label, caption in jats_items(jats):
        name = unquote(urlparse(href).path).rsplit("/", 1)[-1]
        if href.lower().startswith("http"):
            url = href
        elif doi:
            url = SPRINGER_ESM.format(doi=quote(doi, safe=""), name=quote(name))
        else:
            url = None
        out.append(Supplement("springer", url, name, label, caption))
    return out


#: Hosts and paths that serve a publisher's supplementary files, as ACE's pages link them.
#: A link elsewhere counts only when its path says it is supplementary and names a file.
_PUBLISHER_FILES = [
    re.compile(r"^https?://ars\.els-cdn\.com/content/image/1-s2\.0-[^/]+-mmc\d+\.\w+$", re.I),
    re.compile(r"^https?://(?:static-content\.springer\.com|media\.springernature\.com)/.*/esm/", re.I),
    re.compile(r"/inline-supplementary-material-\d+\.\w+", re.I),
    re.compile(r"^https?://[\w.-]+\.silverchair-cdn\.com/.*suppl", re.I),
    re.compile(r"^https?://onlinelibrary\.wiley\.com/action/downloadSupplement\?", re.I),
    re.compile(r"/doi/suppl/[^#]+/suppl_file/", re.I),
    re.compile(r"^https?://(?:www\.)?frontiersin\.org/api/.+/file/", re.I),
]
_MIAMI = re.compile(r"MiamiMultiMediaURL/[^/]+/(1-s2\.0-[^/]+-mmc\d+\.\w+)", re.I)
_SUPPLEMENTARY_PATH = re.compile(r"suppl|/esm/|[-_]mmc\d|data[-_]sheet|supporting", re.I)


def page_url(doc) -> Optional[str]:
    """The page's own address, from its canonical link or Open Graph URL."""
    for xpath in ('//link[@rel="canonical"]/@href', '//meta[@property="og:url"]/@content'):
        found = doc.xpath(xpath)
        if found and str(found[0]).startswith("http"):
            return str(found[0])
    return None


def link_name(url: str) -> str:
    """The file a link names: its `file=` parameter, else its last path segment with an
    extension (Frontiers ends `/file/Data_Sheet_1.docx/1234/1`), else its last segment."""
    query_name = re.search(r"[?&]file=([^&]+)", url)
    if query_name:
        return unquote(query_name.group(1))
    segments = [unquote(s) for s in urlparse(url).path.split("/") if s]
    named = [s for s in segments if extension(s) in READ]
    return named[-1] if named else (segments[-1] if segments else "")


def publisher_items(html: bytes, base: Optional[str] = None) -> List[Supplement]:
    """The supplementary files a publisher's article page links to."""
    from lxml import html as lxml_html

    try:
        doc = lxml_html.fromstring(html)
    except (etree.ParserError, ValueError):
        return []
    base = base or page_url(doc)
    out, seen = [], set()
    for anchor in doc.iter("a"):
        href = (anchor.get("href") or "").strip().replace("&amp;", "&")
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        url = urljoin(base, href) if base else href
        if not url.startswith("http"):
            continue
        miami = _MIAMI.search(url)
        if miami:
            url = f"{ELSEVIER_CDN}/{miami.group(1)}"
        known = any(p.search(url) for p in _PUBLISHER_FILES)
        name = link_name(url)
        if not known and not (_SUPPLEMENTARY_PATH.search(urlparse(url).path) and extension(name) in READ):
            continue
        key = url.split("?", 1)[0] if "silverchair" in url else url
        if key in seen:
            continue
        seen.add(key)
        out.append(Supplement("publisher", url, name, _squash(" ".join(anchor.itertext()))[:200]))
    return out


# -- fetching ----------------------------------------------------------------------


#: The reason of an item a busy or down host did not serve: noted on the artifact as
#: transient, so the article is asked again after the backoff (`Context.due_again`).
TRANSIENT = "transient"

#: What a file of each type starts with: a host that answers with a page or an error in
#: place of the file is caught by it.
_MAGIC = {"pdf": (b"%PDF",), "docx": (b"PK",), "xlsx": (b"PK",), "doc": (b"\xd0\xcf\x11\xe0",),
          "xls": (b"\xd0\xcf\x11\xe0",)}
_PAGE = re.compile(rb"^\s*(<!doctype html|<html|<\?xml[^>]*>\s*<html)", re.I)


def is_transient(item: Supplement) -> bool:
    return bool(item.reason) and item.reason.startswith(TRANSIENT)


def not_the_file(extension_: str, head: bytes) -> Optional[str]:
    """Why bytes are not a file of `extension_`, if they are not."""
    if _PAGE.match(head) and extension_ != "txt":
        return "a page, not the file"
    magic = _MAGIC.get(extension_)
    if magic and not head.startswith(magic):
        return f"not a .{extension_} file"
    return None


def public_host(url: str) -> bool:
    """Whether every address the URL's host resolves to is public: a page's link may name
    any host, and none of ours is to be asked."""
    import ipaddress
    import socket

    host = urlparse(url).hostname
    if not host:
        return False
    try:
        addresses = {info[4][0] for info in socket.getaddrinfo(host, None)}
    except OSError:
        return False
    return bool(addresses) and all(ipaddress.ip_address(a.split("%")[0]).is_global for a in addresses)


def fetch(item: Supplement, dest: Path, session, *, max_bytes: int, reuse: bool = True) -> None:
    """Fetch `item` into `dest`, recording its path, size and sha256, or why it has none.

    A busy or down host (timeout, 429, 5xx) leaves the reason `transient: ...`. A file a
    finished fetch left at `dest` is reused unless `reuse` is false (`--refresh`). A link
    from a publisher page is fetched only from a public host, redirects included."""
    import requests

    from ingestion_workflow.utils.http import transient

    if reuse and dest.is_file():
        # Only a complete fetch is renamed into place, so one there is this file.
        data = dest.read_bytes()
        item.path, item.size, item.sha256 = str(dest), len(data), hashlib.sha256(data).hexdigest()
        return
    if item.route == "publisher" and not public_host(item.url):
        item.reason = "not a public host"
        return
    try:
        with session.get(item.url, stream=True, timeout=(20, 120),
                         headers={"User-Agent": USER_AGENT}) as response:
            if item.route == "publisher" and response.url != item.url and not public_host(response.url):
                item.reason = "redirected to a host that is not public"
                return
            if response.status_code >= 400:
                if response.status_code == 429 or response.status_code >= 500:
                    item.reason = f"{TRANSIENT}: HTTP {response.status_code}"
                else:
                    item.reason = f"HTTP {response.status_code}"
                return
            declared = int(response.headers.get("Content-Length") or 0)
            if declared > max_bytes:
                item.reason = f"larger than the cap ({max_bytes:,} bytes)"
                return
            kind = response.headers.get("Content-Type", "").lower()
            if ("text/html" in kind or "xhtml" in kind) and item.extension != "txt":
                item.reason = "a page, not the file"
                return
            dest.parent.mkdir(parents=True, exist_ok=True)
            digest, size, head = hashlib.sha256(), 0, b""
            part = dest.with_name(dest.name + ".part")
            with part.open("wb") as handle:
                for chunk in response.iter_content(1 << 16):
                    size += len(chunk)
                    if size > max_bytes:
                        break
                    head = head or chunk[:512]
                    digest.update(chunk)
                    handle.write(chunk)
            if size > max_bytes:
                part.unlink(missing_ok=True)
                item.reason = f"larger than the cap ({max_bytes:,} bytes)"
                return
            wrong = not_the_file(item.extension, head)
            if wrong:
                part.unlink(missing_ok=True)
                item.reason = wrong
                return
            part.replace(dest)
    except requests.RequestException as exc:
        part = dest.with_name(dest.name + ".part")
        part.unlink(missing_ok=True)
        item.reason = (f"{TRANSIENT}: {type(exc).__name__}" if transient(exc)
                       else f"{type(exc).__name__}")
        return
    item.path, item.size, item.sha256 = str(dest), size, digest.hexdigest()


def file_name(index: int, item: Supplement) -> str:
    """The name a fetched item is stored under: its order, then its own name made safe."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", item.name).strip("._") or "file"
    return f"{index:02d}-{safe[:120]}"


# -- reading -----------------------------------------------------------------------


def read_sheets(item: Supplement, out_dir: Path) -> None:
    """A spreadsheet or delimited file: one CSV table per sheet, the sheet's name as caption,
    and the sheets' rows as text."""
    import pandas as pd

    path = Path(item.path)
    if item.extension in ("csv", "tsv"):
        sep = "\t" if item.extension == "tsv" else None
        sheets = {"": pd.read_csv(path, sep=sep, engine="python", header=None, dtype=str,
                                  on_bad_lines="skip", encoding_errors="replace")}
    else:
        sheets = pd.read_excel(path, sheet_name=None, header=None, dtype=str)
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    lines = []
    for n, (sheet, frame) in enumerate(sheets.items(), start=1):
        frame = frame.dropna(how="all").dropna(axis=1, how="all").fillna("")
        if frame.empty:
            continue
        table_id = f"sheet-{n:02d}"
        csv_path = tables_dir / f"{table_id}.csv"
        frame.to_csv(csv_path, index=False, header=False)
        caption = str(sheet) if sheet else item.caption
        item.tables.append(ExtractedTable(table_id=table_id, raw_content_path=csv_path,
                                          caption=caption).to_dict())
        lines.append(caption or f"Sheet {n}")
        lines += ["\t".join(str(v).strip() for v in row) for row in frame.values[:SHEET_TEXT_ROWS]]
        lines.append("")
    _write_text(item, out_dir, "\n".join(lines))
    item.read_by = "sheet"


_WORD_CONVERTER = None


def _word_converter():
    """One Docling converter for every Word file a process reads: building one takes seconds."""
    global _WORD_CONVERTER
    if _WORD_CONVERTER is None:
        from docling.datamodel.base_models import InputFormat
        from docling.document_converter import DocumentConverter

        _WORD_CONVERTER = DocumentConverter(allowed_formats=[InputFormat.DOCX])
    return _WORD_CONVERTER


_CAPTION_LINE = re.compile(r"^\W*(?:supplementa\w*\s+)?table\s*s?\d+", re.I)


def word_captions(document) -> List[str]:
    """For each table of a Word document, in order, the nearest paragraph before it that starts
    "Table N": Docling links no caption to a Word table (supplementary-extraction's rule)."""
    out, last = [], ""
    for node, _ in document.iterate_items():
        if node in document.tables:
            out.append(last)
            last = ""
            continue
        text = _squash(getattr(node, "text", "") or "")
        if _CAPTION_LINE.match(text):
            last = text
    return out


def read_docx(item: Supplement, out_dir: Path, path: Optional[Path] = None) -> None:
    """A Word file through Docling: its text, captions and tables as a PDF's are read."""
    from ingestion_workflow.extractors.docling_convert import normalize_text_tokens
    from ingestion_workflow.extractors.pdf_extractor import document_tables, pdf_text_and_captions

    document = _word_converter().convert(str(path or item.path)).document
    text, _ = pdf_text_and_captions(document, normalize_text_tokens)
    (out_dir / "tables").mkdir(parents=True, exist_ok=True)
    tables = document_tables(item.name, document, out_dir / "tables", None)
    for table, caption in zip(tables, word_captions(document)):
        table.caption = table.caption or caption
    item.tables = [t.to_dict() for t in tables]
    _write_text(item, out_dir, text)
    item.read_by = "docling"


def read_doc(item: Supplement, out_dir: Path) -> None:
    """A legacy .doc, converted to .docx by LibreOffice, then read as one."""
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if soffice is None:
        item.reason = "no converter for .doc (LibreOffice not installed)"
        return
    converted = (out_dir / "converted").resolve()
    converted.mkdir(parents=True, exist_ok=True)
    # LibreOffice takes its profile as a file:// URL, which must be absolute; a relative
    # one leaves it waiting until the timeout.
    profile = (out_dir / "lo-profile").resolve()
    subprocess.run([soffice, f"-env:UserInstallation={profile.as_uri()}", "--headless",
                    "--convert-to", "docx", "--outdir", str(converted), str(Path(item.path).resolve())],
                   check=True, capture_output=True, timeout=300)
    shutil.rmtree(profile, ignore_errors=True)
    docx = converted / (Path(item.path).stem + ".docx")
    if not docx.exists():
        item.reason = "LibreOffice made no .docx"
        return
    read_docx(item, out_dir, docx)


def read_txt(item: Supplement, out_dir: Path) -> None:
    raw = Path(item.path).read_bytes()
    _write_text(item, out_dir, raw.decode("utf-8", errors="replace"))
    item.read_by = "text"


def _write_text(item: Supplement, out_dir: Path, text: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "text.txt"
    path.write_text(text.strip() + "\n", encoding="utf-8")
    item.text_path = str(path)


#: Largest a Word file or workbook (a zip) may unpack to, and most it may expand by.
ZIP_MAX_BYTES = 500 * 2**20
ZIP_MAX_RATIO = 200


def zip_too_big(path: Path) -> Optional[str]:
    """Why a .docx or .xlsx is not opened: it would unpack to far more than it holds."""
    import zipfile

    try:
        with zipfile.ZipFile(path) as archive:
            unpacked = sum(info.file_size for info in archive.infolist())
    except zipfile.BadZipFile:
        return "not a zip (a damaged .docx or .xlsx)"
    packed = max(1, Path(path).stat().st_size)
    if unpacked > ZIP_MAX_BYTES or unpacked / packed > ZIP_MAX_RATIO:
        return f"unpacks to {unpacked:,} bytes from {packed:,}"
    return None


def read(item: Supplement, out_dir: Path) -> None:
    """Read a fetched item that is not a PDF (PDFs are read in a batch by `pdf_engines`)."""
    try:
        if item.extension in ("docx", "xlsx"):
            item.reason = zip_too_big(Path(item.path))
            if item.reason:
                return
        if item.extension in SHEETS:
            read_sheets(item, out_dir)
        elif item.extension == "docx":
            read_docx(item, out_dir)
        elif item.extension == "doc":
            read_doc(item, out_dir)
        elif item.extension == "txt":
            read_txt(item, out_dir)
        else:
            item.reason = f"not read: .{item.extension}"
    except Exception as exc:  # noqa: BLE001 - one unreadable file is that file's problem
        logger.warning("%s: could not read %s: %s", item.route, item.name, exc)
        item.reason = f"unreadable: {type(exc).__name__}: {str(exc)[:200]}"
        item.text_path, item.tables = None, []


def from_pdf_content(item: Supplement, content: ExtractedContent) -> None:
    """Record what `pdf_engines` read from a supplementary PDF."""
    if content.full_text_path is None:
        item.reason = f"unreadable: {content.error_message or 'no text'}"
        return
    item.text_path = str(content.full_text_path)
    item.tables = [t.to_dict() for t in content.tables]
    item.read_by = (content.text_source or {}).get("method") or "pdf"


# -- appending to the extraction ------------------------------------------------------

_TABLE_NUMBER = re.compile(r"\btable\s*(s?)\s*(\d+)([a-z]?)\b", re.I)

#: Bump when what reading makes of the same file changes (a reader, a caption rule): the
#: extractions that appended it go stale, and only those.
READ_VERSION = 2


def table_label(caption: str, file_index: int, table_index: int, own_numbers: bool = True) -> str:
    """How the paper cites a supplementary table: "Table S3" when its caption says so, or when
    it says "Table 3" and `own_numbers` (the only file that numbers its tables, whose 3 is the
    paper's S3). Otherwise by file and position, in words no "Table N" citation matches."""
    match = _TABLE_NUMBER.search(caption or "")
    if match and (match.group(1) or own_numbers):
        return f"Table S{match.group(2)}{match.group(3).lower()}"
    return f"Supplementary file {file_index}, table #{table_index}"


def read_digest(items: Sequence[Supplement]) -> Optional[str]:
    """What an extraction appends of `items`, as one value: each read file's route, name,
    bytes (sha256) and reader, and `READ_VERSION`. None when nothing was read."""
    read = sorted((i.route, i.name, i.sha256 or "", i.read_by or "") for i in items if i.read)
    if not read:
        return None
    raw = json.dumps([READ_VERSION, read], separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def is_supplementary(table: ExtractedTable) -> bool:
    return "supplement" in (table.metadata or {})


def without_supplements(content: ExtractedContent) -> ExtractedContent:
    """The extraction as its extractor made it: the text before anything was appended."""
    if not content.supplements:
        return content
    content.tables = [t for t in content.tables if not is_supplementary(t)]
    if content.base_text_path:
        content.full_text_path = content.base_text_path
    content.supplements = []
    content.base_text_path = None
    return content


def append(content: ExtractedContent, supplements: Sequence[Supplement], out_dir: Path,
           max_chars: int = 100_000) -> ExtractedContent:
    """`content` with each read supplement's text after the text (after the figure legends)
    and its tables after the tables.

    The text before is unchanged, so every offset into it holds (figure captions, references,
    links). The joined text is written to `out_dir`, never beside the extractor's: that text may
    sit in a shared source cache, and is left as it is and named `base_text_path`. Each supplement's span in the joined text is recorded in
    `content.supplements`; each table's id is `s<k>-<its id>`, its `metadata.supplement` the
    supplement's index k (1-based), and its `metadata.table_label` how the paper cites it.
    Each table's caption is prefixed with the file it came from, so a supplement's "Table 3"
    is not read as the main text's. The supplements' text is cut at `max_chars` in all (the
    record model's prompt has a length limit); their tables are kept whole.

    Nothing is appended to an extraction without a text: supplements alone are not the
    article. Raises FileNotFoundError when the extraction names a text that is not there,
    for the same reason."""
    content = without_supplements(content)
    read = [s for s in supplements if s.read]
    if not read or content.full_text_path is None:
        return content
    base_path = Path(content.full_text_path)
    if not base_path.is_file():
        raise FileNotFoundError(f"the extraction's text is not there: {base_path}")
    base = base_path.read_text(encoding="utf-8")
    numbering = [s for s in read if any(_TABLE_NUMBER.search(t.get("caption") or "") for t in s.tables)]
    room = max_chars
    atx = re.search(r"(?m)^#{1,6} \S", base) is not None
    text = base.rstrip() + ("\n\n" if base.strip() else "") + ("## " if atx else "") + HEADING
    entries = []
    for k, item in enumerate(read, start=1):
        heading = f"Supplementary file {k} ({item.name})"
        described = " ".join(filter(None, (item.label, item.caption)))
        text += "\n\n" + ("### " if atx else "") + heading + (f": {described}" if described else "")
        body = shortened_tables(Path(item.text_path).read_text(encoding="utf-8")).strip()
        cut = len(body) > room
        body = body[:max(room, 0)] + ("\n[... the rest of this file's text is left out]" if cut else "")
        room -= len(body)
        start = len(text) + 2
        text += "\n\n" + body
        table_ids = []
        for n, payload in enumerate(item.tables, start=1):
            table = ExtractedTable.from_dict(payload)
            table.table_id = f"s{k}-{table.table_id}"
            table.table_number = None
            table.metadata = {**table.metadata, "supplement": k,
                              "table_label": table_label(table.caption, k, n, own_numbers=len(numbering) <= 1)}
            origin = f"Supplementary file {k} ({item.name})"
            table.caption = f"{origin}: {table.caption}" if table.caption else origin
            content.tables.append(table)
            table_ids.append(table.table_id)
        entries.append({"index": k, "route": item.route, "url": item.url, "name": item.name,
                        "label": item.label, "caption": item.caption, "read_by": item.read_by,
                        "span": [start, len(text)], "tables": table_ids, **({"truncated": True} if cut else {})})
    text += "\n"
    out_dir.mkdir(parents=True, exist_ok=True)
    joined = out_dir / f"{content.source.value}+supplements.txt"
    joined.write_text(text, encoding="utf-8")
    content.base_text_path = base_path
    content.full_text_path = joined
    content.supplements = entries
    return content


def _table_row(line: str) -> bool:
    """A line of a table as a supplement's text writes one: a Word table's Markdown row
    (Docling) or a sheet's tab-separated row."""
    return line.startswith("|") or "\t" in line


def shortened_tables(text: str, rows: int = APPENDED_TABLE_ROWS) -> str:
    """`text` with each table in it cut after its first row and `rows` more, a line saying how
    many are left out in place of the rest, the padding of its Markdown cells squashed and its
    `|---|` rule dropped. A table is a run of table lines (`_table_row`); the rest of the text is
    left as it is."""
    out: List[str] = []
    run: List[str] = []

    def close() -> None:
        if len(run) > rows + 1:
            out.extend(run[:rows + 1])
            out.append(f"[... {len(run) - rows - 1:,} more rows of this table are left out of the "
                       "text; the table keeps them]")
        else:
            out.extend(run)
        run.clear()

    for line in text.split("\n"):
        if not _table_row(line):
            close()
            out.append(line)
        elif not _PIPE_RULE.match(line.strip()):
            run.append(re.sub(r" {2,}", " ", line).rstrip() if line.startswith("|") else line)
    close()
    return "\n".join(out)


def joined_dir(settings, article_id: str) -> Path:
    """Where an article's text joined with its supplements is written: the run's own cache."""
    return Path(settings.cache_root) / "supplements" / article_id / "joined"


def supplement_spans(payload: Optional[Dict[str, Any]]) -> List[Tuple[int, int]]:
    """The spans of an extraction payload's appended supplements, in the joined text."""
    return [tuple(s["span"]) for s in (payload or {}).get("supplements") or [] if s.get("span")]


def payload_supplements(payload: Optional[Dict[str, Any]]) -> List[Supplement]:
    return [Supplement.from_dict(s) for s in (payload or {}).get("supplements") or []]


__all__ = [
    "HEADING", "READ", "READ_VERSION", "ROUTES", "Supplement", "TRANSIENT", "append", "elsevier_items",
    "is_transient", "link_name", "not_the_file", "public_host", "read_digest", "zip_too_big",
    "fetch", "file_name", "from_pdf_content", "is_supplementary", "jats_items", "passed_over",
    "payload_supplements", "pmc_items", "publisher_items", "read", "springer_items",
    "supplement_spans", "table_label", "without_supplements",
]

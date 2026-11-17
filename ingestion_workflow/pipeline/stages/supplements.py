"""Each article's supplementary material, fetched and read before the article is extracted.

`extract` appends what this stage read to the extraction it makes, so every later stage
sees the article's text and tables with its supplements' (`services.supplements`).
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import DownloadResult, DownloadSource

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when what is listed, fetched or read changes.
SUPPLEMENTS_VERSION = 1

#: Downloads whose stored files list an article's supplements, by route.
ROUTE_DOWNLOADS = {"elsevier": ("elsevier",), "springer": ("springer",), "publisher": ("ace",)}
#: Downloads that hold the JATS the `pmc` route matches the package against, first first.
JATS_DOWNLOADS = ("pmc", "europepmc", "pubget", "springer")

#: Why an article has nothing to ask.
NO_ROUTE = "no route to supplementary material"

#: Requests in flight at once, over every host.
FETCH_THREADS = 4
#: Articles whose routes are listed at once: PMC's bucket and Elsevier's CDN answer in well
#: under a second, and listing one article at a time held each batch of 500 for 8 minutes.
LIST_THREADS = 8

#: Most processes reading Word files and sheets at once: each holds a Docling converter.
READ_WORKERS = 8


def routes_for(ref: ArticleRef, ok: Iterable[str], wanted: Sequence[str]) -> List[str]:
    """The routes an article can be asked by, in `ROUTES` order: `ok` is the sources it holds
    an OK download from."""
    ok = set(ok)
    out = []
    for route in wanted:
        if route == "pmc":
            if ref.identifier.pmcid:
                out.append(route)
        elif ok & set(ROUTE_DOWNLOADS[route]):
            out.append(route)
    return out


class SupplementsStage:
    name = "supplements"
    requires = "download"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, routes: Sequence[str], pmcid: Optional[str]) -> str:
        return fingerprint("supplements", SUPPLEMENTS_VERSION, *routes,
                           (pmcid or "") if "pmc" in routes else "")

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        wanted = list(self.settings.supplement_routes)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            downloads = upstream.get(ref.id, {})
            if not any(a.status is Status.OK for a in downloads.values()):
                plan.blocked += 1
                continue
            routes = routes_for(ref, [s for s, a in downloads.items() if a.status is Status.OK], wanted)
            if not routes:
                plan.block(NO_ROUTE)
                continue
            fp = self.fingerprint_for(routes, ref.identifier.pmcid)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        import requests

        from ingestion_workflow.services import supplements as sp

        downloads = ctx.catalog.artifacts([w.article_id for w in works], "download")
        root = Path(self.settings.cache_root) / "supplements"
        session = requests.Session()
        listed: Dict[str, List[sp.Supplement]] = {}
        failed: Dict[str, str] = {}
        # The catalog is read here, its connection being this thread's; only the network
        # is asked from the pool.
        stored = {w.article_id: _stored(ctx, downloads.get(w.article_id, {})) for w in works}

        def ask(work):
            try:
                return work.article_id, self._list(work, stored[work.article_id], session), None
            except Exception as exc:  # noqa: BLE001 - that article is retried under backoff
                return work.article_id, None, exc

        with ThreadPoolExecutor(LIST_THREADS) as pool:
            for article_id, items, exc in pool.map(ask, works):
                if exc is None:
                    listed[article_id] = items
                else:
                    logger.warning("supplements: listing failed for %s: %s", article_id, exc)
                    failed[article_id] = f"listing failed: {type(exc).__name__}: {exc}"

        jobs: List[Tuple[str, int, sp.Supplement]] = []
        for work in works:
            for index, item in enumerate(listed.get(work.article_id, []), start=1):
                item.reason = item.reason or sp.passed_over(item)
                if item.reason is None:
                    jobs.append((work.article_id, index, item))

        def get(job):
            article_id, index, item = job
            dest = root / article_id / "files" / sp.file_name(index, item)
            try:
                sp.fetch(item, dest, session, max_bytes=self.settings.supplement_max_mb * 2**20)
            except sp.TransientFetch as exc:
                return article_id, str(exc)
            return article_id, None

        with ThreadPoolExecutor(FETCH_THREADS) as pool:
            for article_id, error in pool.map(get, jobs):
                if error:
                    failed.setdefault(article_id, f"fetch failed: {error}")

        for work in works:
            _dedupe(listed.get(work.article_id, []))
        for article_id, error in self._read(root, [(a, i, s) for a, i, s in jobs
                                                   if s.path and s.reason is None]).items():
            failed.setdefault(article_id, error)

        for work in works:
            if work.article_id in failed:
                yield Outcome.failure(work.article_id, self.name, "", failed[work.article_id],
                                      fingerprint=work.fingerprint)
                continue
            items = listed[work.article_id]
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"supplements": [s.to_dict() for s in items]},
                summary=_summary(items),
            )

    def _list(self, work: Work, stored: Dict[str, DownloadResult], session):
        """Every item the article's routes list, the first route to list a file name first.

        `stored` is the article's OK downloads (`_stored`)."""
        from lxml import etree

        from ingestion_workflow.services import supplements as sp

        routes = routes_for(work.ref, stored, self.settings.supplement_routes)
        items: List[sp.Supplement] = []
        for route in routes:
            if route == "pmc":
                items += _pmc(work.ref.identifier.pmcid, _jats(stored), session, _cloud_record(stored))
            elif route == "elsevier":
                xml = _file(stored.get("elsevier"), lambda p: p.name.startswith("content.") and p.suffix == ".xml")
                if xml is not None:
                    items += sp.elsevier_items(etree.parse(str(xml), _parser()).getroot())
            elif route == "springer":
                jats = _file(stored.get("springer"), lambda p: p.name == "article.xml")
                if jats is not None:
                    items += sp.springer_items(etree.parse(str(jats), _parser()).getroot(),
                                               work.ref.identifier.doi or "")
            elif route == "publisher":
                page = _file(stored.get("ace"), lambda p: p.suffix in (".html", ".htm"))
                if page is not None:
                    items += sp.publisher_items(page.read_bytes(), _page_address(stored.get("ace")))
        seen, out = set(), []
        for item in items:
            key = item.name.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _read(self, root: Path, jobs) -> Dict[str, str]:
        """Read each fetched file: PDFs together through `pdf_engines`, the rest in a pool of
        `max_workers` processes (Docling reads a Word file in seconds, on the CPU).

        Returns why each article whose PDFs no engine could run on is to be tried again."""
        pdfs = [job for job in jobs if job[2].extension == "pdf"]
        rest = [job for job in jobs if job[2].extension != "pdf"]
        if rest:
            self._read_rest(root, rest)
        return self._read_pdfs(root, pdfs) if pdfs else {}

    def _read_rest(self, root: Path, jobs) -> None:
        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor

        from ingestion_workflow.services import supplements as sp

        tasks = [(item.to_dict(), str(root / article_id / "read" / f"{index:02d}"))
                 for article_id, index, item in jobs]
        workers = min(max(1, int(getattr(self.settings, "max_workers", 1) or 1)), READ_WORKERS, len(tasks))
        if workers == 1:
            answers = [_read_one(task) for task in tasks]
        else:
            # spawn: Docling's torch must not be forked from a parent that has started threads
            with ProcessPoolExecutor(workers, mp_context=multiprocessing.get_context("spawn")) as pool:
                answers = list(pool.map(_read_one, tasks, chunksize=4))
        for (_, _, item), answer in zip(jobs, answers):
            read = sp.Supplement.from_dict(answer)
            item.text_path, item.tables, item.read_by, item.reason = (
                read.text_path, read.tables, read.read_by, read.reason)

    def _read_pdfs(self, root: Path, pdfs) -> Dict[str, str]:
        from ingestion_workflow.extractors.pdf_extractor import PdfExtractor
        from ingestion_workflow.extractors.utils import build_downloaded_file
        from ingestion_workflow.models import FileType, Identifier
        from ingestion_workflow.services import supplements as sp

        settings = self.settings.model_copy(update={"data_root": root})
        results = []
        for article_id, index, item in pdfs:
            # Named so each PDF is read into its own directory (`safe_hash_stem` of the slug).
            identifier = Identifier(doi=f"10.0000/supplement.{article_id}.{index:02d}")
            results.append(DownloadResult(
                identifier=identifier, source=DownloadSource.PDF, success=True,
                files=[build_downloaded_file(Path(item.path), FileType.PDF, source=DownloadSource.PDF)]))
        try:
            contents = PdfExtractor(settings).extract(results)
        except Exception as exc:  # noqa: BLE001 - no engine could run: tried again later
            logger.error("supplements: PDF engines could not run: %s", exc)
            return {a: f"PDF engines could not run: {type(exc).__name__}: {exc}" for a, _, _ in pdfs}
        for (_, _, item), content in zip(pdfs, contents):
            sp.from_pdf_content(item, content)
        return {}


def _parser():
    from lxml import etree

    return etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False, no_network=True)


def _read_one(task):
    """Read one non-PDF file in a pool worker: its item as a dict, after reading."""
    from ingestion_workflow.services import supplements as sp

    payload, out_dir = task
    item = sp.Supplement.from_dict(payload)
    sp.read(item, Path(out_dir))
    return item.to_dict()


def _stored(ctx: Context, downloads: Dict[str, Artifact]) -> Dict[str, DownloadResult]:
    """The article's OK downloads, read from the catalog."""
    out = {}
    for source, artifact in downloads.items():
        if artifact.status is Status.OK:
            payload = ctx.payload(artifact)
            if payload:
                out[source] = DownloadResult.from_dict(payload)
    return out


def _file(download: Optional[DownloadResult], wanted) -> Optional[Path]:
    if download is None:
        return None
    return next((f.file_path for f in download.files if wanted(f.file_path) and f.file_path.is_file()), None)


def _jats(stored: Dict[str, DownloadResult]) -> Optional[Path]:
    for source in JATS_DOWNLOADS:
        found = _file(stored.get(source), lambda p: p.name == "article.xml")
        if found is not None:
            return found
    return None


def _page_address(download: Optional[DownloadResult]) -> Optional[str]:
    """The address the publisher page was fetched from, when the download recorded one."""
    for attempt in reversed((download.url_attempts if download else None) or []):
        url = attempt.get("url")
        if isinstance(url, str) and url.startswith("http"):
            return url
    return None


def _cloud_record(stored: Dict[str, DownloadResult]) -> Optional[dict]:
    """The Cloud Service's record of the article (`pmc_cloud.json`) its PMC download kept."""
    path = _file(stored.get("pmc"), lambda p: p.name == "pmc_cloud.json")
    if path is None:
        return None
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return record if record.get("version") and record.get("media_urls") is not None else None


def _pmc(pmcid: str, jats_path: Optional[Path], session, cloud: Optional[dict] = None):
    """The article's newest package in PMC's Cloud Service, matched against its JATS: the
    downloaded one, else the package's own. A `pmc_cloud.json` its PMC download kept names
    the package and its files, so the bucket is not listed."""
    import re

    from lxml import etree

    from ingestion_workflow.services import supplements as sp
    from ingestion_workflow.utils.pmcid import normalize_pmcid

    number = normalize_pmcid(pmcid).removeprefix("PMC")
    if cloud is not None and jats_path is not None:
        prefix = f"PMC{number}.{cloud['version']}/"
        keys = [prefix + unquote(urlparse(u.replace("s3://", "https://", 1)).path).rsplit("/", 1)[-1]
                for u in cloud["media_urls"]]
        root = etree.parse(str(jats_path), _parser()).getroot()
        return sp.pmc_items(root, keys, prefix)
    listing = session.get(sp.PMC_CLOUD, params={"list-type": "2", "prefix": f"PMC{number}."},
                          timeout=60, headers={"User-Agent": sp.USER_AGENT})
    listing.raise_for_status()
    keys = re.findall(r"<Key>([^<]+)</Key>", listing.text)
    versions = sorted({int(m.group(1)) for k in keys if (m := re.match(rf"PMC{number}\.(\d+)/", k))})
    if not versions:
        return []
    prefix = f"PMC{number}.{versions[-1]}/"
    if jats_path is not None:
        root = etree.parse(str(jats_path), _parser()).getroot()
    else:
        stem = f"{prefix}PMC{number}.{versions[-1]}.xml"
        if stem not in keys:
            return []
        answer = session.get(f"{sp.PMC_CLOUD}/{stem}", timeout=120, headers={"User-Agent": sp.USER_AGENT})
        answer.raise_for_status()
        root = etree.fromstring(answer.content, _parser())
    return sp.pmc_items(root, keys, prefix)


def _dedupe(items) -> None:
    """A file fetched twice under two names (PMC's copy of an Elsevier mmc) is read once."""
    seen = {}
    for item in items:
        if item.sha256 and item.reason is None:
            if item.sha256 in seen:
                item.reason = f"same file as {seen[item.sha256]}"
            else:
                seen[item.sha256] = item.name


def _summary(items) -> Dict:
    passed: Dict[str, int] = {}
    routes: Dict[str, int] = {}
    for item in items:
        routes[item.route] = routes.get(item.route, 0) + 1
        if item.reason:
            reason = item.reason.split(":", 1)[0] if item.reason.startswith(("unreadable", "not read")) \
                else item.reason
            passed[reason] = passed.get(reason, 0) + 1
    return {
        "listed": len(items),
        "fetched": sum(bool(i.path) for i in items),
        "read": sum(i.read for i in items),
        "tables": sum(len(i.tables) for i in items if i.read),
        "routes": routes,
        **({"passed_over": passed} if passed else {}),
    }

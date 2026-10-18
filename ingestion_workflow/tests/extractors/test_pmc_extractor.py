"""PMC (NCBI efetch) and Europe PMC sources: fetch, pubget layout, pubget extraction."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from lxml import etree

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import pmc_extractor
from ingestion_workflow.extractors.pmc_extractor import RESTRICTED, EuropePmcExtractor, PmcExtractor
from ingestion_workflow.models import DownloadSource, Identifier, Identifiers
from ingestion_workflow.pipeline.stages.download import _is_permanent, build_extractor

ARTICLE = (
    Path(__file__).resolve().parents[1]
    / "data" / "test_pubget" / "articles" / "1a4" / "pmcid_9056519" / "article.xml"
)


def article(pmcid: str, *, body: bool = True, id_type: str = "pmc") -> etree._Element:
    """The fixture article under another PMCID, optionally without its body."""
    root = copy.deepcopy(etree.parse(str(ARTICLE)).getroot())
    meta = root.find("front/article-meta")
    for tag in meta.findall("article-id"):
        if tag.get("pub-id-type") in ("pmc", "pmcid"):
            meta.remove(tag)
    tag = etree.SubElement(meta, "article-id", {"pub-id-type": id_type})
    tag.text = pmcid if id_type == "pmc" else f"PMC{pmcid}"
    if not body:
        for b in root.findall("body"):
            root.remove(b)
    return root


class Response:
    def __init__(self, status: int, content: bytes = b""):
        self.status_code = status
        self.content = content

    @property
    def text(self) -> str:
        return self.content.decode()

    def json(self):
        return json.loads(self.content)


def listing(*keys: str) -> Response:
    """An S3 ListObjectsV2 answer naming these keys."""
    body = "".join(f"<Contents><Key>{k}</Key></Contents>" for k in keys)
    return Response(200, f"<ListBucketResult>{body}</ListBucketResult>".encode())


class Cloud:
    """Session.get stand-in for PMC's Cloud Service bucket: {key: bytes}."""

    def __init__(self, objects=None, fail=False):
        self.objects = objects or {}
        self.fail = fail
        self.calls = 0

    def __call__(self, url, params=None, timeout=None):  # not bound: an instance is no descriptor
        self.calls += 1
        if self.fail:
            raise pmc_extractor.requests.ConnectionError("bucket unreachable")
        if params:  # a listing
            prefix = params["prefix"]
            return listing(*[k for k in self.objects if k.startswith(prefix)])
        key = url.split(".amazonaws.com/")[1]
        return Response(200, self.objects[key]) if key in self.objects else Response(404)


@pytest.fixture
def settings(tmp_path):
    return Settings(cache_root=tmp_path / "cache", data_root=tmp_path / "data", max_workers=1)


def ids(*pmcids):
    return Identifiers([Identifier(pmcid=f"PMC{p}") for p in pmcids])


def test_pmc_keeps_full_text_and_reports_restricted(settings, monkeypatch):
    batch = etree.Element("pmc-articleset")
    batch.append(article("9056519"))
    batch.append(article("1111111", body=False))
    sent = {}

    def post(self, url, data=None, timeout=None):
        sent.update(data)
        return Response(200, etree.tostring(batch))

    cloud = Cloud()
    monkeypatch.setattr(pmc_extractor.requests.Session, "post", post)
    monkeypatch.setattr(pmc_extractor.requests.Session, "get", cloud)
    monkeypatch.setattr(pmc_extractor.time, "sleep", lambda s: None)
    extractor = PmcExtractor(settings=settings)
    results = extractor.download(ids("9056519", "1111111", "2222222"))
    assert cloud.calls == 3  # the bucket was asked first, for each article

    assert sent["db"] == "pmc" and sent["id"] == "1111111,2222222,9056519"
    full, restricted, missing = results
    assert full.success and full.source is DownloadSource.PMC
    assert {f.file_path.name for f in full.files} >= {"article.xml", "tables.xml"}
    assert full.files[0].file_path.is_relative_to(settings.cache_root / "pmc")
    assert not restricted.success and restricted.error_message == RESTRICTED
    assert _is_permanent(restricted.error_message)
    assert not missing.success and "no article" in missing.error_message

    content = extractor.extract([full])[0]
    assert content.source is DownloadSource.PMC
    assert content.error_message is None
    assert len(content.tables) == 3 and content.has_coordinates
    assert content.full_text_path.read_text(encoding="utf-8")


def test_europepmc_fetches_each_article_and_marks_not_open_access(settings, monkeypatch):
    pages = {
        "9056519": Response(200, etree.tostring(article("9056519", id_type="pmcid"))),
        "3333333": Response(500, b"<errorBean>not open access</errorBean>"),
    }

    def get(self, url, timeout=None):
        return pages[url.split("/PMC")[1].split("/")[0]]

    monkeypatch.setattr(pmc_extractor.requests.Session, "get", get)
    extractor = EuropePmcExtractor(settings=settings)
    full, closed = extractor.download(ids("9056519", "3333333"))

    assert full.success and full.source is DownloadSource.EUROPEPMC
    assert not closed.success and _is_permanent(closed.error_message)
    content = extractor.extract([full])[0]
    assert content.source is DownloadSource.EUROPEPMC and len(content.tables) == 3


def test_a_source_outage_fails_the_batch_without_raising(settings, monkeypatch):
    def post(self, url, data=None, timeout=None):
        raise pmc_extractor.requests.ConnectionError("down")

    monkeypatch.setattr(pmc_extractor.requests.Session, "post", post)
    monkeypatch.setattr(pmc_extractor.requests.Session, "get", Cloud(fail=True))
    monkeypatch.setattr(pmc_extractor.time, "sleep", lambda s: None)
    (result,) = PmcExtractor(settings=settings).download(ids("9056519"))
    assert not result.success and "request failed" in result.error_message
    assert not _is_permanent(result.error_message)


@pytest.mark.parametrize("source", [DownloadSource.PMC, DownloadSource.EUROPEPMC])
def test_sources_are_registered_and_need_a_pmcid(settings, source):
    extractor = build_extractor(source, settings)
    assert extractor.SOURCE is source and extractor._SUPPORTED_IDS == {"pmcid"}


def test_pubget_keeps_its_own_source(settings):
    from ingestion_workflow.extractors.pubget_extractor import PubgetExtractor

    assert PubgetExtractor(settings=settings).SOURCE is DownloadSource.PUBGET


def test_pmc_reads_the_cloud_service_first_and_keeps_its_record(settings, monkeypatch):
    """The newest version the bucket holds wins, its licence record is kept, efetch is not asked."""
    record = {"pmcid": "PMC9056519", "version": 2, "is_manuscript": True, "license_code": "TDM",
              "is_pmc_openaccess": False, "is_retracted": False}
    cloud = Cloud({
        "PMC9056519.1/PMC9056519.1.xml": etree.tostring(article("9056519", body=False)),
        "PMC9056519.2/PMC9056519.2.json": json.dumps(record).encode(),
        "PMC9056519.2/PMC9056519.2.xml": etree.tostring(article("9056519", id_type="pmcid")),
    })
    asked = []
    monkeypatch.setattr(pmc_extractor.requests.Session, "get", cloud)
    monkeypatch.setattr(pmc_extractor.requests.Session, "post",
                        lambda self, url, data=None, timeout=None: asked.append(data) or Response(200, b"<pmc-articleset/>"))
    monkeypatch.setattr(pmc_extractor.time, "sleep", lambda s: None)
    extractor = PmcExtractor(settings=settings)
    (result,) = extractor.download(ids("9056519"))

    assert result.success and not asked
    kept = [f for f in result.files if f.file_path.name == PmcExtractor.CLOUD_METADATA]
    assert kept and json.loads(kept[0].file_path.read_text())["license_code"] == "TDM"
    content = extractor.extract([result])[0]
    assert content.source is DownloadSource.PMC and len(content.tables) == 3


def test_pmc_falls_back_to_efetch_for_what_the_bucket_lacks(settings, monkeypatch):
    batch = etree.Element("pmc-articleset")
    batch.append(article("9056519"))
    asked = []

    def post(self, url, data=None, timeout=None):
        asked.append(data["id"])
        return Response(200, etree.tostring(batch))

    monkeypatch.setattr(pmc_extractor.requests.Session, "get", Cloud())
    monkeypatch.setattr(pmc_extractor.requests.Session, "post", post)
    monkeypatch.setattr(pmc_extractor.time, "sleep", lambda s: None)
    (result,) = PmcExtractor(settings=settings).download(ids("9056519"))
    assert result.success and asked == ["9056519"]
    assert not any(f.file_path.name == PmcExtractor.CLOUD_METADATA for f in result.files)


def test_the_default_order_names_every_source_once_with_pmc_first():
    order = Settings().download_sources
    assert sorted(order) == sorted(s.value for s in DownloadSource) and len(order) == len(set(order))
    assert order[:3] == ["pmc", "europepmc", "pubget"]


@pytest.mark.vcr
def test_a_malformed_article_id_fails_only_its_own_article(settings, caplog):
    """Recorded from the Cloud Service and efetch for two articles PMC holds without a body,
    then edited: in the efetch answer, PMC10002769's `article-id pub-id-type="pmcid"` was
    changed to `PMC10002769.x`."""
    restricted, malformed = PmcExtractor(settings=settings).download(ids("10002673", "10002769"))
    assert "Cloud Service lookup failed" not in caplog.text
    assert restricted.error_message == RESTRICTED
    assert not malformed.success
    assert malformed.error_message == (
        "PMC returned this article under a malformed id (not a PMCID: 'PMC10002769.x')"
    )


@pytest.mark.vcr
def test_articles_under_malformed_ids_share_one_reason(settings, caplog):
    """Recorded as above for three articles, then edited: the `pmcid` article-ids of
    PMC10002769 and PMC10008056 were changed to `PMC10002769.x` and `PMC10008056.x`."""
    extractor = PmcExtractor(settings=settings)
    restricted, *malformed = extractor.download(ids("10002673", "10002769", "10008056"))
    assert "Cloud Service lookup failed" not in caplog.text
    assert restricted.error_message == RESTRICTED
    assert [m.error_message for m in malformed] == [
        "PMC returned no article for this PMCID; this batch's 2 missing PMCIDs share 2 "
        "articles under malformed ids (not a PMCID: 'PMC10008056.x'; "
        "not a PMCID: 'PMC10002769.x')"
    ] * 2


@pytest.mark.vcr
def test_europepmc_does_not_read_the_id_of_the_article_it_returns(settings):
    """Recorded from Europe PMC, then edited: the article's `pmcid` article-id was changed
    from PMC3407125 to PMC3407125.x. The article is filed under the PMCID asked for."""
    [result] = EuropePmcExtractor(settings=settings).download(ids("3407125"))
    assert result.success and result.source is DownloadSource.EUROPEPMC

import json
from types import SimpleNamespace

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.models import (
    DownloadSource,
    FileType,
    Identifier,
    Identifiers,
)


def _make_fake_article(*, doi=None, pmid=None, identifier_type="doi"):
    identifier_value = doi if identifier_type == "doi" else pmid
    metadata = {
        "identifier_lookup": {"doi": doi, "pmid": pmid},
        "identifier_type": identifier_type,
        "identifier": identifier_value,
    }
    return SimpleNamespace(
        payload=b"<xml></xml>",
        content_type="application/xml",
        format="xml",
        metadata=metadata,
    )


def test_elsevier_download_mixed_success(monkeypatch, tmp_path):
    identifiers = Identifiers(
        [
            Identifier(doi="10.1234/failed"),
            Identifier(pmid="123456"),
            Identifier(doi="10.1234/success"),
        ]
    )

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
    )

    extractor = ElsevierExtractor(settings=settings)

    def fake_run_download(self, records, progress_hook=None):
        assert len(records) == 3
        articles = [
            _make_fake_article(pmid="123456", identifier_type="pmid"),
            _make_fake_article(doi="10.1234/success", identifier_type="doi"),
        ]
        if progress_hook:
            for _ in articles:
                progress_hook(1)
        return articles

    monkeypatch.setattr(ElsevierExtractor, "_run_download", fake_run_download)

    results = extractor.download(identifiers)

    assert len(results) == 3

    first, second, third = results

    assert first.identifier is identifiers.identifiers[0]
    assert not first.success
    assert first.source is DownloadSource.ELSEVIER
    assert not first.files
    assert first.error_message

    assert second.identifier is identifiers.identifiers[1]
    assert second.success
    assert second.source is DownloadSource.ELSEVIER
    assert second.files
    metadata_file = next(file for file in second.files if file.file_type is FileType.JSON)
    metadata = json.loads(metadata_file.file_path.read_text(encoding="utf-8"))
    assert metadata["lookup_type"] == "pmid"
    assert metadata["lookup_value"] == "123456"
    assert metadata["identifier_slug"] == second.identifier.slug

    assert third.identifier is identifiers.identifiers[2]
    assert third.success
    metadata_file = next(file for file in third.files if file.file_type is FileType.JSON)
    metadata = json.loads(metadata_file.file_path.read_text(encoding="utf-8"))
    assert metadata["lookup_type"] == "doi"
    assert metadata["lookup_value"] == "10.1234/success"
    assert metadata["identifier_slug"] == third.identifier.slug


NO_TABLE_CAPTION = "Greater insula activation in patients (x = -34, y = 16, z = -6; p < 0.05)."

NO_TABLE_ARTICLE = f"""<?xml version="1.0" encoding="utf-8"?>
<full-text-retrieval-response xmlns="http://www.elsevier.com/xml/svapi/article/dtd"
  xmlns:ce="http://www.elsevier.com/xml/common/dtd" xmlns:ja="http://www.elsevier.com/xml/ja/dtd"
  xmlns:dc="http://purl.org/dc/elements/1.1/">
<coredata><dc:title>T</dc:title></coredata>
<originalText><ja:article><ja:body><ce:sections>
<ce:section id="s1"><ce:section-title>Results</ce:section-title>
<ce:para>Patients showed more activity.</ce:para>
<ce:figure id="f1"><ce:label>Fig. 1</ce:label><ce:caption><ce:simple-para>{NO_TABLE_CAPTION.replace("<", "&lt;")}</ce:simple-para></ce:caption></ce:figure>
</ce:section></ce:sections></ja:body></ja:article></originalText>
</full-text-retrieval-response>"""


def test_an_article_without_tables_keeps_its_figure_caption_spans(tmp_path):
    from ingestion_workflow.extractors.elsevier_extractor import _extract_elsevier_article
    from ingestion_workflow.extractors.utils import build_downloaded_file
    from ingestion_workflow.models import DownloadResult
    from ingestion_workflow.pipeline.stages.passages import find_passages

    download_dir = tmp_path / "download"
    download_dir.mkdir()
    (download_dir / "content.xml").write_text(NO_TABLE_ARTICLE, encoding="utf-8")
    (download_dir / "metadata.json").write_text("{}", encoding="utf-8")
    source = DownloadSource.ELSEVIER
    download = DownloadResult(
        identifier=Identifier(pmid="18602375"),
        source=source,
        success=True,
        files=[
            build_downloaded_file(download_dir / "content.xml", FileType.XML, source=source),
            build_downloaded_file(download_dir / "metadata.json", FileType.JSON, source=source),
        ],
    )

    extraction = _extract_elsevier_article(download, tmp_path / "extracted")

    assert extraction.error_message == "Elsevier found no tables in the article."
    assert extraction.tables == []
    text = extraction.full_text_path.read_text(encoding="utf-8")
    assert [text[slice(*c["span"])] for c in extraction.figure_captions] == [
        f"Fig. 1 {NO_TABLE_CAPTION}"
    ]

    how, passages, _, _ = find_passages(
        {"text_path": str(extraction.full_text_path), "figure_captions": extraction.figure_captions}
    )
    assert [(h.x, h.y, h.z) for p in passages for h in p.hits] == [(-34.0, 16.0, -6.0)]
    assert all(p.from_legend for p in passages)

"""Naming articles from PDFs that arrived without identifiers.

The DOI cases are the ones measured on the first shared folder (532 PDFs).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.cli.main import app
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadSource, Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.download import DownloadStage
from ingestion_workflow.pipeline.stages.extract import ExtractStage
from ingestion_workflow.services import local_pdfs
from ingestion_workflow.services.local_pdfs import (
    LocalPdf,
    attach,
    authors_named,
    clean_doi,
    confirm_dois,
    dois_in,
    find_pdfs,
    match_titles,
    rank_dois,
    read_pdf,
    title_from_filename,
)
from typer.testing import CliRunner


def make_pdf(lines, title=None) -> bytes:
    """A one-page PDF with a real text layer, small enough to build by hand."""
    escaped = [line.replace("(", r"\(").replace(")", r"\)") for line in lines]
    content = "BT /F1 10 Tf 50 750 Td " + " ".join(f"({t}) Tj 0 -14 Td" for t in escaped) + " ET"
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        "/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        f"<< /Length {len(content)} >>\nstream\n{content}\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    if title:
        objects.append(f"<< /Title ({title}) >>")
    out = b"%PDF-1.4\n"
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n{body}\nendobj\n".encode()
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets)
    info = f" /Info {len(objects)} 0 R" if title else ""
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R{info} >>\n"
        f"startxref\n{xref}\n%%EOF\n"
    ).encode()
    return out


# -- reading DOIs --------------------------------------------------------------


@pytest.mark.parametrize(
    "printed,expected",
    [
        ("10.1016/j.drugalcdep.2011.01.009.", "10.1016/j.drugalcdep.2011.01.009"),
        ("10.1038/tp.2013.62;", "10.1038/tp.2013.62"),
        ("10.1007/s00213-013-3342-z)", "10.1007/s00213-013-3342-z"),
        ("10.1093/alcalc/agac029,", "10.1093/alcalc/agac029"),
        ("10.1016/S0924-9338(02)00676-4", "10.1016/S0924-9338(02)00676-4"),
        ("10.1038/s41386-", None),
        ("10.1172/jci", None),
    ],
)
def test_sentence_punctuation_is_not_part_of_a_doi(printed, expected):
    assert clean_doi(printed) == expected


def test_a_doi_broken_across_lines_is_rejoined():
    assert dois_in("https://doi.org/10.1172/jci.\r\ninsight.182331.\r\nA randomized") == [
        "10.1172/jci.insight.182331"
    ]
    assert dois_in("www.pnas.org/cgi/doi/10.1073/pnas. 1011455107 PNAS") == [
        "10.1073/pnas.1011455107"
    ]
    assert dois_in("10.1038/s41386-\n018-0019-7") == ["10.1038/s41386-018-0019-7"]


def test_a_finished_doi_is_not_joined_to_the_next_line():
    assert dois_in("doi:10.1016/j.x.2011.01.009.\n2011 Elsevier") == ["10.1016/j.x.2011.01.009"]


def test_the_article_doi_wins_over_its_supplement_and_landing_path():
    assert rank_dois(["10.1037/a0022228.supp", "10.1037/a0022228"]) == ["10.1037/a0022228"]
    assert rank_dois(["10.1093/ijnp/pyz044", "10.1093/ijnp/pyz044/5550917"]) == [
        "10.1093/ijnp/pyz044"
    ]
    assert rank_dois(["10.1073/pnas.2201074119/-/DCSupplemental", "10.1073/pnas.2201074119"]) == [
        "10.1073/pnas.2201074119"
    ]


def test_data_dois_and_case_variants_are_not_candidates():
    assert rank_dois(["10.5061/dryad.x1", "10.1371/journal.pone.0229187"]) == [
        "10.1371/journal.pone.0229187"
    ]
    assert rank_dois(["10.1111/ejn.14574", "10.1111/EJN.14574"]) == ["10.1111/ejn.14574"]


def _printed(*candidates) -> LocalPdf:
    return LocalPdf(
        path=Path("a.pdf"),
        sha256="0",
        doi=rank_dois(candidates)[0],
        found_by="page text",
        candidates=list(candidates),
    )


def test_a_printed_typo_gives_way_to_the_registered_doi():
    pdf = _printed("10.9758/cpn.2018.16.1.290", "10.9758/cpn.2018.16.3.290")
    confirm_dois([pdf], lambda doi: doi == "10.9758/cpn.2018.16.3.290")
    assert pdf.doi == "10.9758/cpn.2018.16.3.290"


def test_a_landing_path_is_cut_until_the_doi_is_registered():
    pdf = _printed("10.1093/brain/awae369/7895715")
    confirm_dois([pdf], lambda doi: doi == "10.1093/brain/awae369")
    assert pdf.doi == "10.1093/brain/awae369"


def test_an_unregistered_doi_leaves_the_pdf_to_the_title_search():
    pdf = _printed("10.1037/a0025l")
    confirm_dois([pdf], lambda doi: False)
    assert pdf.doi is None and pdf.found_by is None


def test_when_the_registry_cannot_answer_the_first_candidate_stands():
    pdf = _printed("10.9758/cpn.2018.16.1.290", "10.9758/cpn.2018.16.3.290")
    confirm_dois([pdf], lambda doi: None)
    assert pdf.doi == "10.9758/cpn.2018.16.1.290"


# -- reading files -------------------------------------------------------------


def test_pdfs_are_found_by_content_not_extension(tmp_path):
    (tmp_path / "2023").mkdir()
    (tmp_path / "2023" / "437 The Role of Unawareness._").write_bytes(make_pdf(["x"]))
    (tmp_path / "notes.txt").write_text("not a pdf")
    (tmp_path / "265. supp.docx").write_bytes(b"PK\x03\x04")
    assert [p.name for p in find_pdfs(tmp_path)] == ["437 The Role of Unawareness._"]


def test_the_metadata_doi_is_preferred_and_the_page_kept_for_verification(tmp_path):
    path = tmp_path / "145.Ames SL,2014, Neural correlates of a Go NoGo task.pdf"
    path.write_bytes(
        make_pdf(
            ["Neural correlates", "S. L. Ames", "doi:10.1038/s41386-"],
            title="doi:10.1038/s41386-018-0019-7",
        )
    )
    pdf = read_pdf(path)
    assert pdf.doi == "10.1038/s41386-018-0019-7"
    assert pdf.found_by == "pdf metadata"
    assert pdf.year == 2014
    assert "Ames" in pdf.front


@pytest.mark.parametrize(
    "name,title",
    [
        (
            "145.Ames SL,2014, Neural correlates of a Go NoGo task with alcohol stimuli..pdf",
            "Neural correlates of a Go NoGo task with alcohol stimuli",
        ),
        (
            "3. Garavan H, Cue- nduced cocaine craving neuroanatomical specificity.pdf",
            "Cue- nduced cocaine craving neuroanatomical specificity",
        ),
        (
            "478. Ding, et al. Spatial Craving Patterns in Marijuana Users..pdf",
            "Spatial Craving Patterns in Marijuana Users",
        ),
        (
            "84.Beck A,2012, Effect of brain structure on relapse in patients. - Copy.pdf",
            "Effect of brain structure on relapse in patients",
        ),
        (
            "Sex-specific neural activation to stress and alcohol cues, links.pdf",
            "Sex-specific neural activation to stress and alcohol cues, links",
        ),
        ("341.1. Supp.pdf", None),
    ],
)
def test_titles_come_out_of_author_year_filenames(name, title):
    assert title_from_filename(Path(name)) == title


# -- title search --------------------------------------------------------------


def _candidate(title, authors, year=2001, doi="10.1176/appi.ajp.158.7.1075"):
    return (
        Identifier(doi=doi),
        ArticleMetadata(
            title=title, authors=[Author(name=a) for a in authors], publication_year=year
        ),
    )


def _unprinted(**fields) -> LocalPdf:
    base = dict(
        path=Path("5.Shneider F,2001, Subcortical correlates of craving.pdf"),
        sha256="0",
        title="Subcortical correlates of craving in recently abstinent alcoholic patients",
        year=2001,
        front="Subcortical Correlates of Craving\nFrank Schneider, Ute Habel, Michael Wagner",
    )
    base.update(fields)
    return LocalPdf(**base)


TITLE = "Subcortical Correlates of Craving in Recently Abstinent Alcoholic Patients"


def test_a_title_match_is_kept_when_its_first_author_is_on_the_page():
    pdf = _unprinted()
    match_titles([pdf], [("openalex", lambda t: [_candidate(TITLE, ["Frank Schneider"])])])
    assert pdf.doi == "10.1176/appi.ajp.158.7.1075"
    assert pdf.found_by == "title match (openalex)"


def test_a_title_match_by_someone_else_is_rejected_and_the_next_provider_asked():
    pdf = _unprinted()
    wrong = [_candidate(TITLE, ["Pat Nobody"], doi="10.1/wrong")]
    right = [_candidate(TITLE, ["Frank Schneider"])]
    match_titles([pdf], [("openalex", lambda t: wrong), ("pubmed", lambda t: right)])
    assert pdf.doi == "10.1176/appi.ajp.158.7.1075"
    assert pdf.found_by == "title match (pubmed)"


def test_a_match_from_another_year_or_with_another_title_is_rejected():
    pdf = _unprinted()
    match_titles(
        [pdf],
        [
            ("a", lambda t: [_candidate(TITLE, ["Frank Schneider"], year=2009)]),
            ("b", lambda t: [_candidate("Craving: an erratum", ["Frank Schneider"])]),
        ],
    )
    assert pdf.identifier is None


def test_a_failing_provider_does_not_stop_the_search():
    def broken(title):
        raise RuntimeError("403")

    pdf = _unprinted()
    match_titles(
        [pdf], [("s2", broken), ("openalex", lambda t: [_candidate(TITLE, ["F. Schneider"])])]
    )
    assert pdf.found_by == "title match (openalex)"


def test_authors_are_matched_by_surname_or_two_of_five():
    assert authors_named(["Andreas Heinz"], "A. HEINZ, M. Siessmeier")
    assert authors_named(["Klaus Hönig"], "K. Honig")  # accents folded
    # A provider that reversed an East Asian name still has its co-authors.
    assert authors_named(["Guangheng Dong", "Wang Ziliang", "Hu Yanbo"], "Dong G, Wang Z, Hu Y")
    assert not authors_named(["Pat Nobody", "Jane Doe"], "Frank Schneider")


# -- attaching -----------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        catalog_root=tmp_path / "catalog",
        ns_pond_root=tmp_path / "pond",
    )


def _local(tmp_path, name, doi=None, supplement=False) -> LocalPdf:
    path = tmp_path / "shared" / name
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(make_pdf([name]))
    pdf = read_pdf(path)
    pdf.doi, pdf.supplement = doi, supplement
    return pdf


def test_an_attached_pdf_is_the_download_and_goes_straight_to_extraction(tmp_path, settings):
    pdf = _local(tmp_path, "a.pdf", doi="10.1/a")
    stage = DownloadStage(settings)
    fp = stage.fingerprint_for(DownloadSource.PDF)
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(doi="10.1/a"))
        assert attach(catalog, [pdf], fp, tmp_path / "pdfcache") == {pdf.path: "attached"}

        ctx = Context(settings, catalog)
        downloads = catalog.artifacts([ref.id], "download")
        assert stage.plan(ctx, [ref], downloads, {}).fresh == 1

        plan = ExtractStage(settings).plan(ctx, [ref], {}, downloads)
        assert [work.source for work in plan.pending] == ["pdf"]

        payload = catalog.payload(downloads[ref.id]["pdf"])
        copied = Path(payload["files"][0]["file_path"])
        assert copied.read_bytes() == pdf.path.read_bytes()
        assert copied.parent == tmp_path / "pdfcache"


def test_in_place_the_catalog_points_at_the_file_itself(tmp_path, settings):
    pdf = _local(tmp_path, "a.pdf", doi="10.1/a")
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(doi="10.1/a"))
        attach(catalog, [pdf], "fp")
        payload = catalog.payload(catalog.artifact(ref.id, "download", "pdf"))
    assert Path(payload["files"][0]["file_path"]) == pdf.path.resolve()


def test_an_existing_download_supplements_and_repeats_are_left_alone(tmp_path, settings):
    first = _local(tmp_path, "a.pdf", doi="10.1/a")
    again = _local(tmp_path, "a - Copy.pdf", doi="10.1/A")
    supp = _local(tmp_path, "a supp.pdf", doi="10.1/a", supplement=True)
    other = _local(tmp_path, "b.pdf", doi="10.1/b")
    nothing = _local(tmp_path, "c.pdf")
    with Catalog.open(settings.catalog_root) as catalog:
        a = catalog.register(Identifier(doi="10.1/a", pmid="1"))
        catalog.register(Identifier(doi="10.1/A", pmid="1"))
        b = catalog.register(Identifier(doi="10.1/b"))
        catalog.record(
            [Outcome(article_id=b.id, stage="download", source="pubget", status=Status.OK)]
        )
        status = attach(catalog, [first, again, supp, other, nothing], "fp", tmp_path / "c")
        assert catalog.artifact(b.id, "download", "pdf") is None
        assert catalog.artifact(a.id, "download", "pdf").ok

    assert status == {
        first.path: "attached",
        again.path: "duplicate of a.pdf",
        supp.path: "supplement",
        other.path: "already downloaded",
        nothing.path: "unresolved",
    }


def test_a_pdf_is_attached_over_the_sources_it_should_outrank(tmp_path, settings):
    over_ace = _local(tmp_path, "a.pdf", doi="10.1/a")
    beside_pubget = _local(tmp_path, "b.pdf", doi="10.1/b")
    with Catalog.open(settings.catalog_root) as catalog:
        a = catalog.register(Identifier(doi="10.1/a"))
        b = catalog.register(Identifier(doi="10.1/b"))
        catalog.record([
            Outcome(article_id=a.id, stage="download", source="ace", status=Status.OK),
            Outcome(article_id=b.id, stage="download", source="ace", status=Status.OK),
            Outcome(article_id=b.id, stage="download", source="pubget", status=Status.OK),
        ])
        status = attach(catalog, [over_ace, beside_pubget], "fp", prefer_over=["ace"])
        assert catalog.artifact(a.id, "download", "pdf").ok
        assert catalog.artifact(b.id, "download", "pdf") is None

    assert status == {over_ace.path: "attached over ace", beside_pubget.path: "already downloaded"}


# -- the command ---------------------------------------------------------------


def test_add_pdfs_registers_attaches_and_reports(tmp_path, monkeypatch):
    config = tmp_path / "settings.yaml"
    config.write_text(
        json.dumps(
            {
                "data_root": str(tmp_path / "data"),
                "cache_root": str(tmp_path / "cache"),
                "catalog_root": str(tmp_path / "catalog"),
                "ns_pond_root": str(tmp_path / "pond"),
                "log_to_file": False,
                "show_progress": False,
            }
        )
    )
    shared = tmp_path / "Originals"
    shared.mkdir()
    (shared / "1.Smith J,2010, Cue reactivity in smokers.pdf").write_bytes(
        make_pdf(["Cue reactivity in smokers", "doi:10.1016/j.drugalcdep.2011.01.009."])
    )
    (shared / "1.1 Supp.pdf").write_bytes(make_pdf(["Supplementary Materials"]))
    monkeypatch.setattr(local_pdfs, "doi_registered", lambda session: lambda doi: True)
    monkeypatch.setattr(local_pdfs, "title_searchers", lambda settings: [])

    result = CliRunner().invoke(
        app, ["add", "--pdfs", str(shared), "--no-enrich", "--config", str(config)]
    )
    assert result.exit_code == 0, result.output
    assert "attached 1" in result.output and "supplement 1" in result.output

    manifest = tmp_path / "data" / "manifests" / "Originals.jsonl"
    assert [json.loads(line)["doi"] for line in manifest.read_text().splitlines()] == [
        "10.1016/j.drugalcdep.2011.01.009"
    ]
    report = (tmp_path / "data" / "manifests" / "Originals.tsv").read_text().splitlines()
    assert report[0].startswith("status\tfound_by")
    assert len(report) == 3

    shown = CliRunner().invoke(
        app, ["show", "10.1016/j.drugalcdep.2011.01.009", "--config", str(config)]
    )
    assert "download/pdf" in shown.output and "ok" in shown.output

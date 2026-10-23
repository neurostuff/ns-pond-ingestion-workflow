"""The catalog holds an article's ids in three buckets: the version of record's, a
preprint's, and every other id of the paper's. Real ids throughout: each case is one
the catalog holds."""

import logging
import sqlite3

import pytest
from ingestion_workflow.catalog import Catalog, IdentityError
from ingestion_workflow.models.ids import Identifier


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        yield cat


def _aliases(root):
    with sqlite3.connect(f"file:{root / 'catalog.sqlite'}?mode=ro", uri=True) as conn:
        return {(r[0], r[1], r[2]) for r in conn.execute(
            "SELECT article_id, kind, value FROM aliases")}


def _as_version_three(root, *aliases):
    """A catalog as schema 3 held it: a preprint's DOI beside the journal's as `doi`, the other
    DOI and the duplicate records as `alternate_doi`, `duplicate_pmid`, `duplicate_pmcid`."""
    with Catalog.open(root) as cat:
        for article, kind, value in aliases:
            cat._conn.execute(
                "INSERT OR IGNORE INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                (article,))
            cat._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)",
                              (kind, value, article))
        cat._conn.execute("UPDATE meta SET value='3' WHERE key='schema_version'")


#: One article of each case the migration decides.
V3 = [
    # vrr7x2ygtjdt-like: a paper, its PubMed record of the preprint beside its own, and the
    # bioRxiv preprint's DOI
    ("paper", "pmid", "38345852"), ("paper", "doi", "10.7554/elife.92563"),
    ("paper", "doi", "10.1101/2023.09.27.559792"), ("paper", "preprint_pmid", "37808793"),
    ("paper", "preprint_pmcid", "PMC10592780"),
    # known only by its bioRxiv DOI
    ("only", "doi", "10.1101/2025.05.13.653896"),
    # a Cold Spring Harbor journal, whose DOI shares bioRxiv's prefix
    ("journal", "doi", "10.1101/lm.022889.111"),
    # eLife: the base DOI and its versions share one prefix with the preprint's
    ("elife", "pmid", "42663321"), ("elife", "doi", "10.7554/elife.106070"),
    ("elife", "alternate_doi", "10.7554/elife.106070.1"),
    # a rekind in #141's plan made a preprint's DOI the paper's other DOI
    ("rekinded", "pmid", "40462995"), ("rekinded", "alternate_doi", "10.1101/2019.12.17.19014415"),
    ("elife", "doi", "10.1101/2025.02.05.636581"),
    # PubMed holds two records of one paper, and PMC one of them
    ("twice", "pmid", "32341955"), ("twice", "duplicate_pmid", "32341954"),
    ("twice", "pmcid", "PMC7181890"), ("twice", "duplicate_pmcid", "PMC7181891"),
    # Research Square versions are separately registered DOIs
    ("rs", "doi", "10.21203/rs.3.rs-2556690/v2"), ("rs", "doi", "10.21203/rs.3.rs-2556690/v1"),
    ("arxiv", "doi", "10.48550/arxiv.2111.08743"),
]


def test_each_id_is_held_in_the_bucket_it_belongs_to(tmp_path, caplog):
    root = tmp_path / "cat"
    _as_version_three(root, *V3)

    with caplog.at_level(logging.WARNING, logger="ingestion_workflow.catalog.store"):
        Catalog.open(root).close()

    assert _aliases(root) == {
        ("paper", "pmid", "38345852"), ("paper", "doi", "10.7554/elife.92563"),
        ("paper", "preprint_doi", "10.1101/2023.09.27.559792"),
        ("paper", "preprint_pmid", "37808793"), ("paper", "preprint_pmcid", "PMC10592780"),
        ("only", "preprint_doi", "10.1101/2025.05.13.653896"),
        ("journal", "doi", "10.1101/lm.022889.111"),
        ("elife", "pmid", "42663321"), ("elife", "doi", "10.7554/elife.106070"),
        ("elife", "alternative_doi", "10.7554/elife.106070.1"),
        ("rekinded", "pmid", "40462995"),
        ("rekinded", "preprint_doi", "10.1101/2019.12.17.19014415"),
        ("elife", "preprint_doi", "10.1101/2025.02.05.636581"),
        ("twice", "pmid", "32341955"), ("twice", "alternative_pmid", "32341954"),
        ("twice", "pmcid", "PMC7181890"), ("twice", "alternative_pmcid", "PMC7181891"),
        ("rs", "preprint_doi", "10.21203/rs.3.rs-2556690/v1"),
        ("rs", "preprint_doi", "10.21203/rs.3.rs-2556690/v2"),
        ("arxiv", "preprint_doi", "10.48550/arxiv.2111.08743"),
    }
    assert "7 as preprint_doi" in caplog.text
    assert "biorxiv 2, ssrn" not in caplog.text and "researchsquare 2" in caplog.text


def test_the_migration_backs_the_catalog_up_first_and_runs_once(tmp_path):
    root = tmp_path / "cat"
    _as_version_three(root, *V3)
    before = _aliases(root)

    Catalog.open(root).close()
    migrated = _aliases(root)
    Catalog.open(root).close()

    assert _aliases(root) == migrated
    assert sorted(p.name for p in root.glob("catalog.pre-*")) == ["catalog.pre-v3.sqlite"]
    with sqlite3.connect(root / "catalog.pre-v3.sqlite") as copy:
        assert {r for r in copy.execute("SELECT article_id, kind, value FROM aliases")} == before


def test_no_article_gains_or_loses_an_id_and_none_is_merged(tmp_path):
    root = tmp_path / "cat"
    _as_version_three(root, *V3)
    before = {(a, v) for a, _, v in _aliases(root)}

    Catalog.open(root).close()

    assert {(a, v) for a, _, v in _aliases(root)} == before
    with Catalog.open(root) as cat:
        assert {a: cat.live(a) for a, _, _ in V3} == dict.fromkeys({a for a, _, _ in V3}, True)
        # every id still finds its article, as whichever kind it is looked up
        for article, _, value in V3:
            kind = "pmcid" if value.startswith("PMC") else "pmid" if value.isdigit() else "doi"
            assert cat.holder(kind, value) == article


def test_what_each_article_presents_is_what_it_presented_before(tmp_path):
    root = tmp_path / "cat"
    _as_version_three(root, *V3)
    with Catalog.open(root) as cat:
        presented = {a: cat.identifier(a) for a in {a for a, _, _ in V3}}

    paper, only, elife, twice = (presented[a] for a in ("paper", "only", "elife", "twice"))
    assert (paper.pmid, paper.doi, paper.preprint_dois) == (
        "38345852", "10.7554/elife.92563", ("10.1101/2023.09.27.559792",))
    assert (paper.preprint_pmids, paper.preprint_pmcids) == (("37808793",), ("PMC10592780",))
    # known only by its preprint's DOI: it has no version of record's, is asked by the
    # preprint's as before, and is a preprint
    assert (only.doi, only.asking_doi, only.preprint_dois, only.publication_status) == (
        None, "10.1101/2025.05.13.653896", ("10.1101/2025.05.13.653896",), "preprint")
    assert (elife.doi, elife.alternative_dois, elife.preprint_dois) == (
        "10.7554/elife.106070", ("10.7554/elife.106070.1",), ("10.1101/2025.02.05.636581",))
    assert (twice.pmid, twice.alternative_pmids, twice.alternative_pmcids) == (
        "32341955", ("32341954",), ("PMC7181891",))
    assert twice.pmids == ["32341955", "32341954"]
    # a preprint's DOI #141 held as the paper's other DOI is the preprint's, not an alternative
    assert (presented["rekinded"].alternative_dois, presented["rekinded"].preprint_dois) == (
        (), ("10.1101/2019.12.17.19014415",))
    assert presented["journal"].doi == "10.1101/lm.022889.111"
    assert presented["journal"].preprint_dois == ()
    assert presented["rs"].preprint_dois == (
        "10.21203/rs.3.rs-2556690/v1", "10.21203/rs.3.rs-2556690/v2")


def test_a_failed_migration_leaves_the_catalog_as_it_was(tmp_path, monkeypatch):
    root = tmp_path / "cat"
    _as_version_three(root, *V3)
    before = _aliases(root)

    def broken(self, conn):
        conn.execute("UPDATE aliases SET kind='preprint_doi' WHERE kind='doi'")
        raise RuntimeError("disk full")

    monkeypatch.setattr(Catalog, "_bucket_aliases", broken)
    with pytest.raises(RuntimeError, match="disk full"):
        Catalog.open(root)

    assert _aliases(root) == before
    with sqlite3.connect(root / "catalog.sqlite") as conn:
        assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone() == ("3",)


# -- registering ------------------------------------------------------------------------

def test_a_paper_known_only_by_its_preprint_is_one_article_found_by_that_doi(catalog):
    ref = catalog.register(Identifier(doi="10.1101/2025.05.13.653896"))

    assert catalog.aliases(ref.id, "preprint_doi") == ["10.1101/2025.05.13.653896"]
    assert catalog.aliases(ref.id, "doi") == []
    again = catalog.register(Identifier(preprint_dois=["10.1101/2025.05.13.653896"]))
    assert again.id == ref.id == catalog.resolve(Identifier(doi="10.1101/2025.05.13.653896")).id
    assert catalog.count_articles() == 1


def test_a_preprint_later_found_to_have_a_journal_version_is_still_one_article(catalog):
    """dpl6phhniqbj: the bioRxiv preprint first, its journal DOI and PMID later."""
    preprint = catalog.register(Identifier(doi="10.1101/2020.03.25.008318"))

    ref = catalog.register(Identifier(
        pmid="32410303", doi="10.1002/hbm.25137", preprint_dois=["10.1101/2020.03.25.008318"]))

    assert ref.id == preprint.id and catalog.count_articles() == 1
    held = catalog.identifier(ref.id)
    assert (held.pmid, held.doi, held.preprint_dois) == (
        "32410303", "10.1002/hbm.25137", ("10.1101/2020.03.25.008318",))
    assert held.publication_status == "published"


def test_the_version_of_record_s_id_anchors_before_the_preprint_s(catalog):
    paper = catalog.register(Identifier(pmid="38345852"))
    other = catalog.register(Identifier(pmid="38345853", preprint_dois=["10.1101/2023.09.27.559792"]))

    ref = catalog.register(Identifier(
        pmid="38345852", preprint_dois=["10.1101/2023.09.27.559792"]))

    # the PMID names the paper; the preprint's DOI is another article's and is left there,
    # since fusing them would give one article two PMIDs
    assert ref.id == paper.id
    assert catalog.aliases(other.id, "preprint_doi") == ["10.1101/2023.09.27.559792"]
    assert catalog.aliases(paper.id, "preprint_doi") == []
    assert catalog.conflicts >= 1


def test_the_preprint_s_id_anchors_before_the_base_study(catalog):
    based = catalog.register(Identifier(pmid="38345852", neurostore="8YJSSriJx23a"))
    preprint = catalog.register(Identifier(pmid="38345853",
                                           preprint_dois=["10.1101/2023.09.27.559792"]))

    ref = catalog.register(Identifier(neurostore="8YJSSriJx23a",
                                      preprint_dois=["10.1101/2023.09.27.559792"]))

    # two PMIDs would be fused if both joined, so the record stays with the one it anchored on
    assert ref.id == preprint.id != based.id
    assert catalog.aliases(based.id, "neurostore") == ["8YJSSriJx23a"]


def test_a_record_mixing_two_preprint_papers_ids_fuses_neither(catalog):
    """36993482 and 37292981 are PubMed's records of two bioRxiv preprints. A record giving
    the first's DOI with the second's PMID is a fault in the record, not one paper."""
    a = catalog.register(Identifier(preprint_pmids=["36993482"],
                                    preprint_dois=["10.1101/2023.02.15.528738"]))
    b = catalog.register(Identifier(preprint_pmids=["37292981"],
                                    preprint_dois=["10.1101/2023.05.01.538938"]))

    ref = catalog.register(Identifier(preprint_pmids=["37292981"],
                                      preprint_dois=["10.1101/2023.02.15.528738"]))

    assert a.id != b.id and catalog.count_articles() == 2
    assert ref.id in (a.id, b.id)
    assert catalog.aliases(a.id, "preprint_pmid") == ["36993482"]
    assert catalog.aliases(b.id, "preprint_pmid") == ["37292981"]
    assert catalog.conflicts >= 1


def test_several_preprint_records_of_one_paper_are_held_together(catalog):
    """A paper on bioRxiv and Research Square has a PubMed record of each."""
    ref = catalog.register(Identifier(
        preprint_dois=["10.1101/2019.12.17.19014415", "10.21203/rs.3.rs-2556690/v2"],
        preprint_pmids=["34845462", "36000001"], preprint_pmcids=["PMC8629206", "PMC9000001"]))

    held = catalog.identifier(ref.id)
    assert held.preprint_pmids == ("34845462", "36000001")
    assert held.preprint_pmcids == ("PMC8629206", "PMC9000001")
    assert catalog.conflicts == 0
    from ingestion_workflow.catalog.invariants import suspects
    assert ref.id not in suspects(catalog)


def test_two_articles_one_paper_s_preprint_and_journal_ids_join_when_compatible(catalog):
    preprint = catalog.register(Identifier(preprint_dois=["10.1101/2023.10.21.563317"]))
    paper = catalog.register(Identifier(pmid="37961286", doi="10.1016/j.neuroimage.2023.120450"))

    ref = catalog.register(Identifier(
        doi="10.1016/j.neuroimage.2023.120450", preprint_dois=["10.1101/2023.10.21.563317"]))

    assert catalog.count_articles() == 1
    assert catalog.live(ref.id) and not (catalog.live(preprint.id) and catalog.live(paper.id))
    held = catalog.identifier(ref.id)
    assert (held.pmid, held.doi, held.preprint_dois) == (
        "37961286", "10.1016/j.neuroimage.2023.120450", ("10.1101/2023.10.21.563317",))


def test_an_id_held_in_any_bucket_finds_its_article_under_any_kind_of_it(catalog):
    twice = catalog.register(Identifier(pmid="32341955", alternative_pmids=["32341954"],
                                        preprint_pmids=["34845462"]))

    for pmid in ("32341955", "32341954", "34845462"):
        assert catalog.resolve(Identifier(pmid=pmid)).id == twice.id
        assert catalog.holder("pmid", pmid) == twice.id
    assert catalog.resolve(Identifier(alternative_pmids=["32341954"])).id == twice.id
    assert catalog.aliases(twice.id, "alternative_pmid") == ["32341954"]
    assert catalog.aliases(twice.id, "preprint_pmid") == ["34845462"]


def test_an_id_held_as_the_version_of_record_s_is_found_when_given_as_a_preprint_s(catalog):
    """btk5tlwqap5k holds 37808793, PubMed's record of a preprint, as its `pmid` until
    something files it as the preprint's; the preprint's id must still find it."""
    held = catalog.register(Identifier(pmid="37808793"))

    assert catalog.resolve(Identifier(preprint_pmids=["37808793"])).id == held.id
    ref = catalog.register(Identifier(doi="10.7554/elife.92563", preprint_pmids=["37808793"]))
    assert ref.id == held.id and catalog.count_articles() == 1
    assert catalog.aliases(held.id, "doi") == ["10.7554/elife.92563"]


def test_a_second_doi_row_is_held_but_is_no_alternative_until_a_repair_says_so(catalog):
    ref = catalog.register(Identifier(pmid="22848644", doi="10.1371/journal.pone.0041873"))
    catalog._conn.execute("INSERT INTO aliases VALUES('doi', '10.1002/hbm.25137', ?)", (ref.id,))

    held = catalog.identifier(ref.id)

    # the shorter is presented, as before. The other is not called the paper's: of 298
    # articles holding two, most are two works, and a parse of one refuses.
    assert held.doi == "10.1002/hbm.25137"
    assert held.alternative_dois == ()
    assert sorted(catalog.aliases(ref.id, "doi")) == [
        "10.1002/hbm.25137", "10.1371/journal.pone.0041873"]


def test_an_article_known_only_by_a_preprint_is_named_by_it(catalog):
    ref = catalog.register(Identifier(preprint_dois=["10.1101/2025.05.13.653896"]))

    # the DOI is the article's only name, so a repair may not take it away
    with pytest.raises(IdentityError, match="no pmcid, pmid, doi, preprint_doi"):
        catalog.remove_aliases([("preprint_doi", "10.1101/2025.05.13.653896", ref.id)])
    assert catalog.aliases(ref.id, "preprint_doi") == ["10.1101/2025.05.13.653896"]


# -- the header of what the catalog holds ------------------------------------------------

def _header_identifiers(identifier):
    from ingestion_workflow.models import ArticleMetadata
    from ingestion_workflow.services.paper_parse import article_identifiers

    return article_identifiers(identifier, ArticleMetadata(title="T"))


def test_an_elife_doi_the_catalog_holds_as_a_preprint_s_is_never_the_version_of_record_s(catalog):
    """10.7554/eLife.112197 shares its prefix with eLife's journal, so no pattern files it;
    once a repair or the Crossref lookup files it, the header must not repeat it as `doi`."""
    ref = catalog.register(Identifier(preprint_dois=["10.7554/elife.112197"]))
    held = catalog.identifier(ref.id)
    header = _header_identifiers(held)

    assert held.doi is None and held.preprint_dois == ("10.7554/elife.112197",)
    assert header.doi is None
    assert [(p.doi, p.server) for p in header.preprint_dois] == [
        ("10.7554/elife.112197", "elife")]
    assert header.publication_status == "preprint"


def test_a_preprint_only_article_is_headed_as_a_preprint_with_no_doi(catalog):
    ref = catalog.register(Identifier(doi="10.1101/2023.02.15.528738"))

    header = _header_identifiers(catalog.identifier(ref.id))

    assert header.doi is None
    assert [p.doi for p in header.preprint_dois] == ["10.1101/2023.02.15.528738"]
    assert header.publication_status == "preprint"


def test_crossref_and_openalex_are_asked_by_the_journal_s_doi_and_each_preprint_s(catalog):
    """dpl6phhniqbj: 1,581 articles hold a journal DOI beside a preprint's; an alternative
    is not asked."""
    from ingestion_workflow.pipeline.stages.metadata import held_dois

    ref = catalog.register(Identifier(
        doi="10.7554/elife.106070", preprint_dois=["10.1101/2025.02.05.636581"],
        alternative_dois=["10.7554/elife.106070.1"]))

    assert held_dois(catalog, ref.id) == ["10.7554/elife.106070", "10.1101/2025.02.05.636581"]

"""Identity repairs: each edit checks the catalog is as its caller says, and is all or nothing.

The identifiers are the repaired cases of the production catalog's conflicts.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, IdentityError, Outcome, Status
from ingestion_workflow.models.ids import Identifier


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        yield cat


def _aliases(catalog, article_id):
    return sorted(tuple(r) for r in catalog._conn.execute(
        "SELECT kind, value FROM aliases WHERE article_id=?", (article_id,)))


def _artifacts(catalog, article_id):
    return sorted((a.stage, a.source) for a in catalog.artifacts_for_article(article_id))


def _attempts(catalog, article_id):
    return sorted(tuple(r) for r in catalog._conn.execute(
        "SELECT stage, source FROM attempts WHERE article_id=?", (article_id,)))


def _hold(catalog, article_id, kind, value):
    """An alias written as the catalog took them before registration refused a second paper's."""
    catalog._conn.execute("INSERT INTO aliases VALUES (?, ?, ?)", (kind, value, article_id))


def _download(catalog, article_id, source, payload="x"):
    catalog.record([Outcome(article_id, "download", source, Status.OK, payload=payload),
                    Outcome(article_id, "extract", source, Status.OK, payload=payload)])


def _older(catalog, article_id):
    """Make the article the oldest, so a merge keeps it."""
    catalog._conn.execute("UPDATE articles SET created_at='2000-01-01' WHERE id=?", (article_id,))


def _snapshot(catalog):
    conn = catalog._conn
    return [sorted(tuple(r) for r in conn.execute(f"SELECT * FROM {table}"))
            for table in ("articles", "aliases", "artifacts", "attempts")]


# -- reassign_aliases ---------------------------------------------------------

def test_a_swap_of_pmids_is_one_call(catalog):
    # 4kzbqyqzbwwh held 21529758, the PMID of ylnafcdw4v2r's DOI, and the other way round.
    a = catalog.register(Identifier(doi="10.1016/j.ajog.2011.02.050", pmid="21529779")).id
    b = catalog.register(Identifier(doi="10.1016/j.ajog.2011.02.051", pmid="21529758")).id

    catalog.reassign_aliases([("pmid", "21529779", a, b), ("pmid", "21529758", b, a)])

    assert catalog.identifier(a).pmid == "21529758"
    assert catalog.identifier(b).pmid == "21529779"


def test_a_move_whose_alias_is_elsewhere_moves_nothing(catalog):
    a = catalog.register(Identifier(doi="10.1/a", pmid="11")).id
    b = catalog.register(Identifier(doi="10.1/b", pmid="22")).id
    c = catalog.register(Identifier(doi="10.1/c")).id
    before = _snapshot(catalog)

    with pytest.raises(IdentityError, match="22 is held by .*, not"):
        catalog.reassign_aliases([("pmid", "11", a, c), ("pmid", "22", a, c)])

    assert _snapshot(catalog) == before
    assert catalog.identifier(b).pmid == "22"


def test_a_move_may_not_leave_an_article_without_a_paper_id(catalog):
    a = catalog.register(Identifier(pmid="11")).id
    catalog.add_aliases([(a, "neurostore", "3Rob7cK8y6gY")])
    b = catalog.register(Identifier(doi="10.1/b")).id
    before = _snapshot(catalog)

    with pytest.raises(IdentityError, match="no pmcid, pmid, doi"):
        catalog.reassign_aliases([("pmid", "11", a, b)])
    assert _snapshot(catalog) == before


def test_a_move_that_would_leave_two_pmids_on_an_article_is_refused(catalog):
    # 2dv6myl5baiq kept 28283997 while the plan moved 28284050 to it.
    a = catalog.register(Identifier(doi="10.1016/j.eplepsyres.2017.02.019", pmid="28284050")).id
    b = catalog.register(Identifier(doi="10.1016/j.eplepsyres.2017.02.017", pmid="28283997")).id
    before = _snapshot(catalog)

    with pytest.raises(IdentityError, match=f"{b} would hold two pmids: 28283997, 28284050"):
        catalog.reassign_aliases([("pmid", "28284050", a, b)])
    assert _snapshot(catalog) == before


def test_a_move_into_an_article_merged_away_is_refused(catalog):
    a = catalog.register(Identifier(doi="10.1/a", pmid="11")).id
    b = catalog.register(Identifier(doi="10.1/b")).id
    c = catalog.register(Identifier(pmcid="PMC5")).id
    survivor = catalog.merge([b, c])
    retired = c if survivor == b else b

    with pytest.raises(IdentityError, match="not a live article"):
        catalog.reassign_aliases([("pmid", "11", a, retired)])


def test_an_alias_moved_twice_is_refused(catalog):
    a = catalog.register(Identifier(doi="10.1/a", pmid="11")).id
    b = catalog.register(Identifier(doi="10.1/b")).id
    with pytest.raises(IdentityError, match="moved twice"):
        catalog.reassign_aliases([("pmid", "11", a, b), ("pmid", "11", a, b)])


# -- remove_aliases -----------------------------------------------------------

def test_a_removed_alias_is_returned_in_the_shape_that_puts_it_back(catalog):
    # gzqigqvxj3op held its DOI twice, once with a trailing dot nobody registered.
    a = catalog.register(
        Identifier(pmid="31004629", doi="10.1016/j.neuropsychologia.2019.04.014")).id
    _hold(catalog, a, "doi", "10.1016/j.neuropsychologia.2019.04.014.")

    undo = catalog.remove_aliases([("doi", "10.1016/j.neuropsychologia.2019.04.014.", a)])

    assert ("doi", "10.1016/j.neuropsychologia.2019.04.014.") not in _aliases(catalog, a)
    assert ("doi", "10.1016/j.neuropsychologia.2019.04.014") in _aliases(catalog, a)
    assert undo == [(a, "doi", "10.1016/j.neuropsychologia.2019.04.014.")]


def test_a_removal_that_leaves_no_paper_id_is_refused(catalog):
    a = catalog.register(Identifier(pmid="28932410")).id
    with pytest.raises(IdentityError, match="no pmcid, pmid, doi"):
        catalog.remove_aliases([("pmid", "28932410", a)])
    assert catalog.identifier(a).pmid == "28932410"


def test_a_removal_from_the_wrong_article_is_refused(catalog):
    a = catalog.register(Identifier(doi="10.1/a", pmid="11")).id
    b = catalog.register(Identifier(doi="10.1/b")).id
    with pytest.raises(IdentityError, match="held by"):
        catalog.remove_aliases([("pmid", "11", b)])
    assert catalog.identifier(a).pmid == "11"


# -- split ----------------------------------------------------------------------

def test_a_split_takes_the_other_papers_ids_and_download(catalog):
    # og3a4u5wsy26 held a Biol Psychiatry paper and a Neurobiol Learn Mem one; its
    # Elsevier download was the second.
    a = catalog.register(Identifier(doi="10.1016/j.biopsych.2011.09.005", pmid="22015111")).id
    _hold(catalog, a, "doi", "10.1016/j.nlm.2011.06.006")
    _download(catalog, a, "elsevier")
    _download(catalog, a, "pubget")
    catalog.record([Outcome(a, "metadata", "", Status.OK, payload={"title": "t"})])

    new = catalog.split(a, [("doi", "10.1016/j.nlm.2011.06.006")],
                        [("download", "elsevier"), ("extract", "elsevier")])

    assert new != a
    assert catalog.resolve(Identifier(doi="10.1016/j.nlm.2011.06.006")).id == new
    assert catalog.resolve(Identifier(pmid="22015111")).id == a
    assert _artifacts(catalog, new) == [("download", "elsevier"), ("extract", "elsevier")]
    assert _attempts(catalog, new) == [("download", "elsevier"), ("extract", "elsevier")]
    assert _artifacts(catalog, a) == [
        ("download", "pubget"), ("extract", "pubget"), ("metadata", "")]
    assert catalog.live(new) and catalog.live(a)


def test_a_split_naming_an_artifact_the_article_lacks_writes_nothing(catalog):
    a = catalog.register(Identifier(doi="10.1016/j.bandl.2013.02.012", pmid="23587493")).id
    _hold(catalog, a, "doi", "10.1016/j.neuropsychologia.2013.01.006")
    before = _snapshot(catalog)

    with pytest.raises(IdentityError, match="no download/elsevier"):
        catalog.split(a, [("doi", "10.1016/j.neuropsychologia.2013.01.006")],
                      [("download", "elsevier")])
    assert _snapshot(catalog) == before


def test_a_split_must_leave_and_make_an_article_with_a_paper_id(catalog):
    a = catalog.register(Identifier(doi="10.1/a")).id
    catalog.add_aliases([(a, "neurostore", "3Rob7cK8y6gY")])
    with pytest.raises(IdentityError, match="new article would hold no"):
        catalog.split(a, [("neurostore", "3Rob7cK8y6gY")])
    with pytest.raises(IdentityError, match=f"{a} would be left with no"):
        catalog.split(a, [("doi", "10.1/a")])
    assert _aliases(catalog, a) == [("doi", "10.1/a"), ("neurostore", "3Rob7cK8y6gY")]


# -- move_artifacts / forget_artifacts ------------------------------------------

def test_a_download_of_another_paper_is_handed_to_that_papers_article(catalog):
    # mwni3etc4i7l held an Elsevier copy of 54xpppoekzpj's paper.
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    b = catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
    _download(catalog, a, "elsevier")

    catalog.move_artifacts(a, b, [("download", "elsevier"), ("extract", "elsevier")])

    assert _artifacts(catalog, a) == []
    assert _artifacts(catalog, b) == [("download", "elsevier"), ("extract", "elsevier")]
    assert _attempts(catalog, b) == [("download", "elsevier"), ("extract", "elsevier")]


def test_a_move_onto_an_artifact_the_target_holds_is_refused(catalog):
    a = catalog.register(Identifier(doi="10.1/a")).id
    b = catalog.register(Identifier(doi="10.1/b")).id
    _download(catalog, a, "elsevier", "a's copy")
    _download(catalog, b, "elsevier", "b's own")
    before = _snapshot(catalog)

    with pytest.raises(IdentityError, match="already holds a download/elsevier"):
        catalog.move_artifacts(a, b, [("download", "elsevier")])
    assert _snapshot(catalog) == before


def test_a_forgotten_artifact_is_planned_as_never_run(catalog):
    a = catalog.register(Identifier(doi="10.1016/0022-510x(96)00033-0")).id
    _download(catalog, a, "elsevier")
    _download(catalog, a, "pubget")

    forgotten = catalog.forget_artifacts(a, [("download", "elsevier"), ("extract", "elsevier")])

    assert [(f.stage, f.source) for f in forgotten] == [
        ("download", "elsevier"), ("extract", "elsevier")]
    assert catalog.artifact(a, "download", "elsevier") is None
    assert _attempts(catalog, a) == [("download", "pubget"), ("extract", "pubget")]
    assert catalog.payload(forgotten[0]) == "x"  # its blob stays


def test_forgetting_an_artifact_the_article_lacks_is_refused(catalog):
    a = catalog.register(Identifier(doi="10.1/a")).id
    _download(catalog, a, "pubget")
    with pytest.raises(IdentityError, match="no download/elsevier"):
        catalog.forget_artifacts(a, [("download", "pubget"), ("download", "elsevier")])
    assert _artifacts(catalog, a) == [("download", "pubget"), ("extract", "pubget")]


# -- merge ----------------------------------------------------------------------

def test_merge_joins_one_paper_held_twice(catalog):
    a = catalog.register(Identifier(doi="10.1016/j.jse.2011.01.043")).id
    b = catalog.register(Identifier(pmid="21524925")).id
    _download(catalog, b, "pubget")

    survivor = catalog.merge([a, b])

    assert catalog.resolve(Identifier(pmid="21524925")).id == survivor
    assert catalog.resolve(Identifier(doi="10.1016/j.jse.2011.01.043")).id == survivor
    assert _artifacts(catalog, survivor) == [("download", "pubget"), ("extract", "pubget")]


@pytest.mark.parametrize("first, second", [
    (Identifier(pmid="22015111"), Identifier(pmid="21704176")),
    (Identifier(doi="10.1016/j.biopsych.2011.09.005"),
     Identifier(doi="10.1016/j.nlm.2011.06.006")),
])
def test_merge_refuses_two_papers(catalog, first, second):
    a, b = catalog.register(first).id, catalog.register(second).id
    with pytest.raises(IdentityError, match="would hold two"):
        catalog.merge([a, b])
    assert catalog.live(a) and catalog.live(b)


def test_merge_keeps_a_preprint_beside_its_journal_doi(catalog):
    a = catalog.register(Identifier(doi="10.1101/2023.10.21.563317")).id
    b = catalog.register(Identifier(doi="10.1016/j.neuroimage.2024.120001")).id
    survivor = catalog.merge([a, b])
    assert catalog.aliases(survivor, "doi") == ["10.1016/j.neuroimage.2024.120001"]
    assert catalog.aliases(survivor, "preprint_doi") == ["10.1101/2023.10.21.563317"]


def test_merge_needs_two_live_articles(catalog):
    a = catalog.register(Identifier(doi="10.1/a")).id
    with pytest.raises(IdentityError, match="needs two"):
        catalog.merge([a, a])
    with pytest.raises(IdentityError, match="not a live article"):
        catalog.merge([a, "aaaaaaaaaaaa"])


# -- a preprint's PubMed record held as its own article ----------------------------

def _preprint_duplicate(catalog):
    # btk5tlwqap5k held only PMID 37808793, PubMed's Preprint record of vrr7x2ygtjdt's
    # eLife paper, and a PDF of it.
    paper = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852",
                                        pmcid="PMC10942577")).id
    _hold(catalog, paper, "preprint_doi", "10.1101/2023.09.27.559792")
    preprint = catalog.register(Identifier(pmid="37808793")).id
    _download(catalog, preprint, "pdf")
    return paper, preprint


def test_a_preprint_s_record_merges_into_its_paper_as_the_preprint_s(catalog):
    paper, preprint = _preprint_duplicate(catalog)

    survivor = catalog.merge([paper, preprint], rekind=[("pmid", "37808793")])

    assert catalog.aliases(survivor, "pmid") == ["38345852"]
    assert catalog.aliases(survivor, "preprint_pmid") == ["37808793"]
    identifier = catalog.identifier(survivor)
    assert identifier.pmid == "38345852"
    assert identifier.preprint_pmids == ("37808793",)
    assert _artifacts(catalog, survivor) == [("download", "pdf"), ("extract", "pdf")]


def test_the_preprint_s_pmid_still_finds_its_paper(catalog):
    paper, preprint = _preprint_duplicate(catalog)
    survivor = catalog.merge([paper, preprint], rekind=[("pmid", "37808793")])
    articles = catalog.count_articles()

    assert catalog.resolve(Identifier(pmid="37808793")).id == survivor
    assert catalog.register(Identifier(pmid="37808793")).id == survivor
    preprint_doi = "10.1101/2023.09.27.559792"
    assert catalog.register(Identifier(pmid="37808793", doi=preprint_doi)).id == survivor
    assert catalog.count_articles() == articles and catalog.conflicts == 0
    other = catalog.register(Identifier(doi="10.1016/j.neuroimage.2024.120001")).id
    held = f"pmid 37808793 is held by {survivor} as preprint_pmid"
    with pytest.raises(IdentityError, match=held):
        catalog.reassign_aliases([("pmid", "37808793", survivor, other)])


def test_merging_the_preprint_s_record_unmarked_is_refused(catalog):
    paper, preprint = _preprint_duplicate(catalog)
    with pytest.raises(IdentityError, match="would hold two"):
        catalog.merge([paper, preprint])


def test_a_pmid_and_a_pmcid_are_held_as_the_preprint_s(catalog):
    # a PMC copy of the preprint beside the version of record's, as in hxgigfhowfuh
    a = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852",
                                    pmcid="PMC10942577")).id
    _hold(catalog, a, "pmid", "37808793")
    _hold(catalog, a, "pmcid", "PMC10592780")

    catalog.rekind_aliases([("pmid", "37808793", a), ("pmcid", "PMC10592780", a)])

    held = catalog.identifier(a)
    assert (held.preprint_pmids, held.preprint_pmcids) == (("37808793",), ("PMC10592780",))
    assert catalog.resolve(Identifier(pmcid="PMC10592780")).id == a


def test_a_move_may_not_give_an_article_two_preprint_pmids(catalog):
    a = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id
    _hold(catalog, a, "preprint_pmid", "37808794")
    b = catalog.register(Identifier(doi="10.1101/2023.09.27.559792")).id
    _hold(catalog, b, "preprint_pmid", "37808793")
    with pytest.raises(IdentityError, match="two preprint_pmids"):
        catalog.reassign_aliases([("preprint_pmid", "37808793", b, a)])


def test_a_rekind_may_not_leave_an_article_without_a_paper_id(catalog):
    a = catalog.register(Identifier(pmid="37808793")).id
    with pytest.raises(IdentityError, match="no pmcid, pmid, doi"):
        catalog.rekind_aliases([("pmid", "37808793", a)])
    assert catalog.aliases(a, "pmid") == ["37808793"]


def test_a_rekind_may_not_give_an_article_two_preprint_pmids(catalog):
    a = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id
    _hold(catalog, a, "preprint_pmid", "37808794")
    _hold(catalog, a, "pmid", "37808793")
    with pytest.raises(IdentityError, match="two preprint_pmids"):
        catalog.rekind_aliases([("pmid", "37808793", a)])


def test_only_the_article_s_alias_is_rekinded(catalog):
    paper, preprint = _preprint_duplicate(catalog)
    with pytest.raises(IdentityError, match="pmid 37808793 is held by .*, not"):
        catalog.rekind_aliases([("pmid", "37808793", paper)])
    with pytest.raises(IdentityError, match="a neurostore is held as no other kind"):
        catalog.rekind_aliases([("neurostore", "x", paper)])
    assert catalog.aliases(preprint, "pmid") == ["37808793"]


def test_a_doi_held_as_an_alternate_still_finds_its_article(catalog):
    # mxp7s3fbegzb: PubMed's DOI for the paper, and the publisher's alias spelling
    a = catalog.register(Identifier(doi="10.1176/appi.neuropsych.21.3.307")).id
    _hold(catalog, a, "doi", "10.1176/jnp.2009.21.3.307")
    catalog.rekind_aliases([("doi", "10.1176/appi.neuropsych.21.3.307", a)])
    assert catalog.aliases(a, "doi") == ["10.1176/jnp.2009.21.3.307"]
    assert catalog.resolve(Identifier(doi="10.1176/appi.neuropsych.21.3.307")).id == a
    assert catalog.identifier(a).doi == "10.1176/jnp.2009.21.3.307"


def test_a_removed_preprint_pmid_is_put_back(catalog):
    a = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id
    _hold(catalog, a, "preprint_pmid", "37808793")
    undo = catalog.remove_aliases([("preprint_pmid", "37808793", a)])
    catalog.add_aliases(undo)
    assert catalog.aliases(a, "preprint_pmid") == ["37808793"]


def test_merge_refuses_two_base_studies_or_two_preprint_pmids(catalog):
    # bqb4eprbjxw7 came of two articles holding one base study each.
    a = catalog.register(
        Identifier(doi="10.1016/j.pnpbp.2026.111696", neurostore="8YJSSriJx23a")).id
    b = catalog.register(Identifier(pmid="41962621", neurostore="AkYZauDLLAS5")).id
    with pytest.raises(IdentityError, match="would hold two"):
        catalog.merge([a, b])
    c = catalog.register(Identifier(doi="10.7554/elife.92563")).id
    _hold(catalog, c, "preprint_pmid", "37808793")
    d = catalog.register(Identifier(pmcid="PMC10942577")).id
    _hold(catalog, d, "preprint_pmid", "37808794")
    with pytest.raises(IdentityError, match="would hold two"):
        catalog.merge([c, d])


def test_merge_carries_exclusions_and_attempts(catalog):
    a = catalog.register(Identifier(doi="10.1016/j.jse.2011.01.043")).id
    b = catalog.register(Identifier(pmid="21524925")).id
    catalog.record([Outcome(b, "download", "pubget", Status.FAILED, error="timeout")])
    catalog.exclude([(a, "t1", "kept", ""), (b, "t1", "same table", ""), (b, "t2", "own", "")])
    _older(catalog, a)

    survivor = catalog.merge([a, b])

    assert survivor == a
    assert _attempts(catalog, a) == [("download", "pubget")]
    assert _attempts(catalog, b) == []
    tables = sorted(tuple(r) for r in catalog._conn.execute(
        "SELECT article_id, table_id FROM exclusions"))
    assert tables == [(survivor, "t1"), (survivor, "t2")]


def test_a_split_takes_the_articles_once_merged_under_its_ids(catalog):
    # nyyxvhbyrkyn was registered by doi:10.1167/5.8.648 and merged into i3gw3nwrz4en.
    a = catalog.register(Identifier(doi="10.1016/j.visres.2005.01.001", pmid="15721630")).id
    seeded = catalog.register(Identifier(doi="10.1167/5.8.648")).id
    # merged as the catalog merged before it refused a second journal DOI
    catalog._conn.execute("UPDATE articles SET merged_into=? WHERE id=?", (a, seeded))
    catalog._conn.execute("UPDATE aliases SET article_id=? WHERE article_id=?", (a, seeded))

    new = catalog.split(a, [("doi", "10.1167/5.8.648")])

    assert catalog.current_id(seeded) == new
    assert catalog.current_id(a) == a


def test_a_merge_drops_a_second_pubmed_record_of_the_paper(catalog):
    # 5qxpg3twr524 held only 37401785, PubMed's second record of w5rawsklx347's paper
    # (35770780, both 10.1684/epd.2022.1433). A PubMed search finds both records, so
    # the second must still find the paper.
    paper = catalog.register(Identifier(doi="10.1684/epd.2022.1433", pmid="35770780")).id
    duplicate = catalog.register(Identifier(pmid="37401785")).id
    _download(catalog, duplicate, "pubget")
    with pytest.raises(IdentityError, match="would hold two"):
        catalog.merge([paper, duplicate])

    survivor = catalog.merge([paper, duplicate], drop=[("pmid", "37401785")])

    assert catalog.aliases(survivor, "pmid") == ["35770780"]
    assert catalog.aliases(survivor, "alternative_pmid") == ["37401785"]
    assert catalog.register(Identifier(pmid="37401785")).id == survivor
    assert catalog.identifier(survivor).pmids == ["35770780", "37401785"]
    assert _artifacts(catalog, survivor) == [("download", "pubget"), ("extract", "pubget")]


def test_a_merge_drops_only_the_merged_articles_aliases(catalog):
    paper = catalog.register(Identifier(doi="10.1684/epd.2022.1433", pmid="35770780")).id
    duplicate = catalog.register(Identifier(pmid="37401785")).id
    other = catalog.register(Identifier(pmid="21524925")).id
    before = _snapshot(catalog)
    held = f"pmid 21524925 is held by {other}, not by the merged"
    with pytest.raises(IdentityError, match=held):
        catalog.merge([paper, duplicate], drop=[("pmid", "37401785"), ("pmid", "21524925")])
    assert _snapshot(catalog) == before


# -- transaction ------------------------------------------------------------------

def test_a_transaction_commits_its_writes_together_or_not_at_all(catalog):
    a = catalog.register(Identifier(doi="10.1/a", pmid="11")).id
    b = catalog.register(Identifier(doi="10.1/b")).id
    before = _snapshot(catalog)

    with pytest.raises(RuntimeError):
        with catalog.transaction():
            catalog.reassign_aliases([("pmid", "11", a, b)])
            raise RuntimeError("stop")
    assert _snapshot(catalog) == before

    with catalog.transaction():
        catalog.reassign_aliases([("pmid", "11", a, b)])
        with pytest.raises(IdentityError):
            catalog.remove_aliases([("doi", "10.1/a", a)])  # a's last paper id
    assert catalog.identifier(b).pmid == "11"
    assert catalog.identifier(a).doi == "10.1/a"
    assert not catalog._conn.in_transaction


def test_a_rekind_may_not_hide_the_journal_doi_behind_a_preprint_s(catalog):
    # 7tsawnkwprg4: PubMed 35462035 is the NeuroImage paper; its medRxiv preprint's DOI
    # beside it must not become the DOI the article presents.
    a = catalog.register(Identifier(pmid="35462035", doi="10.1016/j.neuroimage.2022.119210")).id
    catalog.add_aliases([(a, "doi", "10.1101/2021.10.29.21265645")])
    before = _snapshot(catalog)
    with pytest.raises(IdentityError, match="only as an alternate"):
        catalog.rekind_aliases([("doi", "10.1016/j.neuroimage.2022.119210", a)])
    assert _snapshot(catalog) == before
    assert catalog.identifier(a).doi == "10.1016/j.neuroimage.2022.119210"


def test_a_merge_may_not_hide_the_journal_doi_behind_a_preprint_s(catalog):
    # 4zfbz7lcpfjz and f46p2xjhcx6a: the Epilepsia record's article and the bioRxiv one's
    paper = catalog.register(Identifier(pmid="42080316", doi="10.1002/epi.70276")).id
    preprint = catalog.register(Identifier(doi="10.64898/2025.12.17.694941")).id
    before = _snapshot(catalog)
    with pytest.raises(IdentityError, match="only as an alternate"):
        catalog.merge([paper, preprint], rekind=[("doi", "10.1002/epi.70276")])
    assert _snapshot(catalog) == before


def test_a_merge_drops_only_a_record_s_pmid_or_pmcid(catalog):
    paper = catalog.register(Identifier(doi="10.1684/epd.2022.1433", pmid="35770780")).id
    duplicate = catalog.register(Identifier(pmid="37401785", doi="10.1684/epd.2022.1434")).id
    with pytest.raises(IdentityError, match="a doi is no duplicate record's"):
        catalog.merge([paper, duplicate],
                      drop=[("pmid", "37401785"), ("doi", "10.1684/epd.2022.1434")])


def test_two_articles_holding_each_other_s_download_swap_them(catalog):
    # ml7z5xc7cvgo's ACE copy is 15465288.html, su74e7qsacoi's paper, and su74e7qsacoi's
    # is 15658005.html, ml7z5xc7cvgo's: each keeps a copy, the right one.
    a = catalog.register(Identifier(pmid="15658005")).id
    b = catalog.register(Identifier(pmid="15465288")).id
    _download(catalog, a, "ace", payload={"files": [{"file_path": "/ace/15465288.html"}]})
    _download(catalog, b, "ace", payload={"files": [{"file_path": "/ace/15658005.html"}]})
    catalog.swap_artifacts(a, b, [("download", "ace"), ("extract", "ace")])
    assert catalog.payload(catalog.artifact(a, "download", "ace"))["files"][0]["file_path"] == \
        "/ace/15658005.html"
    assert catalog.payload(catalog.artifact(b, "extract", "ace"))["files"][0]["file_path"] == \
        "/ace/15465288.html"
    before = _snapshot(catalog)
    with pytest.raises(IdentityError, match="holds no download/pubget"):
        catalog.swap_artifacts(a, b, [("download", "pubget")])
    assert _snapshot(catalog) == before


def test_a_split_may_not_leave_the_journal_doi_only_as_an_alternate(catalog):
    # ssohepivf7zd: Circ Cardiovasc Imaging's paper fused with an HTLV-1 paper whose DOI a
    # round had made an alternate; splitting the cardiology paper off left it no journal DOI
    a = catalog.register(Identifier(pmid="28292861", doi="10.1161/circimaging.116.005695")).id
    catalog.add_aliases([(a, "pmcid", "PMC5329009"),
                         (a, "alternative_doi", "10.3389/fnmol.2017.00052")])
    before = _snapshot(catalog)
    with pytest.raises(IdentityError, match="only as an alternate"):
        catalog.split(a, [("pmid", "28292861"), ("doi", "10.1161/circimaging.116.005695")])
    with pytest.raises(IdentityError, match="only as an alternate"):
        catalog.remove_aliases([("doi", "10.1161/circimaging.116.005695", a)])
    assert _snapshot(catalog) == before


def test_a_split_of_the_id_that_made_the_article_leaves_the_article_where_it_is(catalog):
    # og3a4u5wsy26's shape: registered by the Neurobiol Learn Mem DOI, holding Biol Psychiatry's
    a = catalog.register(Identifier(doi="10.1016/j.nlm.2011.06.006")).id
    catalog._conn.executemany("INSERT INTO aliases VALUES (?, ?, ?)", [
        ("pmid", "22015111", a), ("doi", "10.1016/j.biopsych.2011.09.005", a)])
    new = catalog.split(a, [("doi", "10.1016/j.nlm.2011.06.006")])
    assert catalog.live(a) and catalog.current_id(a) == a
    assert catalog.holder("doi", "10.1016/j.biopsych.2011.09.005") == a
    assert catalog.holder("doi", "10.1016/j.nlm.2011.06.006") == new


def test_a_split_leaves_an_article_merged_elsewhere_where_it_is(catalog):
    third = catalog.register(Identifier(pmid="21704176")).id
    seeded = catalog.register(Identifier(doi="10.1016/j.nlm.2011.06.006")).id
    _older(catalog, third)
    assert catalog.merge([third, seeded]) == third
    a = catalog.register(Identifier(pmid="22015111", doi="10.1016/j.biopsych.2011.09.005")).id
    catalog.reassign_aliases([("doi", "10.1016/j.nlm.2011.06.006", third, a)])
    catalog.split(a, [("doi", "10.1016/j.nlm.2011.06.006")])
    assert catalog.current_id(seeded) == third


def test_a_merge_drops_a_second_pmc_record_of_the_paper(catalog):
    paper = catalog.register(
        Identifier(doi="10.1684/epd.2022.1433", pmid="35770780", pmcid="PMC100")).id
    duplicate = catalog.register(Identifier(pmcid="PMC200")).id
    survivor = catalog.merge([paper, duplicate], drop=[("pmcid", "PMC200")])
    assert catalog.aliases(survivor, "alternative_pmcid") == ["PMC200"]
    assert catalog.register(Identifier(pmcid="PMC200")).id == survivor
    assert not (catalog.identifier(survivor).other_ids or {}).get("duplicate_pubmed_records")

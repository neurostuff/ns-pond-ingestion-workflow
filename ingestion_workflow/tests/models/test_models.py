import pytest
from ingestion_workflow.models.ids import Identifier, Identifiers


def test_identifier_normalizes_fields() -> None:
    identifier = Identifier(
        pmid=" 12345 ",
        doi="https://doi.org/10.1234/example ",
        pmcid=" 654321 ",
        other_ids={"arxiv": " 2101.00001 ", "empty": "   "},
    )

    assert identifier.pmid == "12345"
    assert identifier.doi == "10.1234/example"
    assert identifier.pmcid == "PMC654321"
    assert identifier.other_ids == {"arxiv": "2101.00001"}


#: One PMCID as providers and people write it: PubMed and Europe PMC prefixed,
#: Semantic Scholar bare, OpenAlex as an address on PMC's old website.
PMCID_SPELLINGS = [
    "PMC7212345",
    "pmc7212345",
    "Pmc7212345",
    "7212345",
    " 7212345 ",
    "PMC07212345",
    "https://www.ncbi.nlm.nih.gov/pmc/articles/7212345",
    "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7212345/",
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC7212345/",
]


@pytest.mark.parametrize("spelling", PMCID_SPELLINGS)
def test_every_spelling_of_a_pmcid_is_canonical_however_it_is_set(spelling) -> None:
    assert Identifier(pmcid=spelling).pmcid == "PMC7212345"

    enriched = Identifier(pmid="1")
    enriched.pmcid = spelling
    assert enriched.pmcid == "PMC7212345"
    assert enriched.slug == Identifier(pmid="1", pmcid="PMC7212345").slug

    mapped = Identifier(pmid="1")
    mapped["PMCID"] = spelling
    assert mapped.pmcid == "PMC7212345"

    held = Identifier(pmcid="PMC7212345")
    assert Identifiers([held]).lookup(spelling, key="pmcid") is held


@pytest.mark.parametrize("value", [
    "PMC", "PMC12a", "PMC 123", "PMID:7212345", "10.1/x", "PMC0", "000", "PMC\u0667\u0662\u0661",
])
def test_a_value_that_is_not_a_pmcid_is_refused(value) -> None:
    with pytest.raises(ValueError, match="not a PMCID"):
        Identifier(pmcid=value)


def test_a_blank_pmcid_is_no_pmcid() -> None:
    identifier = Identifier(pmcid="   ")
    assert identifier.pmcid is None
    identifier.pmcid = ""
    assert identifier.pmcid is None


def test_identifiers_set_index_and_lookup_multiple_keys() -> None:
    id_one = Identifier(pmid="1", doi="10.1234/foo")
    id_two = Identifier(pmid="2", pmcid="PMC123456")
    collection = Identifiers([id_one, id_two])

    collection.set_index("pmid", "doi")

    assert collection.lookup("1", key="pmid") is id_one
    assert collection.lookup("10.1234/foo", key="doi") is id_one

    assert collection.lookup("PMC123456", key="pmcid") is id_two
    assert collection.lookup("999", key="pmid") is None
    assert collection._index_keys == {"pmid", "doi", "pmcid"}


def test_identifiers_lookup_requires_configured_key() -> None:
    id_one = Identifier(pmid="1", doi="10.1234/foo")
    id_two = Identifier(doi="10.2345/bar")
    collection = Identifiers([id_one, id_two])
    collection.set_index("pmid", "doi")

    with pytest.raises(ValueError):
        collection.lookup("1")


def test_identifiers_lookup_normalizes_pmid_urls() -> None:
    identifier = Identifier(pmid="1")
    collection = Identifiers([identifier])
    collection.set_index("pmid")

    result = collection.lookup("https://pubmed.ncbi.nlm.nih.gov/1/", key="pmid")
    assert result is identifier


def test_identifiers_deduplicate_updates_indices() -> None:
    id_primary = Identifier(pmid="1", doi="10.1234/foo")
    id_duplicate = Identifier(pmid=" 1 ", doi="https://doi.org/10.1234/foo")
    collection = Identifiers([id_primary, id_duplicate])

    collection.set_index("pmid")
    collection.deduplicate()

    assert len(collection) == 1
    assert collection.lookup("1", key="pmid") is collection[0]


def test_identifiers_mutations_keep_indices_in_sync() -> None:
    collection = Identifiers()
    collection.set_index("pmid")

    first = Identifier(pmid="1")
    second = Identifier(pmid="2")
    third = Identifier(pmid="3")

    collection.append(first)
    assert collection.lookup("1", key="pmid") is first

    collection.extend([second])
    assert collection.lookup("2", key="pmid") is second

    collection.insert(0, third)
    assert collection.lookup("3", key="pmid") is third

    collection.remove(second)
    assert collection.lookup("2", key="pmid") is None

    popped = collection.pop()
    assert popped is first
    assert collection.lookup("1", key="pmid") is None

    collection.clear()
    assert len(collection) == 0
    assert collection.lookup("3", key="pmid") is None


def test_an_extracted_table_s_coordinates_carry_no_other_statistics_key():
    from ingestion_workflow.models.analysis import Coordinate
    from ingestion_workflow.models.extract import ExtractedTable

    table = ExtractedTable(table_id="t1", raw_content_path="t1.xml",
                           coordinates=[Coordinate(x=1, y=2, z=3, statistic_value=4.1,
                                                   statistic_type="T")])
    (written,) = table.to_dict()["coordinates"]
    assert "other_statistics" not in written
    assert ExtractedTable.from_dict(table.to_dict()).to_dict() == table.to_dict()


#: One PMID as providers and people write it: PubMed bare, a spreadsheet's zero-padded,
#: Europe PMC and OpenAlex prefixed or as an address.
PMID_SPELLINGS = [
    "37961286",
    " 37961286 ",
    "0037961286",
    "PMID:37961286",
    "pmid: 37961286",
    "PMID:0037961286",
    "https://pubmed.ncbi.nlm.nih.gov/37961286",
    "https://pubmed.ncbi.nlm.nih.gov/37961286/",
]


@pytest.mark.parametrize("spelling", PMID_SPELLINGS)
def test_every_spelling_of_a_pmid_is_canonical_however_it_is_set(spelling) -> None:
    assert Identifier(pmid=spelling).pmid == "37961286"

    enriched = Identifier(doi="10.1016/j.neuroimage.2023.120450")
    enriched.pmid = spelling
    assert enriched.pmid == "37961286"

    mapped = Identifier(doi="10.1016/j.neuroimage.2023.120450")
    mapped["PMID"] = spelling
    assert mapped.pmid == "37961286"

    held = Identifier(pmid="37961286")
    assert Identifiers([held]).lookup(spelling, key="pmid") is held
    assert Identifier(alternative_pmids=[spelling]).alternative_pmids == ("37961286",)


def test_what_is_not_a_spelling_of_a_pmid_is_kept_as_given() -> None:
    # The Neurosynth import's duplicate markers are catalog aliases; rewriting one would
    # give the duplicate its paper's id.
    assert Identifier(pmid="10196461-DUPLICATE").pmid == "10196461-DUPLICATE"
    assert Identifier(pmid="   ").pmid is None


def test_a_paper_has_its_ids_in_three_buckets() -> None:
    # 10.7554/eLife.106070: its journal ids, its bioRxiv preprint, and its other versions.
    identifier = Identifier(
        pmid="42663321", pmcid="PMC13524378", doi="10.7554/eLife.106070",
        preprint_dois=["10.1101/2025.02.05.636581"],
        alternative_dois=["10.7554/eLife.106070.2", "10.7554/eLife.106070.1",
                          "10.7554/eLife.106070.10"])
    assert identifier.doi == "10.7554/elife.106070"
    assert identifier.preprint_dois == ("10.1101/2025.02.05.636581",)
    # versions sort as numbers, so the latest is last
    assert identifier.alternative_dois == (
        "10.7554/elife.106070.1", "10.7554/elife.106070.2", "10.7554/elife.106070.10")
    assert identifier.publication_status == "published"


def test_an_id_is_in_one_slot_only() -> None:
    identifier = Identifier(
        pmid="32341955", doi="10.3934/Neuroscience.2018.2.97",
        alternative_pmids=["32341955", "32341954"],
        alternative_dois=["10.3934/neuroscience.2018.2.97"],
        preprint_pmcids=["PMC7181890"], alternative_pmcids=["PMC7181890"])
    assert identifier.alternative_pmids == ("32341954",)
    assert identifier.alternative_dois == ()
    assert (identifier.preprint_pmcids, identifier.alternative_pmcids) == (("PMC7181890",), ())


def test_doi_is_the_version_of_record_s_and_a_paper_known_only_by_a_preprint_is_asked_by_that() -> None:
    preprint_only = Identifier(preprint_dois=["10.21203/rs.3.rs-2556690/v2", "10.1101/2019.12.17.19014415"])
    assert preprint_only.doi is None
    # of two, the shortest, then the lowest: the same one wherever a provider is asked
    assert preprint_only.asking_doi == "10.1101/2019.12.17.19014415"
    assert preprint_only.asking("doi") == "10.1101/2019.12.17.19014415"
    assert preprint_only.asking("pmid") is None
    paper = Identifier(doi="10.1016/j.neuroimage.2023.120450",
                       preprint_dois=["10.1101/2023.10.21.563317"])
    assert paper.asking_doi == "10.1016/j.neuroimage.2023.120450"
    assert Identifier().asking_doi is None


def test_a_preprint_server_s_doi_is_never_the_doi_whoever_gives_it() -> None:
    given = Identifier(doi="10.1101/2025.05.13.653896", pmid="40462995")
    assert (given.doi, given.preprint_dois) == (None, ("10.1101/2025.05.13.653896",))

    enriched = Identifier(pmid="40462995")
    enriched.doi = "https://doi.org/10.1101/2025.05.13.653896"
    assert (enriched.doi, enriched.preprint_dois) == (None, ("10.1101/2025.05.13.653896",))
    enriched.doi = "10.1101/2025.05.13.653896"
    assert enriched.preprint_dois == ("10.1101/2025.05.13.653896",)
    enriched.doi = "10.1016/j.neuroimage.2025.121234"
    assert enriched.doi == "10.1016/j.neuroimage.2025.121234"

    # a Cold Spring Harbor journal shares bioRxiv's prefix, and eLife's DOIs are its journal's
    assert Identifier(doi="10.1101/lm.022889.111").doi == "10.1101/lm.022889.111"
    assert Identifier(doi="10.7554/elife.92563").doi == "10.7554/elife.92563"


def test_an_identifier_never_writes_a_doi_twice(tmp_path) -> None:
    identifier = Identifier(pmid="36993482", preprint_dois=["10.1101/2023.02.15.528738"])
    written = identifier.to_dict()
    assert written["doi"] is None
    assert written["preprint_dois"] == ["10.1101/2023.02.15.528738"]
    # the one provider-facing reader is derived, not stored
    assert "asking_doi" not in written


def test_lookups_by_doi_find_a_paper_by_its_preprint_s_doi() -> None:
    held = Identifier(pmid="36993482", preprint_dois=["10.1101/2023.02.15.528738"])
    assert Identifiers([held]).lookup("10.1101/2023.02.15.528738", key="doi") is held


@pytest.mark.parametrize("identifier, status", [
    (Identifier(preprint_dois=["10.1101/2025.05.13.653896"]), "preprint"),
    (Identifier(preprint_pmids=["34845462"]), "preprint"),
    (Identifier(pmid="40462995", preprint_dois=["10.1101/2025.05.13.653896"]), "published"),
    (Identifier(doi="10.1016/j.neuroimage.2023.120450",
                preprint_dois=["10.1101/2023.10.21.563317"]), "published"),
    # alternative ids never count: eLife's `.1` may be a reviewed preprint's version
    (Identifier(preprint_dois=["10.7554/elife.112197"],
                alternative_dois=["10.7554/elife.112197.1"]), "preprint"),
    (Identifier(pmid="1"), "published"),
])
def test_a_paper_is_a_preprint_while_only_a_preprint_s_ids_are_known(identifier, status) -> None:
    assert identifier.publication_status == status


def test_a_preprint_only_paper_is_not_nameless() -> None:
    # Every one of these once had the slug `--`, and slugs key metadata and downloads.
    one = Identifier(preprint_dois=["10.1101/2025.05.13.653896"])
    two = Identifier(preprint_dois=["10.1101/2019.12.17.19014415"])
    assert one.slug != two.slug != "--"
    assert Identifier(preprint_pmids=["34845462"]).slug == Identifier(pmid="34845462").slug
    # the slug of an article is the same whether its preprint's DOI is `doi` or a preprint's
    assert one.slug == Identifier(doi="10.1101/2025.05.13.653896").slug
    # of two preprints, the shortest DOI names it, as it asks providers
    two = Identifier(preprint_dois=["10.21203/rs.3.rs-2556690/v2", "10.1101/2019.12.17.19014415"])
    assert two.slug == Identifier(doi="10.1101/2019.12.17.19014415").slug
    # a paper's other ids do not rename it
    assert Identifier(pmid="1", alternative_pmids=["2"]).slug == Identifier(pmid="1").slug


def test_an_identifier_holding_only_preprint_ids_is_not_empty() -> None:
    identifier = Identifier(preprint_dois=["10.1101/2025.05.13.653896"])
    assert identifier and len(identifier) == 1
    assert identifier["preprint_dois"] == ("10.1101/2025.05.13.653896",)
    assert not Identifier()


def test_the_buckets_survive_a_dict_and_a_file(tmp_path) -> None:
    identifier = Identifier(
        pmid="42663321", doi="10.7554/elife.106070", preprint_dois=["10.1101/2025.02.05.636581"],
        alternative_dois=["10.7554/elife.106070.1"], alternative_pmids=["1"])
    assert Identifier.from_dict(identifier.to_dict()) == identifier
    # a bucket is written only when it holds ids, so an identifier without any is as before
    assert set(Identifier(pmid="1").to_dict()) == {"neurostore", "pmid", "doi", "pmcid", "other_ids"}
    path = tmp_path / "ids.jsonl"
    Identifiers([identifier]).save(path)
    assert Identifiers.load(path).identifiers == [identifier]


def test_the_pmids_of_a_paper_are_its_pmid_and_its_other_records() -> None:
    assert Identifier(pmid="32341955", alternative_pmids=["32341954"]).pmids == [
        "32341955", "32341954"]
    assert Identifier(preprint_pmids=["34845462"]).pmids == []

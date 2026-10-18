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


@pytest.mark.parametrize("value", ["PMC", "PMC12a", "PMC 123", "PMID:7212345", "10.1/x"])
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

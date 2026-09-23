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

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services.id_lookup import (
    IDLookupService,
    PubMedIDLookupService,
)


class _DummyLookup(IDLookupService):
    extractor_name = "dummy_lookup"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        raise NotImplementedError


def _settings(tmp_path) -> Settings:
    return Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        catalog_root=tmp_path / "catalog",
    )


def test_hydrate_skips_an_identifier_that_needs_nothing(tmp_path):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        service = _DummyLookup(settings, catalog)
        complete = Identifiers([Identifier(pmid="123", doi="10.1000/xyz", pmcid="PMC456")])
        assert service._hydrate_from_cache(complete) == []


def test_hydrate_fills_gaps_the_catalog_already_knows(tmp_path):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog.register(Identifier(pmid="123", doi="10.1000/xyz", pmcid="PMC456"))
        service = _DummyLookup(settings, catalog)

        partial = Identifier(pmid="123")
        pending = service._hydrate_from_cache(Identifiers([partial]))

        assert pending == []
        assert partial.doi == "10.1000/xyz"
        assert partial.pmcid == "PMC456"


def test_hydrate_returns_what_is_still_unknown(tmp_path):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        service = _DummyLookup(settings, catalog)
        unknown = Identifier(pmid="999")
        assert service._hydrate_from_cache(Identifiers([unknown])) == [unknown]


def test_discovered_ids_become_aliases(tmp_path):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        service = _DummyLookup(settings, catalog)
        service._persist_cache_entries([Identifier(pmid="55", pmcid="PMC55")])
        assert catalog.resolve(Identifier(pmcid="PMC55")) is not None


@pytest.mark.vcr()
@pytest.mark.parametrize(
    "missing_fields",
    [
        ("pmid",),
        ("pmcid",),
        ("pmid", "pmcid"),
    ],
)
def test_pubmed_lookup_restores_missing_ids(
    manifest_identifiers,
    missing_fields,
    tmp_path,
    monkeypatch,
):
    original = next(
        identifier
        for identifier in manifest_identifiers.identifiers
        if identifier.pmid and identifier.doi and identifier.pmcid
    )

    monkeypatch.setenv("EMAIL", "tests@example.com")
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        pubmed_tool="ingestion-workflow-tests",
    )
    service = PubMedIDLookupService(settings)

    ablated = Identifier(
        neurostore=original.neurostore,
        pmid=original.pmid if "pmid" not in missing_fields else None,
        doi=original.doi,
        pmcid=original.pmcid if "pmcid" not in missing_fields else None,
    )

    identifiers = Identifiers([ablated])
    service.find_identifiers(identifiers)

    restored = identifiers.identifiers[0]
    assert restored.doi == original.doi
    assert restored.pmid == original.pmid
    assert restored.pmcid == original.pmcid

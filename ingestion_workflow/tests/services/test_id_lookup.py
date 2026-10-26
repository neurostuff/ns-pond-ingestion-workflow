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


def test_hydrate_skips_an_identifier_that_needs_nothing(tmp_path, evidence):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        service = _DummyLookup(settings, catalog, evidence)
        complete = Identifiers([Identifier(pmid="123", doi="10.1000/xyz", pmcid="PMC456")])
        assert service._hydrate_from_cache(complete) == []


def test_hydrate_fills_gaps_the_catalog_already_knows(tmp_path, evidence):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog.register(Identifier(pmid="123", doi="10.1000/xyz", pmcid="PMC456"), evidence)
        service = _DummyLookup(settings, catalog, evidence)

        partial = Identifier(pmid="123")
        pending = service._hydrate_from_cache(Identifiers([partial]))

        assert pending == []
        assert partial.doi == "10.1000/xyz"
        assert partial.pmcid == "PMC456"


def test_hydrate_returns_what_is_still_unknown(tmp_path, evidence):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        service = _DummyLookup(settings, catalog, evidence)
        unknown = Identifier(pmid="999")
        assert service._hydrate_from_cache(Identifiers([unknown])) == [unknown]


def test_discovered_ids_become_aliases(tmp_path, evidence):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        service = _DummyLookup(settings, catalog, evidence)
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
    evidence,
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
    with Catalog.open(tmp_path / "catalog") as catalog:
        service = PubMedIDLookupService(settings, catalog, evidence)
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


def test_a_preprint_only_paper_with_a_pmid_and_a_pmcid_still_needs_its_journal_doi(
        tmp_path, evidence):
    """4,276 catalog articles hold a preprint's DOI and a PMID or PMCID but no version of
    record's DOI; PubMed may give the journal's."""
    with Catalog.open(tmp_path / "catalog") as catalog:
        service = _DummyLookup(_settings(tmp_path), catalog, evidence)
        preprint_only = Identifier(pmid="40462995", pmcid="PMC12132522",
                                   doi="10.1101/2025.05.13.653896")
        assert not service._is_complete(preprint_only)
        assert service._is_complete(Identifier(pmid="40462995", pmcid="PMC12132522",
                                               doi="10.1016/j.neuroimage.2025.121234"))


def test_hydrate_carries_the_catalog_s_preprint_and_alternative_ids(tmp_path, evidence):
    settings = _settings(tmp_path)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog.register(Identifier(
            pmid="42663321", doi="10.7554/elife.106070",
            preprint_dois=["10.1101/2025.02.05.636581"],
            alternative_dois=["10.7554/elife.106070.1"]), evidence)
        service = _DummyLookup(settings, catalog, evidence)

        partial = Identifier(pmid="42663321")
        service._hydrate_from_cache(Identifiers([partial]))

        assert partial.doi == "10.7554/elife.106070"
        assert partial.preprint_dois == ("10.1101/2025.02.05.636581",)
        assert partial.alternative_dois == ("10.7554/elife.106070.1",)


import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services import cache
from ingestion_workflow.services.id_lookup import (
    IDLookupService,
    PubMedIDLookupService,
)


class _DummyLookup(IDLookupService):
    extractor_name = "dummy_lookup"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        raise NotImplementedError


def test_hydrate_skips_complete_identifier(monkeypatch, tmp_path):
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
    )
    service = _DummyLookup(settings)

    called = False

    def fake_get_identifier_cache_entry(
        settings_param: Settings,
        extractor_name_param: str,
        identifier_param: Identifier,
        *,
        namespace: str = cache.GATHER_CACHE_NAMESPACE,
    ) -> None:
        nonlocal called
        called = True
        return None

    monkeypatch.setattr(
        cache,
        "get_identifier_cache_entry",
        fake_get_identifier_cache_entry,
    )

    identifiers = Identifiers([Identifier(pmid="123", doi="10.1000/xyz", pmcid="PMC456")])

    pending = service._hydrate_from_cache(identifiers)

    assert pending == []
    assert not called


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

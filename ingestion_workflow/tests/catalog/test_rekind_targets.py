"""An alias is held as the other kinds `REKIND_TO` names, and no others."""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.catalog.store import IdentityError
from ingestion_workflow.models.ids import Identifier


@pytest.fixture()
def catalog(tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as cat:
        yield cat


def test_a_doi_is_held_as_a_preprint_s_when_asked(catalog):
    a = catalog.register(Identifier(pmid="1", doi="10.1000/paper")).id
    b = catalog.register(Identifier(doi="10.1000/preprint")).id

    catalog.rekind_aliases([("doi", "10.1000/preprint", b, "preprint_doi")])

    assert catalog.aliases(b, "doi") == [] and catalog.aliases(b, "preprint_doi") == [
        "10.1000/preprint"]
    assert catalog.aliases(a, "doi") == ["10.1000/paper"]


def test_a_kind_an_alias_is_not_held_as_is_refused(catalog):
    a = catalog.register(Identifier(pmid="1", doi="10.1000/paper")).id

    with pytest.raises(IdentityError, match="a doi is not held as a preprint_pmid"):
        catalog.rekind_aliases([("doi", "10.1000/paper", a, "preprint_pmid")])
    assert catalog.aliases(a, "doi") == ["10.1000/paper"]

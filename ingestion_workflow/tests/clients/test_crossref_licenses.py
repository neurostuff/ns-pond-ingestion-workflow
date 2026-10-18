"""Crossref's licence deposits, asked for many works at once."""

from __future__ import annotations

import pytest
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.licenses import crossref_statements
from study_schema.models.paper_parse import CopyVersion

#: Each DOI's version-of-record licence as its publisher deposits it, read from Crossref
#: on 2026-10-18; None where it deposits none for the version of record.
DEPOSITS = {
    "10.1093/scan/nsw123": "cc-by-nc",
    "10.1016/j.seizure.2015.08.008": "other",  # Elsevier's open-access user licence
    "10.1002/ceat.70128": "cc-by",
    "10.1002/stab.201590143": "other",  # Wiley's terms and conditions
    "10.1007/s44470-025-00027-x": "cc-by",
    "10.1016/j.engfracmech.2023.109223": "cc-by-nc-nd",
    "10.1080/20445911.2021.1954013": None,
    "10.9999/no-such-work": None,
}


@pytest.mark.vcr()
def test_every_doi_asked_is_answered_with_its_deposits_in_one_query():
    identifiers = [Identifier(doi=doi) for doi in DEPOSITS]
    found = CrossrefClient().get_metadata(identifiers)

    assert set(found) == {i.slug for i in identifiers}
    for identifier in identifiers:
        record = found[identifier.slug]
        assert record.source == "crossref"
        vor = [s for s in crossref_statements(record.raw_metadata["crossref"]["license"])
               if s.version is CopyVersion.vor]
        assert [s.license.license_id for s in vor] == (
            [DEPOSITS[identifier.doi]] if DEPOSITS[identifier.doi] else []), identifier.doi


@pytest.mark.vcr()
def test_two_articles_sharing_a_doi_each_get_its_deposits(vcr):
    one, other = (Identifier(doi="10.1093/scan/nsw123", pmid="1"),
                  Identifier(doi="10.1093/scan/nsw123", pmid="2"))
    found = CrossrefClient().get_metadata([one, other])
    assert found[one.slug].raw_metadata == found[other.slug].raw_metadata
    assert found[one.slug].raw_metadata["crossref"]["license"]
    assert len(vcr.requests) == 1
    assert "mailto" not in vcr.requests[0].uri

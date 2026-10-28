"""Crossref's licence deposits, asked for many works at once."""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import quote, unquote

import pytest
from ingestion_workflow.clients.crossref import CROSSREF_FILTER_BYTES, CrossrefClient, packed
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.licenses import crossref_statements
from study_schema.models.paper_parse import CopyVersion

NOW = datetime(2026, 10, 18, tzinfo=timezone.utc)

#: Each DOI's version-of-record licence as its publisher deposits it, read from Crossref
#: on 2026-10-18; None where it deposits none for the version of record.
DEPOSITS = {
    "10.1093/scan/nsw123": "cc_by_nc",
    "10.1016/j.seizure.2015.08.008": "other",  # Elsevier's open-access user licence
    "10.1002/ceat.70128": "cc_by",
    "10.1002/stab.201590143": "other",  # Wiley's terms and conditions
    "10.1007/s44470-025-00027-x": "cc_by",
    "10.1016/j.engfracmech.2023.109223": "cc_by_nc_nd",
    "10.1080/20445911.2021.1954013": None,
    "10.9999/no-such-work": None,
}


def _played(vcr):
    """The recorded requests this test made, in the order they were recorded."""
    return [vcr.requests[i] for i in sorted(vcr.play_counts)]


@pytest.mark.vcr()
def test_every_doi_asked_is_answered_with_its_deposits_in_one_query():
    identifiers = [Identifier(doi=doi) for doi in DEPOSITS]
    found = CrossrefClient().get_metadata(identifiers)

    assert set(found) == {i.slug for i in identifiers}
    for identifier in identifiers:
        record = found[identifier.slug]
        assert record.source == "crossref"
        vor = [s for s in crossref_statements(record.raw_metadata["crossref"]["license"], NOW)
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
    (asked,) = _played(vcr)
    assert "mailto" not in asked.uri


#: 51 NeuroImage DOIs from 2003 the catalog holds.
NEUROIMAGE_2003 = [f"10.1016/j.neuroimage.2003.{n}" for n in (
    "05.001", "06.001", "06.002", "06.003", *(f"07.{i:03d}" for i in range(1, 36) if i not in (8, 10)),
    *(f"08.{i:03d}" for i in range(1, 15)))]


@pytest.mark.vcr()
def test_dois_that_fit_one_query_by_size_go_in_one(vcr):
    identifiers = [Identifier(doi=doi) for doi in NEUROIMAGE_2003]
    found = CrossrefClient().get_metadata(identifiers)

    assert set(found) == {i.slug for i in identifiers}
    queries = dict.fromkeys(r.uri for r in _played(vcr) if "filter=" in r.uri)
    assert [uri.count("doi%3A") for uri in queries] == [51]


def test_a_query_s_filters_are_packed_to_a_size_not_a_count():
    # 50 DOIs of 160 characters, as some publishers mint, would pass the ~8,000 bytes of
    # request line Crossref takes; packed, each query stays under the budget
    long = [f"10.1002/(sici)1097-{i:04d}" + "x" * 140 for i in range(50)]
    runs = list(packed("doi", long))

    assert [v for run in runs for v in run] == long
    assert len(runs) > 1
    assert all(len(quote(",".join(f"doi:{v}" for v in run), safe="")) <= CROSSREF_FILTER_BYTES
               for run in runs)
    # room left for the rest of a filter (relation.type values) comes off the budget
    spared = list(packed("doi", long, spare=500))
    assert all(len(quote(",".join(f"doi:{v}" for v in run), safe=""))
               <= CROSSREF_FILTER_BYTES - 500
               for run in spared)


#: A DOI Crossref knows, with no licence deposit, which a filter query for it does not
#: return.
SICI = "10.1002/(sici)1097-0150(2000)5:1<1::aid-igs1>3.0.co;2-4"


@pytest.mark.vcr()
def test_a_doi_a_filter_query_leaves_out_is_asked_for_singly(vcr):
    identifiers = [Identifier(doi=SICI), Identifier(doi="10.1093/scan/nsw123")]
    found = CrossrefClient().get_metadata(identifiers)

    assert set(found) == {i.slug for i in identifiers}
    assert vcr.all_played
    single = [r.uri for r in _played(vcr) if "filter=" not in r.uri]
    # Crossref answers the SICI DOI with a redirect to the work's DOI
    assert [unquote(uri) for uri in dict.fromkeys(single)] == [
        "https://api.crossref.org/works/" + SICI,
        "https://api.crossref.org/works/10.3109/10929080009148866"]
    assert found[identifiers[0].slug].raw_metadata == {"crossref": {"license": []}}


@pytest.mark.vcr()
def test_a_doi_crossref_would_refuse_the_query_for_is_asked_for_singly(vcr):
    # a prefix of three digits: in a filter, Crossref refuses the whole query (400
    # doi-not-valid), which would fail the batch's other DOIs with it
    identifiers = [Identifier(doi="10.137/journal.pone.0066241"),
                   Identifier(doi="10.1093/scan/nsw123")]
    found = CrossrefClient().get_metadata(identifiers)

    assert set(found) == {i.slug for i in identifiers}
    assert found[identifiers[1].slug].raw_metadata["crossref"]["license"]
    listed = [unquote(r.uri) for r in vcr.requests if "filter=" in r.uri]
    assert len(listed) == 1 and "10.137/" not in listed[0]

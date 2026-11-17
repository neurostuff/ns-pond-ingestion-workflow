"""Only the curated TSV makes the dictionary; a scraped record joins an entry by an id or a full
name, or is a candidate."""

from __future__ import annotations

import pytest
from ingestion_workflow.data_sources.merge import (
    Record,
    curated_records,
    digest,
    merge,
    scraped_aliases,
)


@pytest.fixture(scope="module")
def curated():
    return curated_records()


def test_the_curated_tsv_merges_into_one_entry_per_row(curated):
    sources, aliases, candidates = merge(curated)
    assert not candidates
    assert [s.id for s in sources] == sorted(r.record_id for r in curated)
    adni = next(s for s in sources if s.id == "adni")
    assert (adni.part_of, adni.rrids, adni.providers) == ("loni_ida", ["SCR_003007"],
                                                         ["curated"])
    assert ("adni", "ADNI", "safe") in aliases


def test_a_record_sharing_an_rrid_joins_the_curated_entry_and_adds_no_aliases(curated):
    scraped = Record(provider="scicrunch", record_id="SCR_003007", name="ADNI portal",
                     aliases=[("ADNI portal", "name"), ("AP", "defined")], rrids=["SCR_003007"],
                     url="https://example.org")
    sources, aliases, candidates = merge([*curated, scraped])
    assert (len(sources), candidates) == (len(curated), [])
    adni = next(s for s in sources if s.id == "adni")
    assert adni.providers == ["curated", "scicrunch"]
    assert adni.url == "http://adni-info.org/"
    assert aliases == merge(curated)[1]


def test_a_record_joins_by_a_full_name_but_never_by_its_acronym(curated):
    named = Record(provider="jpnd", record_id="jpnd_adni", name="Alzheimers Disease "
                   "Neuroimaging Initiative", aliases=[("ADNI", "defined")], rrids=["SCR_1"])
    lookalike = Record(provider="jpnd", record_id="jpnd_other", name="Another Dementia "
                       "Network Initiative", aliases=scraped_aliases(
                           ["Another Dementia Network Initiative"], ["ADNI"]))
    sources, aliases, candidates = merge([*curated, named, lookalike])
    assert [c.record_id for c in candidates] == ["jpnd_other"]
    assert {s.id for s in sources} == {r.record_id for r in curated}
    adni = next(s for s in sources if s.id == "adni")
    assert (adni.providers, adni.rrids) == (["curated", "jpnd"], ["SCR_003007", "SCR_1"])
    assert aliases == merge(curated)[1]


def test_scraped_aliases_are_given_the_precise_tiers():
    assert scraped_aliases(["UK Biobank", "Biobank", "UK  Biobank"], ["UKB", "U K"]) == [
        ("UK Biobank", "name"), ("Biobank", "qualified"), ("UKB", "defined")]


def test_a_part_of_naming_no_entry_is_refused():
    with pytest.raises(ValueError, match="part_of"):
        merge([Record(provider="curated", record_id="a", name="A", part_of="missing")])


def test_the_digest_follows_the_aliases_only(curated):
    aliases = merge(curated)[1]
    assert digest(aliases) == digest(list(reversed(aliases)))
    assert digest(aliases) != digest(aliases[1:])

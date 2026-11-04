"""The curated TSV's entries win; a scraped record joins one only by an id or a full name."""

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
    sources, aliases = merge(curated)
    assert [s.id for s in sources] == sorted(r.record_id for r in curated)
    adni = next(s for s in sources if s.id == "adni")
    assert (adni.part_of, adni.rrids, adni.providers) == ("loni_ida", ["SCR_003007"],
                                                         ["curated"])
    assert ("adni", "ADNI", "safe") in aliases


def test_a_record_sharing_an_rrid_joins_the_curated_entry_and_adds_no_aliases(curated):
    scraped = Record(provider="scicrunch", record_id="SCR_003007", name="ADNI portal",
                     aliases=[("ADNI portal", "name"), ("AP", "defined")], rrids=["SCR_003007"],
                     url="https://example.org")
    sources, aliases = merge([*curated, scraped])
    assert len(sources) == len(curated)
    adni = next(s for s in sources if s.id == "adni")
    assert adni.providers == ["curated", "scicrunch"]
    assert adni.url == "http://adni-info.org/"
    assert aliases == merge(curated)[1]


def test_a_record_joins_by_a_full_name_but_never_by_its_acronym(curated):
    named = Record(provider="jpnd", record_id="jpnd_adni", name="Alzheimers Disease "
                   "Neuroimaging Initiative", aliases=[("ADNI", "defined")])
    lookalike = Record(provider="jpnd", record_id="jpnd_other", name="Another Dementia "
                       "Network Initiative", aliases=scraped_aliases(
                           ["Another Dementia Network Initiative"], ["ADNI"]))
    sources, aliases = merge([*curated, named, lookalike])
    ids = {s.id for s in sources}
    assert "jpnd_adni" not in ids and "jpnd_other" in ids
    assert ("jpnd_other", "ADNI", "defined") in aliases


def test_two_new_records_of_one_data_source_merge(curated):
    first = Record(provider="re3data", record_id="r3d1", name="Example Brain Bank",
                   aliases=scraped_aliases(["Example Brain Bank"], ["EBB"]), re3data_ids=["r3d1"])
    second = Record(provider="scicrunch", record_id="SCR_1", name="Example Brain Bank",
                    aliases=scraped_aliases(["Example Brain Bank", "EBBank"], []), rrids=["SCR_1"])
    sources, aliases = merge([*curated, second, first])
    bank = next(s for s in sources if s.id == "r3d1")
    assert (bank.rrids, bank.re3data_ids, bank.providers) == (["SCR_1"], ["r3d1"],
                                                             ["re3data", "scicrunch"])
    assert [(a, t) for i, a, t in aliases if i == "r3d1"] == [
        ("EBB", "defined"), ("EBBank", "qualified"), ("Example Brain Bank", "name")]


def test_scraped_aliases_are_given_the_precise_tiers():
    assert scraped_aliases(["UK Biobank", "Biobank", "UK  Biobank"], ["UKB", "U K"]) == [
        ("UK Biobank", "name"), ("Biobank", "qualified"), ("UKB", "defined")]


def test_a_part_of_naming_no_entry_is_refused():
    with pytest.raises(ValueError, match="part_of"):
        merge([Record(provider="curated", record_id="a", name="A", part_of="missing")])


def test_the_digest_follows_the_aliases_only(curated):
    _, aliases = merge(curated)
    assert digest(aliases) == digest(list(reversed(aliases)))
    assert digest(aliases) != digest(aliases[1:])

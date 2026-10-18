"""Licences: what each source's statement names, and which statement speaks for a copy."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from ingestion_workflow.services.licenses import (
    ASSUMED,
    OWN,
    Statement,
    Undecided,
    copyright_line,
    crossref_statements,
    elsevier_license,
    jats_license,
    license_of,
    normalise_license,
    own_license,
    stated,
)
from lxml import etree
from study_schema.models.paper_parse import CopyVersion, MetadataSource

DATA = Path(__file__).parents[1] / "data"
LICENSES = DATA / "licenses"
COPIES = DATA / "copies"


@pytest.mark.parametrize("statement, expected", [
    ("http://creativecommons.org/licenses/by/4.0/", "cc-by"),
    ("https://creativecommons.org/licenses/by-nc-nd/4.0/", "cc-by-nc-nd"),
    ("http://creativecommons.org/publicdomain/zero/1.0/", "cc0"),
    # OpenAlex's slugs
    ("cc-by-nc", "cc-by-nc"),
    ("publisher-specific-oa", "other"),
    # PMC's Cloud Service codes
    ("CC BY-NC-ND", "cc-by-nc-nd"),
    ("CC0", "cc0"),
    ("TDM", "all-rights-reserved"),
    ("CC BY 4.0", "cc-by"),
    ("This is an open-access article distributed under the terms of the Creative Commons "
     "Attribution License (CC BY).", "cc-by"),
    ("Creative Commons Attribution-NonCommercial License", "cc-by-nc"),
    # Elsevier's open-access user licence is its own terms, not a CC licence
    ("http://www.elsevier.com/open-access/userlicense/1.0/", "other"),
    # Terms that grant text and data mining only
    ("https://www.elsevier.com/tdm/userlicense/1.0/", "all-rights-reserved"),
    ("https://www.springernature.com/gp/researchers/text-and-data-mining", "all-rights-reserved"),
    ("http://doi.wiley.com/10.1002/tdm_license_1.1", "all-rights-reserved"),
    ("This file is available for text mining. It may also be used consistent with the "
     "principles of fair use under the copyright law.", "all-rights-reserved"),
    ("http://onlinelibrary.wiley.com/termsAndConditions#vor", "other"),
    ("Copyright © 2013 Elsevier Inc. All rights reserved.", "all-rights-reserved"),
])
def test_each_way_a_source_states_a_licence_names_one(statement, expected):
    assert normalise_license(statement) == expected


def _jats(name):
    return etree.parse(str(LICENSES / name)).getroot()


@pytest.mark.parametrize("fixture, license_id, stated_as", [
    # PMC's text-mining license_ref is the CC BY the paragraph states too
    ("jats_ali_license_ref_pmc8358179.xml", "cc-by",
     "https://creativecommons.org/licenses/by/4.0/"),
    ("jats_cc_by_href_pmc2430701.xml", "cc-by", "https://creativecommons.org/licenses/by/2.0/"),
    # a copyright line and no licence
    ("jats_copyright_only_pmc3822871.xml", "all-rights-reserved",
     "© 2009 The Authors Journal compilation © 2009 Foundation for Cellular and Molecular "
     "Medicine/Blackwell Publishing Ltd"),
    # an NIH manuscript PMC offers for text mining only
    ("jats_manuscript_text_mining_pmc2739903.xml", "all-rights-reserved",
     "This file is available for text mining. It may also be used consistent with the "
     "principles of fair use under the copyright law."),
])
def test_a_jats_copy_states_its_own_licence(fixture, license_id, stated_as):
    found = jats_license(_jats(fixture))
    assert (found.license_id, found.determination, found.stated_by, found.stated_as) == (
        license_id, "stated", "jats", stated_as)


def test_a_jats_textmining_ref_yields_to_a_version_ref():
    article = etree.fromstring(
        b'<article><front><article-meta><permissions><license>'
        b'<ali:license_ref xmlns:ali="http://www.niso.org/schemas/ali/1.0/" '
        b'specific-use="textmining">https://creativecommons.org/licenses/by-nc/4.0/'
        b'</ali:license_ref>'
        b'<ali:license_ref xmlns:ali="http://www.niso.org/schemas/ali/1.0/" '
        b'specific-use="vor">https://creativecommons.org/licenses/by/4.0/</ali:license_ref>'
        b'</license></permissions></article-meta></front></article>')
    assert jats_license(article).stated_as == "https://creativecommons.org/licenses/by/4.0/"


def test_a_jats_licence_named_in_its_text_beats_its_type():
    """`license-type="open-access"` names no licence; the paragraph after it does."""
    article = etree.fromstring(
        b'<article><front><article-meta><permissions><license license-type="open-access">'
        b'<license-p>This is an open access article under the terms of the Creative Commons '
        b'Attribution-NonCommercial License.</license-p></license></permissions>'
        b'</article-meta></front></article>')
    assert jats_license(article).license_id == "cc-by-nc"


@pytest.mark.parametrize("fixture, license_id, stated_as", [
    # 10.1016/j.seizure.2015.08.008
    ("elsevier_user_license", "other", "http://www.elsevier.com/open-access/userlicense/1.0/"),
    # 10.1016/j.neurobiolaging.2012.04.016
    ("elsevier_copyright_only", "all-rights-reserved",
     "Copyright © 2013 Elsevier Inc. All rights reserved."),
])
def test_an_elsevier_copy_states_its_own_licence(fixture, license_id, stated_as):
    found = elsevier_license(LICENSES / fixture / "content.xml")
    assert (found.license_id, found.stated_by, found.stated_as) == (
        license_id, "elsevier", stated_as)


@pytest.mark.parametrize("source, fixture, expected", [
    # PMC10518222's cloud record codes its manuscript TDM, whatever its JATS says
    ("pmc", "pmc_tdm_manuscript", ("all-rights-reserved", "pmc_cloud", "TDM")),
    ("pmc", "pmc_links_its_preprint", ("cc-by-nc-nd", "pmc_cloud", "CC BY-NC-ND")),
    ("pubget", "pubget_open_licence", None),
])
def test_a_held_copy_s_own_record_is_read_cloud_record_first(source, fixture, expected):
    files = sorted((COPIES / fixture).iterdir())
    found = own_license(source, files)
    if expected is None:
        assert found == jats_license(etree.parse(str(COPIES / fixture / "article.xml")).getroot())
        assert found.stated_by == "jats"
    else:
        assert (found.license_id, found.stated_by, found.stated_as) == expected


def test_a_copy_with_no_record_of_its_licence_states_none(tmp_path):
    pdf = tmp_path / "article.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    assert own_license("wiley", [pdf]) is None


def test_an_unreadable_record_is_an_error_not_a_silence(tmp_path):
    cloud = tmp_path / "pmc_cloud.json"
    cloud.write_text('{"license_code": ', encoding="utf-8")
    with pytest.raises(ValueError):
        own_license("pmc", [cloud])


def _deposit(url, version, start="2020-01-01T00:00:00Z"):
    return {"URL": url, "content-version": version, "start": {"date-time": start}}


NOW = datetime(2026, 10, 18, tzinfo=timezone.utc)


def test_crossref_states_one_licence_per_version_and_reads_no_mining_or_sharing_deposit():
    found = crossref_statements([
        _deposit("http://doi.wiley.com/10.1002/tdm_license_1.1", "tdm"),
        _deposit("https://doi.org/10.15223/policy-004", "stm-asf"),
        _deposit("http://onlinelibrary.wiley.com/termsAndConditions#am", "am"),
        _deposit("http://onlinelibrary.wiley.com/termsAndConditions#vor", "vor"),
        _deposit("http://creativecommons.org/licenses/by/4.0/", "vor", "2021-01-01T00:00:00Z"),
    ], now=NOW)
    by_version = {s.version: (s.license.license_id, s.license.stated_as) for s in found}
    assert by_version == {
        CopyVersion.vor: ("cc-by", "http://creativecommons.org/licenses/by/4.0/"),
        CopyVersion.am: ("other", "http://onlinelibrary.wiley.com/termsAndConditions#am"),
    }


def test_an_embargoed_crossref_deposit_is_not_yet_in_force_and_order_does_not_matter():
    entries = [
        _deposit("http://creativecommons.org/licenses/by/4.0/", "am", "2027-01-01T00:00:00Z"),
        _deposit("https://creativecommons.org/licenses/by-nc/4.0/", "unspecified"),
        _deposit("https://creativecommons.org/licenses/by/4.0/", "unspecified"),
    ]
    one = crossref_statements(entries, now=NOW)
    other = crossref_statements(entries[::-1], now=NOW)
    assert [(s.version, s.license.stated_as) for s in one] == \
        [(s.version, s.license.stated_as) for s in other] == \
        [(None, "https://creativecommons.org/licenses/by-nc/4.0/")]


def _said(by, text, version=None, url=None):
    return Statement(stated(by, text), version, url)


COPYRIGHT = copyright_line(MetadataSource.jats, "© 2009 The Authors")
CROSSREF_VOR_CC = _said(MetadataSource.crossref, "http://creativecommons.org/licenses/by/4.0/",
                        CopyVersion.vor)
CROSSREF_VOR_TERMS = _said(MetadataSource.crossref,
                           "http://onlinelibrary.wiley.com/termsAndConditions#vor", CopyVersion.vor)
CROSSREF_AM = _said(MetadataSource.crossref,
                    "http://onlinelibrary.wiley.com/termsAndConditions#am", CopyVersion.am)
OPENALEX_VOR = _said(MetadataSource.openalex, "cc-by-nc", CopyVersion.vor,
                     "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/alz.12697")


def test_a_copy_s_own_statement_comes_first():
    assert COPYRIGHT.license_id == "all-rights-reserved"
    found = license_of(CopyVersion.vor, COPYRIGHT, {MetadataSource.crossref: [CROSSREF_VOR_CC]},
                       set())
    assert found is COPYRIGHT


def test_crossref_s_vor_deposit_comes_before_openalex():
    statements = {MetadataSource.crossref: [CROSSREF_VOR_CC],
                  MetadataSource.openalex: [OPENALEX_VOR]}
    assert license_of(CopyVersion.vor, None, statements, set()) is CROSSREF_VOR_CC.license


def test_a_named_licence_comes_before_an_other_from_a_better_source():
    statements = {MetadataSource.crossref: [CROSSREF_VOR_TERMS],
                  MetadataSource.openalex: [OPENALEX_VOR]}
    assert license_of(CopyVersion.vor, None, statements, set()) is OPENALEX_VOR.license


def test_a_manuscript_never_takes_the_version_of_record_s_licence():
    statements = {MetadataSource.crossref: [CROSSREF_VOR_CC, CROSSREF_AM],
                  MetadataSource.openalex: [OPENALEX_VOR]}
    assert license_of(CopyVersion.am, None, statements, set()) is CROSSREF_AM.license
    assert license_of(CopyVersion.am, None, {MetadataSource.crossref: [CROSSREF_VOR_CC]},
                      set()) == ASSUMED


def test_nothing_stated_is_assumed_only_when_every_source_asked_answered():
    assert license_of(CopyVersion.vor, None, {}, set()) == ASSUMED
    with pytest.raises(Undecided, match="crossref"):
        license_of(CopyVersion.vor, None, {}, {"crossref"})
    with pytest.raises(Undecided, match="semantic_scholar"):
        license_of(CopyVersion.vor, None, {}, {"semantic_scholar"})
    with pytest.raises(Undecided, match=OWN):
        license_of(None, None, {}, {OWN})
    # nobody speaks for a copy of no known version but itself
    assert license_of(None, None, {}, {"crossref"}) == ASSUMED
    # a statement decides whoever did not answer
    assert license_of(CopyVersion.vor, None, {MetadataSource.crossref: [CROSSREF_VOR_CC]},
                      {"openalex"}) is CROSSREF_VOR_CC.license

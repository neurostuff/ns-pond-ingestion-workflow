"""The DOIs Crossref relates to one DOI, from its own record and from the records naming it.

Crossref's answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

import pytest


@pytest.mark.vcr()
def test_a_version_doi_s_other_side_is_asked_by_the_publisher_s_spelling(evidence):
    # the base record says is-same-as "10.7554/eLife.106070.3"; Crossref matches it by case,
    # and the version's own record gives that spelling as its alternative-id
    related = evidence.related("10.7554/elife.106070.3")

    assert related.versions == ("10.7554/elife.106070.1", "10.7554/elife.106070.2",
                                "10.7554/elife.106070")


@pytest.mark.vcr()
def test_a_preprint_s_papers_come_from_either_side(evidence):
    # the paper's record says has-preprint; bioRxiv's says nothing
    related = evidence.related("10.1101/2020.05.06.058792")

    assert related.papers == ("10.1093/schbul/sbaa075",)
    assert related.preprints == ()

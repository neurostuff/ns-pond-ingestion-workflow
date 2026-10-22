"""The identity evidence: one title test, and what the registries say of a DOI."""

from __future__ import annotations

import httpx
import pytest
from ingestion_workflow.catalog.evidence import same_title
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import _retryable


@pytest.mark.parametrize("a, b, same", [
    # PubMed's and Crossref's titles of 10.1016/j.biopsych.2011.09.005
    ("Fronto-striato-cerebellar dysregulation in adolescents with depression.",
     "Fronto-Striato-Cerebellar Dysregulation in Adolescents with Depression", True),
    # a title and the same title with its subtitle
    ("What makes man human", "What makes man human: thirty-ninth James Arthur lecture on the "
                             "evolution of the human brain, 1970.", True),
    # two papers PubMed gives one DOI, 10.14670/HH-25-013
    ("Ocular adnexal lymphomas: A comprehensive review with emphasis on histopathologic and "
     "magnetic resonance imaging appearance.",
     "Immunohistochemical localization of D-beta-aspartic acid and periostin in benign "
     "parotid gland tumors.", False),
    ("", "Anything", False),
    (None, None, False),
])
def test_same_title(a, b, same):
    assert same_title(a, b) is same


@pytest.mark.vcr()
def test_the_registry_has_no_title_for_a_doi_it_does_not_know():
    registry = DoiRegistry(CrossrefClient())
    assert registry.title("10.137/journal.pone.0066241") is None
    assert registry.title("10.14670/hh-25-014").startswith("Ocular adnexal lymphomas")


@pytest.mark.parametrize("response, retried", [(429, True), (500, True), (503, True), (404, False),
                                               (400, False)])
def test_ncbi_is_asked_again_after_a_refusal_to_come_back_later(response, retried):
    request = httpx.Request("GET", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi")
    error = httpx.HTTPStatusError("x", request=request, response=httpx.Response(response, request=request))
    assert _retryable(error) is retried
    assert _retryable(httpx.ConnectError("refused", request=request)) is True


def test_an_agency_serving_another_host_s_certificate_is_not_asked_again():
    # doi.org sends some DOIs to data.chinadoi.cn, whose certificate names another host.
    from ingestion_workflow.clients.doi_registry import _bad_certificate, _retryable as registry_retryable

    request = httpx.Request("GET", "https://data.chinadoi.cn/x")
    bad = httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname "
                             "mismatch, certificate is not valid for 'data.chinadoi.cn'.", request=request)
    down = httpx.ConnectError("[Errno 111] Connection refused", request=request)
    assert _bad_certificate(bad) and not registry_retryable(bad)
    assert not _bad_certificate(down) and registry_retryable(down)

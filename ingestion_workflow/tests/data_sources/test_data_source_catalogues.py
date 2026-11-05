"""Each catalogue's answers, recorded, read into records of imaging data sources only.

re3data's "magnetic resonance" listing holds chemistry's NMR databases (BMRB) beside
NDAR, so a record must name an imaging modality and the brain to be kept.
"""

from __future__ import annotations

import itertools
import os
from types import SimpleNamespace

import pytest
from ingestion_workflow.clients import jpnd as jpnd_client
from ingestion_workflow.clients import scicrunch as scicrunch_client
from ingestion_workflow.clients.jpnd import JpndClient
from ingestion_workflow.clients.re3data import Re3dataClient
from ingestion_workflow.clients.scicrunch import SciCrunchClient
from ingestion_workflow.data_sources import providers

NDAR, BMRB = "r3d100010717", "r3d100010191"


class _Listed(Re3dataClient):
    """re3data's client, asked only for the repositories given."""

    def __init__(self, ids):
        super().__init__()
        self._ids = ids

    def repository_ids(self):
        return self._ids


@pytest.mark.vcr
def test_re3data_keeps_the_brain_imaging_repository_and_not_the_nmr_one():
    (ndar,) = providers.re3data(_Listed([NDAR, BMRB]))
    assert (ndar.record_id, ndar.re3data_ids, ndar.kinds) == (NDAR, [NDAR], ["repository"])
    assert ("National Database for Autism Research", "name") in ndar.aliases
    assert "MRI" in ndar.modalities


@pytest.mark.vcr
def test_jpnd_keeps_the_cohorts_with_brain_imaging(monkeypatch):
    monkeypatch.setattr(jpnd_client, "PAGE", 10)
    cohorts = list(itertools.islice(JpndClient().cohorts(), 10))
    kept = providers.jpnd(SimpleNamespace(cohorts=lambda: cohorts))
    assert 0 < len(kept) < len(cohorts)
    imaging = {f"jpnd_{c['slug']}" for c in cohorts if providers.modalities(c["brain_imaging"])}
    assert {r.record_id for r in kept} == imaging
    aric = next(r for r in kept if r.name == "The Atherosclerosis Risk in Communities Study")
    assert ("ARIC", "defined") in aric.aliases


@pytest.mark.vcr
def test_scicrunch_reads_rrids_names_and_abbreviations(monkeypatch):
    monkeypatch.setattr(scicrunch_client, "PAGE", 25)
    client = SciCrunchClient(os.environ.get("SCICRUNCH_API_KEY") or "DUMMY")
    items = list(itertools.islice(client.resources(), 25))
    kept = providers.scicrunch(SimpleNamespace(resources=lambda: items))
    assert 0 < len(kept) <= len(items)
    assert all(r.rrids == [r.record_id] and r.record_id.startswith("SCR_") for r in kept)
    assert all({"MRI", "PET"} & set(r.modalities) for r in kept)


def test_scicrunch_needs_a_key():
    with pytest.raises(ValueError, match="scicrunch_api_key"):
        SciCrunchClient("")

"""What OSF and OpenNeuro say of the projects and datasets papers link to, as recorded.

OSF: 4b9v8 (Ashby et al. 2025's project, NIfTI templates under MRI_code/), 4drz6 (Stoll
et al. 2026's project: figures, videos and code, its data on OpenNeuro), w6khb (a project
NeuroImage 10.1016/j.neuroimage.2021.117808 links to, now private) and zzzzz (no such
guid). OpenNeuro: ds006128 (Stoll et al.'s MRI), ds003061 (EEG), ds002718 (EEG with a
T1w per subject) and ds999999 (no such dataset).
"""

from __future__ import annotations

import pytest
import tenacity
from ingestion_workflow.clients import openneuro
from ingestion_workflow.clients import osf as osf_client
from ingestion_workflow.clients.openneuro import OpenNeuroClient
from ingestion_workflow.clients.osf import OsfClient


@pytest.fixture(autouse=True)
def unpaced(monkeypatch):
    """A replay keeps no provider's limit."""
    monkeypatch.setattr(osf_client, "OSF_REQUEST_LIMIT", float("inf"))
    monkeypatch.setattr(openneuro, "OPENNEURO_REQUEST_LIMIT", float("inf"))


@pytest.fixture()
def no_waiting(monkeypatch):
    monkeypatch.setattr(OsfClient._get.retry, "wait", tenacity.wait_none())
    monkeypatch.setattr(OpenNeuroClient._query.retry, "wait", tenacity.wait_none())


@pytest.fixture()
def osf(monkeypatch):
    monkeypatch.delenv("OSF_TOKEN", raising=False)
    return OsfClient()


@pytest.mark.vcr()
def test_a_project_with_a_nifti_file_among_its_folders_holds_nifti(osf):
    listing = osf.listing("4b9v8")

    assert (listing.has_nifti, listing.reason, listing.kind) == (True, None, "nodes")
    assert listing.nifti[0] == "/MRI_code/priors_ACT/Template_BrainCerebellumExtractionMask.nii.gz"
    # stops at the folder that held one: experiment_code/, data/, MRI_code/, priors_ACT/
    assert listing.requests < 10


@pytest.mark.vcr()
def test_a_project_whose_every_file_was_listed_and_none_is_nifti_holds_none(osf):
    listing = osf.listing("4drz6")

    assert (listing.has_nifti, listing.reason, listing.failed) == (False, None, {})
    assert listing.files == 35 and listing.nifti == []


@pytest.mark.vcr()
@pytest.mark.parametrize("guid, reason", [
    ("w6khb", "private on OSF (401: Authentication credentials were not provided.)"),
    ("zzzzz", "not found on OSF (404: Not found.)"),
], ids=["private", "missing"])
def test_a_private_or_missing_project_says_nothing_and_why(osf, guid, reason):
    listing = osf.listing(guid)

    assert (listing.has_nifti, listing.reason, listing.requests) == (None, reason, 1)


@pytest.mark.vcr()
def test_osf_down_for_a_listing_under_the_project_leaves_it_unsettled(osf, no_waiting):
    """Recorded from 4b9v8, then MRI_code/'s listing edited to answer 503 on every try."""
    listing = osf.listing("4b9v8")

    assert (listing.has_nifti, listing.transient) == (None, True)
    assert listing.reason == "1 listing(s) failed"
    (failed,) = listing.failed.values()
    assert "503" in failed


@pytest.mark.vcr()
def test_osf_down_for_the_project_itself_raises(osf, no_waiting):
    """Recorded from 4b9v8, then edited to answer 503 on every try."""
    import requests

    with pytest.raises(requests.HTTPError, match="503"):
        osf.listing("4b9v8")


@pytest.mark.vcr()
@pytest.mark.parametrize("dataset, tag, has_nifti, modalities", [
    ("ds006128", None, True, ["mri"]),
    ("ds006128", "1.0.9", True, ["mri"]),
    ("ds003061", None, False, ["eeg"]),
    ("ds002718", None, True, ["eeg", "mri"]),
], ids=["mri", "mri-tagged", "eeg", "eeg-with-mri"])
def test_an_openneuro_snapshot_holds_nifti_when_it_holds_mri_or_pet(dataset, tag, has_nifti,
                                                                     modalities):
    snapshot = OpenNeuroClient().snapshot(dataset, tag)

    assert (snapshot.has_nifti, snapshot.modalities, snapshot.reason) == (
        has_nifti, modalities, None)
    assert snapshot.tag == (tag or snapshot.tag) and snapshot.tag


@pytest.mark.vcr()
@pytest.mark.parametrize("dataset, tag, reason", [
    ("ds999999", None, "OpenNeuro: Dataset ds999999 does not exist."),
    ("ds006128", "9.9.9", "OpenNeuro: Not Found"),
], ids=["missing-dataset", "missing-snapshot"])
def test_a_missing_dataset_or_snapshot_says_nothing_and_why(dataset, tag, reason):
    snapshot = OpenNeuroClient().snapshot(dataset, tag)

    assert (snapshot.has_nifti, snapshot.reason) == (None, reason)

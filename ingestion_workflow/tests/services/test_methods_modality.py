"""A paper's modalities from its methods text, on sentences quoted from real papers.

Each sentence is one the 2026-11-19 hand-check read, from a paper with no modality in
Neurostore: the misses the first rules made, and what they got right.
"""

import pytest
from ingestion_workflow.services import methods_modality as mm


def modalities(text):
    return mm.read(text).modalities


@pytest.mark.parametrize("text, expected", [
    # fMRI and DTI in one session
    ("All subjects underwent both DTI and fMRI scanning on a 3.0 T GE Signa system (GE Medical "
     "Systems). For DTI, each volume was acquired with 40 mT/m gradients. Functional MRI (fMRI) "
     "procedure. Functional data were collected on a 3 T Siemens TIM Trio whole body system.",
     ["dMRI", "fMRI"]),
    # simultaneous EEG-fMRI
    ("Simultaneous EEG-fMRI data collection and preprocessing. This study included resting-state "
     "fMRI data. Continuous 128-channel EEG was acquired through a Biosemi amplifier.",
     ["EEG", "fMRI"]),
    ("We used positron emission tomography to measure dopamine synaptic markers. PET Scans. "
     "A Siemens HR+ tomograph was used (Siemens/CTI Knoxville, Tennessee).", ["PET"]),
    ("MEG recording. Magnetic event-related signals were recorded with a CTF Omega 151 apparatus. "
     "MEG data were filtered.", ["MEG"]),
    # bare MRI, kind unstated
    ("Magnetic resonance imaging (MRI) was performed postoperatively at days 3 and 10 using a "
     "4.7-T horizontal scanner. MRI images were analysed blind.", ["MRI"]),
])
def test_reads_the_acquisitions(text, expected):
    assert modalities(text) == expected


@pytest.mark.parametrize("text, absent", [
    # EPI is the readout of a diffusion series, not fMRI
    ("Furthermore, a single-shot EPI (echo planar imaging) scan was made as part of a diffusion "
     "tensor imaging (DTI)-series (SENSE factor 2.5). Single-shell DWI data were acquired with a "
     "spin echo echo-planar imaging (EPI) sequence.", "fMRI"),
    # electrode positions named after the EEG system
    ("The left M1-Hand was identified using electrode position C3 of the international 10–20 EEG "
     "system [24]. Detectors were placed over the bilateral frontal lobes according to the "
     "international 10–20 EEG system, resulting in 28 standard channels.", "EEG"),
    # EEG in a clinical diagnosis
    ("The diagnosis of epilepsy was made by expert neurologists based on clinical, "
     "electroencephalographic (EEG), and MRI findings. We recorded seizure type, "
     "electroencephalography (EEG) or video EEG findings, and seizure outcomes.", "EEG"),
    ("Presurgical workup consisted of electroencephalography (EEG)-video monitoring and "
     "neuropsychological testing. Video-EEG monitoring was done in all patients.", "EEG"),
    # receptor atlases, not an acquisition
    ("We performed spatial correlation calculations with the PET- and SPECT-derived mappings. "
     "The PET and SPECT mappings in JuSpace were made in MNI space.", "PET"),
    ("We performed spatial correlation calculations with the PET- and SPECT-derived mappings. "
     "The PET and SPECT mappings in JuSpace were made in MNI space.", "SPECT"),
    # a cited study's modality
    ("The experimental protocol was adapted from a previous functional magnetic resonance imaging "
     "(fMRI) study (Basten et al., 2010). It followed a previous fMRI study of choice.", "fMRI"),
    # TMS neuronavigation on a template
    ("The coil was positioned by means of stereotaxic navigation on individual estimated magnetic "
     "resonance images (MRI) obtained through a 3D warping procedure fitting a high-resolution "
     "anatomical MRI T1 template (Softaxic, EMS).", "MRI"),
])
def test_mentions_that_are_not_an_acquisition(text, absent):
    assert absent not in modalities(text)


def test_one_mention_is_not_enough():
    assert modalities("Participants had no history of PET scanning. Behaviour was recorded.") == []


def test_methods_section_first_then_full_text():
    methods = ("## Introduction\nPrior PET studies of pain. PET has shown much.\n"
               "## Methods\nfMRI data were acquired. BOLD images were collected.\n"
               "## Results\nPET agreed.\n")
    got = mm.read(methods)
    assert (got.modalities, got.where) == (["fMRI"], "methods")
    no_heading = "fMRI data were acquired. BOLD images were collected."
    got = mm.read(no_heading)
    assert (got.modalities, got.where) == (["fMRI"], "full_text")


def test_a_reference_list_inside_methods_is_cut():
    text = ("## Methods\nfMRI data were acquired. BOLD images were collected.\n"
            "References\nVolkow, N. (1993). Dopamine D2 receptors measured by positron emission "
            "tomography (PET) and [11C]-raclopride. Ann NY Acad Sci.\n")
    assert modalities(text) == ["fMRI"]


def test_values_are_study_schema_modalities():
    """The record's modalities are pondie's, in study_schema's spelling; the text's must match them."""
    from study_schema.models.storage import Modality

    assert set(mm.RULES) <= {m.value for m in Modality}

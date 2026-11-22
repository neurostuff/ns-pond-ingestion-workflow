"""Acquisition modality onto the closed `acquisition_modality` vocabulary, on real values.

The wordings are `Acquisition.modality` values and OtherModality `modality_label`s as
production records carry them (beast catalog, 2026-11-19). `data/modality_records.json`
holds nine real records trimmed to their acquisitions, the analyses' acquisition and
measure links, and those measures' types.
"""

import json
from pathlib import Path

import pytest
from ingestion_workflow.normalization import modality

DATA = json.loads((Path(__file__).parent / "data" / "modality_records.json").read_text())


@pytest.mark.parametrize("value, label, expected", [
    # the schema's own values, as the extractor writes them
    ("fMRI", None, "fmri"), ("sMRI", None, "smri"), ("dMRI", None, "dmri"),
    ("EEG", None, "eeg"), ("fNIRS", None, "fnirs"), ("PET", None, "pet"),
    ("MEG", "magnetoencephalography (MEG)", "meg"),
    ("SPECT", "single photon emission computed tomography (SPECT)", "spect"),
    ("SPECT", "123I-FP-CIT SPECT", "spect"),
    # free text in `modality`
    ("CT", None, "ct"), ("CT", "computerized tomography (CT)", "ct"),
    ("ECoG", None, "ieeg"), ("electrocorticography", None, "ieeg"), ("sEEG", None, "ieeg"),
    ("ECG", None, "physiological"), ("EOG", None, "physiological"), ("EMG", None, "physiological"),
    ("eye-tracking", None, "physiological"), ("eye tracking", None, "physiological"),
    ("nasal pressure recording", None, "physiological"),
    ("OtherModality", "eye tracking", "physiological"),
    # `other`, read through its label
    ("other", "CT", "ct"), ("other", "computed tomography (CT)", "ct"),
    ("other", "whole-brain research CT scans without contrast", "ct"), ("other", "CBCT", "ct"),
    ("other", "X-ray micro-CT", "ct"), ("other", "postoperative CT", "ct"),
    ("other", "subdural electrocorticography (ECoG)", "ieeg"),
    ("other", "stereoelectroencephalography (SEEG)", "ieeg"),
    ("other", "intracranial EEG (iEEG)", "ieeg"),
    ("other", "local field potentials (LFPs)", "ieeg"),
    ("other", "extracellular single-unit recordings", "ieeg"),
    ("other", "stereotactically placed depth electrodes (sEEGs)", "ieeg"),
    ("other", "1H-MRS", "mrs"), ("other", "fMRS", "mrs"), ("other", "MRS", "mrs"),
    ("other", "high-density diffuse optical tomography (HD-DOT)", "fnirs"),
    ("other", "event-related optical signal (EROS)", "fnirs"),
    ("other", "electromyography (EMG)", "physiological"),
    ("other", "MRI-compatible EMG", "physiological"),
    ("other", "MR-compatible infrared EYElink 1000 eye-tracker system", "physiological"),
    ("other", "fMRI-compatible high-speed eye-tracker", "physiological"),
    ("other", "electrocardiogram (ECG)", "physiological"),
    ("other", "skin conductance response (SCR)", "physiological"),
    ("other", "Pupil diameter", "physiological"),
    ("other", "photoplethysmography (PPG)", "physiological"),
    ("other", "respiration", "physiological"),
    ("other", "magnetoencephalography (MEG)", "meg"),
    # an acquisition outside the vocabulary stays `other`
    ("other", "transcranial magnetic stimulation", "other"),
    ("other", "functional transcranial Doppler", "other"),
    ("other", "swept-source optical coherence tomography angiography (SS-OCTA)", "other"),
    ("other", None, "other"),
    ("human developmental anatomy", None, "other"),
])
def test_wording_maps(value, label, expected):
    reading = modality.classify(value, label)
    assert (reading.status, reading.value) == ("mapped", expected)


@pytest.mark.parametrize("value, label", [
    ("other", "BrainMap® functional database"), ("other", "Neurosynth database"),
    ("other", "coordinate-based meta-analysis (CBMA)"),
    ("meta-analytic connectivity modeling", None), ("coordinate-based ALE meta-analysis", None),
    ("other", "whole-exome sequencing (WES)"), ("other", "RNA-seq"),
    ("other", "Immunohistochemistry"), ("other", "confocal laser scanning microscopy"),
    ("other", "Simulated MEG data"), ("other", "Molecular docking"),
    ("other", "Quantitative autoradiographic receptor binding"),
])
def test_not_an_acquisition_is_unmapped(value, label):
    reading = modality.classify(value, label)
    assert (reading.status, reading.value) == ("unmapped", None)
    assert reading.method.startswith("not an acquisition")


@pytest.mark.parametrize("value, label", [
    ("CT or MRI", None),
    ("other", "fMRI or positron emission tomography (PET)"),
    ("OtherModality", "fMRI and positron emission tomography (PET) studies"),
])
def test_two_modalities_are_ambiguous(value, label):
    reading = modality.classify(value, label)
    assert (reading.status, reading.value) == ("ambiguous", None)


def test_unreported_modality_is_not_read():
    assert modality.classify(None, None) is None


def readings(case):
    return {k: (r.status, r.value) for k, r in modality.read(DATA[case]["record"]).items()}


def test_bare_mri_takes_its_analyses_measure():
    # an ASL acquisition whose one analysis is cerebral blood flow: fMRI, as the schema defines it
    assert readings("cbf") == {"acq_smri": ("mapped", "smri"), "acq_asl": ("mapped", "fmri"),
                               "acq_fmri": ("mapped", "fmri")}
    assert readings("mrs") == {f"acq_mrs_{t}": ("mapped", "mrs") for t in ("15t", "3t", "4t")}


def test_measure_a_sibling_explains_is_no_evidence():
    # gray matter volume is the linked sMRI acquisition's; the MRI acquisition (ASL) is not refined
    assert readings("gmv")["acq_asl"] == ("mapped", "mri")
    # diffusion is the dMRI acquisition's; the bare MRI beside it (spectroscopy) stays MRI
    assert readings("diff")["acq_mrs"] == ("mapped", "mri")
    assert readings("mixed")["acq_asl"] == ("mapped", "mri")


def test_a_sibling_of_another_modality_leaves_the_evidence():
    # lesions traced on MRI beside CT: the CT does not explain a structural measure away
    assert readings("ct") == {"acq_mri": ("mapped", "smri"), "acq_ct": ("mapped", "ct")}


def test_other_acquisitions_in_real_records():
    assert readings("ecog") == {"acq_ecog_nihon": ("mapped", "ieeg"),
                                "acq_ecog_blackrock": ("mapped", "ieeg")}
    assert readings("notacq") == {"acq_brainmap": ("unmapped", None)}
    assert readings("physio_compat")["acq_emg"] == ("mapped", "physiological")


@pytest.mark.parametrize("measures, expected", [
    ({"bold_response"}, "fmri"), ({"cerebral_blood_flow"}, "fmri"),
    ({"connectivity_strength"}, "fmri"),
    ({"gray_matter_volume", "cortical_thickness"}, "smri"),
    ({"magnetic susceptibility"}, "smri"),
    ({"myelin water fraction"}, "smri"), ({"diffusion_metric"}, "dmri"),
    ({"31P metabolite concentration"}, "mrs"),
    ({"bold_response", "gray_matter_volume"}, "mri"),        # disagree
    ({"component_loading"}, "mri"), ({"postural stability"}, "mri"), (set(), "mri"),
])
def test_refine_mri(measures, expected):
    assert modality.refine_mri(measures)[0] == expected


def test_every_value_has_a_label_and_parents_are_values():
    assert set(modality.LABELS) == set(modality.VALUES)
    assert set(modality.PARENT) <= set(modality.VALUES)
    assert set(modality.PARENT.values()) <= set(modality.VALUES)

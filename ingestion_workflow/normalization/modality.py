"""`Acquisition.modality` onto a closed vocabulary, `acquisition_modality`.

The vocabulary is study_schema's Modality, lower-cased, plus four values its `other` and `MRI`
hide: `ct`, `ieeg` (ECoG, sEEG, LFP, units) and `physiological` (EMG, ECG, EDA, eye and pupil,
pulse, respiration, movement) under `other`, and `mrs` under `mri`.

- A schema value maps to itself.
- `other`, `OtherModality` and free text are read through the wording: the OtherModality's
  `modality_label`, else the value.
- A wording no rule reaches stays `other`. One that names no acquisition (a database,
  sequencing, histology) is unmapped. One that names two modalities is ambiguous.
- A bare `MRI` takes fMRI, sMRI, dMRI or MRS from the measures of the analyses that use it, when
  they agree. An analysis whose other acquisition already has the measure's modality is not
  evidence: the extractor links every acquisition of a session to each analysis.
"""
import re
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

VOCABULARY = "acquisition_modality"

LABELS = {
    "fmri": "functional MRI", "smri": "structural MRI", "dmri": "diffusion MRI",
    "mrs": "MR spectroscopy", "mri": "MRI", "eeg": "EEG", "ieeg": "intracranial EEG",
    "meg": "MEG", "fnirs": "fNIRS", "pet": "PET", "spect": "SPECT", "ct": "CT",
    "physiological": "peripheral physiological recording", "other": "other",
}
VALUES = tuple(LABELS)
PARENT = {"fmri": "mri", "smri": "mri", "dmri": "mri", "mrs": "mri",
          "ct": "other", "ieeg": "other", "physiological": "other"}

#: The schema's values, lower-cased, that map to themselves.
SCHEMA = {"fmri", "smri", "dmri", "mri", "eeg", "fnirs", "pet", "meg", "spect"}
OTHER = {"other", "othermodality"}

RULES = tuple((value, re.compile(pattern, re.I)) for value, pattern in (
    ("fmri", r"\bfmri\b|functional (?:mri|magnetic resonance)|\bbold\b|arterial spin|\basl\b"),
    ("smri", r"\bsmri\b|structural (?:mri|magnetic resonance)|anatomical (?:mri|scan)|\bt1[\s-]?weighted\b"),
    ("dmri", r"\bdmri\b|\bdti\b|\bdwi\b|diffusion[\s-](?:mri|tensor|weighted)|tractograph"),
    ("mrs", r"\bf?mrs\b|\b1h[\s-]?mrs|\b31p\b|(?:magnetic resonance|mr) spectroscop"),
    ("mri", r"\bmri\b|magnetic resonance"),
    ("ieeg", r"\becogs?\b|electrocortico|\bseegs?\b|stereo-?electroenceph|stereotactic (?:eeg|electroenceph)|"
             r"\bieeg\b|intracranial|intra-?cerebral|depth electrode|subdural|\blfps?\b|local field potential|"
             r"single[\s-]unit|multi-?unit|micro-?(?:wire|electrode)|extracellular|\bifps?\b"),
    ("eeg", r"\beeg\b|electroenceph|(?<!motor )(?<!motor-)evoked potential|\berps?\b"),
    ("meg", r"\bmeg\b|magneto-?enceph"),
    ("fnirs", r"\bf?nirs\b|near[\s-]?infrared spectroscop|diffuse optical|\bhd-dot\b|\bdot\b|"
              r"event-related optical|\beros\b"),
    ("pet", r"\bpet\b|positron emission"),
    ("spect", r"\bspect\b|single[\s-]photon emission"),
    ("ct", r"(?<![a-z])(?:micro-?)?ct(?![a-z])|\bc?cbct\b|\bcct\b|(?<!emission )computed tomograph|"
           r"computeri[sz]ed (?:axial )?tomograph|x-ray (?:micro)?tomograph|microtomograph"),
    ("physiological", r"\bemgs?\b|electromyo|\becgs?\b|\bekg\b|electrocardio|eye[\s-]?(?:track|movement|position)|"
                      r"eyetrack|\beyelink|pupil|\bgaze\b|oculo|\bocular|\beogs?\b|saccad|skin conduct|"
                      r"electroderm|\bscrs?\b|\beda\b|\bgsr\b|plethysmo|\bppgs?\b|\bpulse\b|respirat|breath|"
                      r"heart|cardi|\begg\b|electrogastro|electrointestin|accelero|\bimu\b|kinemat|"
                      r"motion (?:capture|track)|body tracking|sympathetic nerve|blood pressure|polysomno|"
                      r"nasal pressure|tumescence|motor[\s-]evoked|\bmeps?\b"),
))

#: A value hit alongside one it refines is the one meant: iEEG over EEG, PET-CT is PET.
DOMINATES = {"ieeg": {"eeg"}, "mrs": {"mri"}, "fmri": {"mri"}, "smri": {"mri"}, "dmri": {"mri"},
             "pet": {"ct"}, "spect": {"ct"}}

#: Names something that is not an acquisition: decisive over every rule above.
NOT_ACQUISITION = re.compile(
    r"meta[\s-]?analy|brainmap|neurosynth|\bale\b|database|coordinate|literature|atlas|simulat|"
    r"sequenc|genom|exome|rna-?seq|\b[dr]na\b|methylation|genotyp|genechip|microarray|chip-chip|"
    r"histolog|immuno|microscop|confocal|autoradiograph|tracer stud|\btracing\b|docking|stain|"
    r"in situ hybrid|mass spectrometr|\bgc-ms\b|questionnaire|rainfall|meteorolog|water sample|lidar|"
    r"seismic|seafloor|geographic|density functional|dipole moment|particle trajector", re.I)

#: "MRI-compatible EMG" is EMG.
COMPATIBLE = re.compile(r"\b(?:f?mri|mr|magnetic resonance)[\s-](?:compatible|conditional)\b", re.I)

MEASURE = {
    **dict.fromkeys(("bold_response", "bold_derived_metric", "cerebral_blood_flow", "cerebral_blood_volume",
                     "connectivity_strength", "decoding_performance", "representational_similarity"), "fmri"),
    **dict.fromkeys(("gray_matter_volume", "gray_matter_density", "white_matter_volume", "cortical_thickness",
                     "surface_area", "structural_morphometry_other"), "smri"),
    "diffusion_metric": "dmri",
}
#: Free-text measure types; a MeasureType value outside MEASURE decides nothing.
MEASURE_TEXT = tuple((value, re.compile(pattern, re.I)) for value, pattern in (
    ("mrs", r"metabolite|spectroscop|\bgaba\b|glutam|\bnaa\b"),
    ("dmri", r"diffusion|anisotrop|tractograph"),
    ("smri", r"susceptib|relax|myelin|magneti[sz]ation transfer|lesion|atroph|morpholog"),
    ("fmri", r"perfusion|blood flow|\bcbf\b|\bbold\b|\balff\b|\breho\b"),
))
MEASURE_TYPES = {"metabolic_rate", "receptor_binding", "tracer_uptake", "electrophysiological_amplitude",
                 "electrophysiological_power", "component_loading", *MEASURE}


@dataclass(frozen=True)
class Reading:
    status: str
    value: Optional[str] = None
    method: Optional[str] = None
    also_read: Optional[Tuple[str, ...]] = None


def _match(text: str) -> Tuple[str, ...]:
    text = COMPATIBLE.sub(" ", text)
    hits = {value for value, rx in RULES if rx.search(text)}
    hits -= {lower for value in hits for lower in DOMINATES.get(value, ())}
    return tuple(v for v in VALUES if v in hits)


def classify(value: Optional[str], label: Optional[str] = None) -> Optional[Reading]:
    """One acquisition's modality and OtherModality label -> its reading; None when unstated."""
    if not isinstance(value, str) or not value.strip():
        return None
    key = value.strip().lower()
    if key in SCHEMA:
        return Reading("mapped", key, "schema value")
    texts = [(value, None)] if key not in OTHER else []
    if isinstance(label, str) and label.strip():
        texts.append((label, ("modality_label",)))
    for text, read in texts:
        found = NOT_ACQUISITION.search(text)
        if found:
            return Reading("unmapped", None, f"not an acquisition: {found.group().lower()}", read)
        hits = _match(text)
        if len(hits) > 1:
            return Reading("ambiguous", None, "matches " + " and ".join(hits), read)
        if hits:
            return Reading("mapped", hits[0], "lexical", read)
    return Reading("mapped", "other", "no rule names it", texts[-1][1] if texts else None)


def measure_class(measure_type: Optional[str]) -> Optional[str]:
    """The MRI modality a measure type implies, or None."""
    if not isinstance(measure_type, str):
        return None
    if measure_type in MEASURE_TYPES:
        return MEASURE.get(measure_type)
    return next((value for value, rx in MEASURE_TEXT if rx.search(measure_type)), None)


def refine_mri(measure_types: Iterable[str]) -> Tuple[str, str]:
    """A bare MRI acquisition's modality from its analyses' measure types: (value, method)."""
    types = sorted({t for t in measure_types if isinstance(t, str)})
    classes = {measure_class(t) for t in types} - {None}
    if len(classes) == 1:
        return classes.pop(), "measure: " + ", ".join(types)
    if classes:
        return "mri", "measures disagree: " + ", ".join(sorted(classes))
    return "mri", "no measure decides"


def _v(x):
    return x.get("value") if isinstance(x, dict) else x


def _stated(slot):
    """A slot's value when extracted (or bare), else None."""
    if isinstance(slot, dict) and slot.get("extraction_status", "extracted") != "extracted":
        return None
    return _v(slot)


def read(record: dict) -> Dict[str, Reading]:
    """Each acquisition's reading, by local id: `classify`, with a bare MRI refined by `refine_mri`."""
    acqs = {str(_v(a.get("local_id"))): a for a in record.get("acquisitions") or [] if isinstance(a, dict)}
    base = {aid: classify(_stated(a.get("modality")), _stated(a.get("modality_label"))) for aid, a in acqs.items()}
    measures = {str(_v(m.get("local_id"))): _stated(m.get("type"))
                for m in record.get("measures") or [] if isinstance(m, dict)}
    evidence = {aid: [] for aid, r in base.items() if r is not None and r.value == "mri"}
    for analysis in record.get("analyses") or []:
        if not isinstance(analysis, dict):
            continue
        linked = [str(x) for x in analysis.get("acquisitions") or [] if isinstance(x, str)]
        mid = analysis.get("measure")
        mid = mid[0] if isinstance(mid, list) and mid else mid
        mtype = measures.get(str(mid))
        implied = measure_class(mtype)
        for aid in linked:
            if aid not in evidence or mtype is None:
                continue
            siblings = {base[o].value for o in linked if o != aid and base.get(o) is not None}
            if implied is None or implied not in siblings:
                evidence[aid].append(mtype)
    out = {aid: r for aid, r in base.items() if r is not None}
    for aid, types in evidence.items():
        value, method = refine_mri(types)
        out[aid] = Reading("mapped", value, method)
    return out

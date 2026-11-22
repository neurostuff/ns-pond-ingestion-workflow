"""A paper's acquisition modalities read off its methods section, for papers whose record
states none.

The methods section is the one `coordinate_space.sectionize` finds, cut at a reference list;
the whole text when no heading names one. A modality is read when it is mentioned in at least
`MIN_MENTIONS` times in sentences that are not about something else: a cited study, the 10–20
electrode system, a clinical EEG workup, a receptor atlas, or TMS neuronavigation. A generic
MRI counts only when no functional, structural or diffusion MRI is read. Values are
`normalization.modality`'s vocabulary.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List

from .coordinate_space import sectionize

#: Matched case-sensitively: an acronym in capitals is the modality; phrases carry `(?i:)`.
RULES = {name: re.compile(pattern) for name, pattern in {
    "fmri": r"\bfMRI\b|\bBOLD\b|\bASL\b|T2\*|(?i:functional (?:magnetic resonance|MRI?)\b"
            r"|blood[- ]oxygen(?:ation)?[- ]level|arterial spin label)",
    "smri": r"\bVBM\b|MP-?RAGE|T1[- ]weighted|\bSPGR\b|FreeSurfer|(?i:voxel[- ]based morphometry"
            r"|cortical thickness|gr[ae]y matter (?:volume|density))",
    "dmri": r"\bDTI\b|\bDWI\b|\bdMRI\b|\bHARDI\b|\bNODDI\b"
            r"|(?i:diffusion[- ](?:tensor|weighted|MRI)|fractional anisotropy|tractograph)",
    "pet": r"\bPET\b|\bFDG\b|\bPiB\b|(?i:positron emission|raclopride)|\[\s?1[18]\s?[CF]\s?\]"
           r"|\b18F-|\b11C-|H2\s?15O|\[15O\]|15O-water",
    "spect": r"\bSPECT\b|(?i:single[- ]photon emission)|99m\s?Tc|HMPAO|\[123I\]|\b123I-"
             r"|DaTscan|FP-CIT",
    "eeg": r"\bEEG\b|\bERPs?\b|(?i:electroencephalogra|event[- ]related potentials?)",
    "meg": r"\bMEG\b|(?i:magneto-?encephalogra)",
    "fnirs": r"\bf?NIRS\b|(?i:near[- ]infrared spectroscopy)",
    "mri": r"\bMRI\b|(?i:magnetic resonance imag|\bMR (?:scanner|imaging|system))"
           r"|\b[1-9](?:\.\d)?[- ]?T(?:esla)?\b[^.\n]{0,40}(?i:scanner)",
}.items()}
#: A sentence that names a modality without the paper acquiring it.
NOT_ACQUIRED = {
    "*": re.compile(r"(?i)\b(?:previous|prior|earlier|published)\b[^.]{0,80}\bstud(?:y|ies)\b"),
    "eeg": re.compile(r"(?i)10\s?[-–/]\s?20|ten-twenty|diagnos|findings|video[- ]?EEG|EEG[- ]video"
                      r"|monitoring|\bfocus\b"),
    "pet": re.compile(r"(?i)JuSpace|-derived map|atlas|neurotransmitter map"),
    "spect": re.compile(r"(?i)JuSpace|-derived map|atlas|neurotransmitter map"),
    "mri": re.compile(r"(?i)navigat|Softaxic|template|estimated"),
}
REFERENCES = re.compile(r"(?im)^[ \t#]*(?:references?|bibliography|literature cited)[ \t:]*$")
SENTENCE = re.compile(r"(?<=[.!?])\s+|\n+")
MIN_MENTIONS = 2
MRI_KINDS = ("fmri", "smri", "dmri")


@dataclass
class Reading:
    modalities: List[str]
    #: "methods", or "full_text" when no heading names a methods section.
    where: str
    #: Mentions counted per modality.
    mentions: Dict[str, int] = field(default_factory=dict)


def methods_text(text: str) -> str:
    """The methods sections, each cut at a reference list it holds; "" when there are none."""
    parts = []
    for start, end, label in sectionize(text):
        if label == "methods":
            part = text[start:end]
            cut = REFERENCES.search(part)
            parts.append(part[:cut.start()] if cut else part)
    return "\n".join(parts)


def counts(text: str) -> Counter:
    """Mentions of each modality, leaving out the sentences NOT_ACQUIRED names."""
    seen: Counter = Counter()
    for sentence in SENTENCE.split(text):
        if NOT_ACQUIRED["*"].search(sentence):
            continue
        for name, rule in RULES.items():
            skip = NOT_ACQUIRED.get(name)
            if not (skip and skip.search(sentence)):
                seen[name] += len(rule.findall(sentence))
    return +seen


def read(text: str) -> Reading:
    methods = methods_text(text)
    where = "methods" if methods.strip() else "full_text"
    if where == "full_text":
        cut = REFERENCES.search(text)
        methods = text[:cut.start()] if cut else text
    seen = counts(methods)
    found = {name for name, n in seen.items() if n >= MIN_MENTIONS and name != "mri"}
    if seen["mri"] >= MIN_MENTIONS and not found & set(MRI_KINDS):
        found.add("mri")
    return Reading(sorted(found), where, dict(sorted(seen.items())))

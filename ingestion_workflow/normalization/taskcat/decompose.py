"""Decompose a task name into paradigm stem + canonical task + qualifiers-by-axis.

family -> paradigm stem -> canonical task (hed-task id or tct_*) -> variant (hedvar_*) + qualifiers
The stem groups hed-task tasks that share a procedure skeleton and differ in content (hed-task keeps e.g. Emotional
Stroop as its own task; the stem lets queries span both). Qualifier axes follow study_schema's TaskVariantAxis
(stimulus_content, stimulus_modality, response_modality, design, adaptation) plus 'population' and 'other'.
"""
import re

from .lexicon import Lexicon, _norm

STEMS = {  # stem -> canonical ids sharing a procedure skeleton
    "stroop": ["hedtsk_stroop_color_word", "hedtsk_emotional_stroop"],
    "incentive_delay": ["hedtsk_monetary_incentive_delay", "hedtsk_social_incentive_delay"],
    "n_back": ["hedtsk_n_back"], "go_no_go": ["hedtsk_go_no_go"], "stop_signal": ["hedtsk_stop_signal"],
    "flanker": ["hedtsk_eriksen_flanker"], "oddball": ["hedtsk_oddball", "hedtsk_mismatch_negativity"],
    "item_recognition_wm": ["hedtsk_sternberg_item_recognition", "hedtsk_delayed_match_to_sample"],
    "picture_viewing": ["hedtsk_affective_picture_viewing", "tct_cue_reactivity"],
    "face_perception": ["hedtsk_face_processing", "hedtsk_facial_emotion_recognition", "tct_emotional_face_matching"],
    "economic_game": ["hedtsk_ultimatum_game", "hedtsk_trust_game", "hedtsk_dictator_game", "hedtsk_prisoners_dilemma"],
    "conditioning": ["hedtsk_pavlovian_fear_conditioning", "tct_fear_extinction", "hedtsk_instrumental_conditioning"],
    "rest": ["hedtsk_rest"],
}
STEM_OF = {k: s for s, ks in STEMS.items() for k in ks}

AXES = [
    ("stimulus_content", r"\b(emotion(al)?|affective|fear(ful)?|threat|angry|happy|sad|disgust|neutral|aversive|"
                         r"food|alcohol|drug|smoking|cigarette|cocaine|cannabis|nicotine|gambling|sexual|erotic|"
                         r"face|faces|facial|word|words|verbal|letters?|digits?|numerical|number|spatial|object|"
                         r"picture|pictorial|scene|social|self|money|monetary|pain|music|semantic|phonological|"
                         r"body|tool|animal|house|color|colour|shape)\b"),
    ("stimulus_modality", r"\b(visual|auditory|tactile|olfactory|gustatory|audiovisual|somatosensory|vibrotactile)\b"),
    ("response_modality", r"\b(vocal|verbal response|manual|button|saccad\w*|oculomotor|covert|overt|silent|spoken|joystick)\b"),
    ("design", r"\b(block(ed)?|event[- ]related|mixed|parametric|jittered|rapid|slow|sparse)\b"),
    ("adaptation", r"\b(modified|adapted|adaptive|short(ened)?|simplified|child(ren)?['s]*|pediatric|version|"
                   r"computeri[sz]ed|virtual|novel|revised|hybrid|dual)\b"),
    ("population", r"\b(patients?|elderly|older|infants?|adolescents?)\b"),
]
PROMOTE_EMO = r"emotion|affective|threat|drug|alcohol|smok|cigarette|cocaine|food|pain|trauma|suicide|negative words?|addiction"
FILLER = r"\b(task|tasks|paradigm|test|game|experiment|procedure|fmri|the|a|an|of|and|with|in|for|during|condition|run|session)\b"


AXIS_WORDS = {"emotional", "emotion", "affective", "auditory", "visual", "verbal", "spatial", "numerical", "face", "facial",
              "word", "words", "food", "drug", "social", "monetary", "vocal", "manual", "tactile", "pictorial", "picture"}


class Decomposer:
    def __init__(self, lexicon=None):
        self.L = lexicon or Lexicon()

    def __call__(self, name, paradigm_term=None):
        m = self.L.match(paradigm_term) or self.L.match(name)
        out = {"canonical_task": m[0] if m else None, "variant": m[1] if m else None,
               "stem": STEM_OF.get(m[0], m[0]) if m else None,
               "family": (self.L.family.get(m[0]) or [None])[0] if m else None, "qualifiers": []}
        # promotions that follow hed-task's own variant lists
        if out["canonical_task"] == "hedtsk_stroop_color_word" and re.search(PROMOTE_EMO, name or "", re.I):
            out["canonical_task"] = "hedtsk_emotional_stroop"
        rest = " " + re.sub(r"[-/]", " ", _norm(name)) + " "
        if m:
            core = set(re.sub(r"[-/]", " ", _norm(self.L.names.get(m[0], ""))).split())
            core |= set(re.sub(r"[-/]", " ", _norm(m[3] or "")).split()) - AXIS_WORDS
            for w in core:
                rest = re.sub(rf"\b{re.escape(w)}\b", " ", rest)
        rest = re.sub(FILLER, " ", rest, flags=re.I)
        used = set()
        for axis, pat in AXES:
            for q in re.finditer(pat, rest, re.I):
                w = q.group(0).lower()
                if w not in used:
                    out["qualifiers"].append({"text": w, "axis": axis}); used.add(w)
        left = [w for w in re.findall(r"[a-z][a-z'-]+", rest) if w not in used and len(w) > 2]
        if left:
            out["qualifiers"].append({"text": " ".join(left), "axis": "other"})
        # hed-task's separate-task cases carry their defining qualifier implicitly
        out["qualifiers"] = [q for q in out["qualifiers"] if not (q["axis"] == "other" and "-" in q["text"])]
        if out["canonical_task"] == "hedtsk_emotional_stroop" and not any(q["axis"] == "stimulus_content" for q in out["qualifiers"]):
            out["qualifiers"].append({"text": "emotional", "axis": "stimulus_content", "implied_by": "canonical"})
        return out

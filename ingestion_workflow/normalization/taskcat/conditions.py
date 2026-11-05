"""Condition normalization: verbatim condition name -> kind + phase + axis:value atoms.

kind follows study_schema ConditionKind (task_state | control_state | rest | fixation).
Atoms are (axis, value) pairs. Axes line up with facets.AXES and BrainMap's 'Stimulus Content'. A condition may
carry several atoms ("negative reappraise" -> valence:negative + regulation:reappraise). Unmatched words are kept in
`residue` for corpus-level clustering.
"""
import re

KIND = [("fixation", r"\bfixat|\bcross(hair)?\b|\bblank screen\b"),
        ("rest", r"\brest(ing)?\b|\bbaseline rest|\boff\b block"),
        ("control_state", r"\bcontrol\b|\bbaseline\b|\bneutral task\b|\bscrambled\b|\bsensorimotor control|\bshapes?\b(?= (matching|condition))|"
                          r"\b0[- ]back\b|\bzero[- ]back\b|\bpassive\b(?! avoidance)|\blow[- ]level\b|\bnull\b|^shapes?\b|\bshape matching")]
PHASE = [("anticipation", r"anticipat|expectan"), ("cue", r"\bcues?\b|\bcue period|precue"),
         ("outcome", r"outcome|feedback|consummat|receipt|win(ning)? outcome"), ("encoding", r"encod|\bstudy\b|learning phase|memoriz"),
         ("retrieval", r"retriev|recall|recogni|\btest phase|\btest\b"), ("delay", r"\bdelay|maintenance|retention interval"),
         ("decision", r"decision|choice phase|\bchoose|\bselection"), ("response", r"response (phase|period)|motor response"),
         ("rating", r"\brating|\brate\b|evaluation phase"), ("instruction", r"instruction"), ("stimulus", r"stimulus (phase|period)|viewing phase|presentation phase")]
ATOMS = [  # (axis, value, pattern)
    ("congruency", "incongruent", r"\bincongru|\bconflict\b|\bmismatch|\bincompatib"),
    ("congruency", "congruent", r"(?<!in)congruent|(?<!in)\bcompatible\b|(?<!colour )(?<!color )(?<!shape )\bmatch(ing|ed)?\b(?! task)"),
    ("load", "0", r"\b0[- ]?back\b|\bzero[- ]back\b"), ("load", "1", r"\b1[- ]?back\b|\bone[- ]back\b"),
    ("load", "2", r"\b2[- ]?back\b|\btwo[- ]back\b"), ("load", "3", r"\b3[- ]?back\b|\bthree[- ]back\b|\bn3\b|\b3 (letters|items|digits)\b"),
    ("load", "high", r"high[- ]load|\bhigh (memory|wm|working)|\bhard\b|\bdifficult\b|\bcomplex\b"),
    ("load", "low", r"low[- ]load|\blow (memory|wm|working)|\beasy\b|\bsimple\b"),
    ("response_demand", "nogo", r"no[- ]?go\b"), ("response_demand", "go", r"(?<!no)(?<!no-)(?<!no )\bgo\b"),
    ("response_demand", "stop", r"\bstop\b|\bsuccessful stop|\bfailed stop|\bunsuccessful stop"),
    ("switch", "switch", r"\bswitch(?!ing task)|\bshift\b"), ("switch", "repeat", r"\brepeat(ed)?\b|\brepetition\b|\bstay\b"),
    ("cue_validity", "invalid", r"\binvalid"), ("cue_validity", "valid", r"(?<!in)\bvalid"),
    ("novelty", "new", r"\bnew\b|\bnovel\b|\bunfamiliar\b|\bunstudied\b|\blure"), ("novelty", "old", r"\bold\b|\bstudied\b|\bfamiliar\b|\brepeated items?\b"),
    ("memory_outcome", "remembered", r"\bremember(?!ed target)|\bhits?\b|\bsubsequently remembered|\bcorrect(ly)? recogni"),
    ("memory_outcome", "forgotten", r"\bforgot|\bmiss(es)?\b|\bsubsequently forgotten"),
    ("emotion", "fear", r"fear|afraid|scared"), ("emotion", "anger", r"\bang(ry|er)\b"), ("emotion", "happy", r"happ(y|iness)|smil|joy"),
    ("emotion", "sad", r"\bsad"), ("emotion", "disgust", r"disgust"), ("emotion", "surprise", r"surpris"), ("emotion", "pain", r"\bpain"),
    ("valence", "negative", r"negative|unpleasant|aversive|threat|\bnegat"), ("valence", "positive", r"positive|(?<!un)pleasant(?!ness)|appetitive"),
    ("valence", "neutral", r"\bneutral"),
    ("outcome", "gain", r"\bgain|\bwin(s|ning)?\b|(?<!non-)(?<!non )(?<!non)\breward(ed|ing)?\b(?! anticipation)|(?<!non-)(?<!non)\bincentive"),
    ("outcome", "loss", r"\bloss|\blose|\blosing|\bpunish"), ("outcome", "no_outcome", r"no[- ](reward|loss|gain|outcome)|\bnon[- ]?(incentive|reward)"),
    ("magnitude", "high", r"\$?\b(5|10|large|big|high)\b.{0,10}(reward|gain|loss|\$)|\blarge (reward|gain|loss)"),
    ("magnitude", "low", r"\b(small|low)\b.{0,10}(reward|gain|loss)|\$?0\.2"),
    ("regulation", "reappraise", r"reapprais|reinterpret|distanc|decrease|down[- ]?regulat"),
    ("regulation", "suppress", r"suppress"), ("regulation", "increase", r"\bincrease|up[- ]?regulat|enhance"),
    ("regulation", "look", r"\blook(ing)?\b|\bwatch\b|\bmaintain (negative|emotion|feeling)|attend naturally"),
    ("referent", "self", r"\bself\b|\bown\b|\bmyself\b"), ("referent", "other", r"\bother(s)?\b|\bfriend|\bstranger|\bclose other"),
    ("social", "social", r"\bsocial\b|\binclusion\b|\bexclusion\b"), ("social", "exclusion", r"exclu"), ("social", "inclusion", r"inclu"),
    ("stimulus", "faces", r"\bfaces?\b|facial"), ("stimulus", "houses_scenes", r"\bhouses?\b|\bscenes?\b|\bplaces?\b"),
    ("stimulus", "words", r"(?<!non[- ])(?<!non)(?<!pseudo[- ])(?<!pseudo)\bwords?\b|\bnouns?\b|\bverbs?\b"), ("stimulus", "pseudowords", r"pseudo[- ]?words?|non[- ]?words?"),
    ("stimulus", "objects", r"\bobjects?\b|\btools?\b"), ("stimulus", "bodies", r"\bbod(y|ies)\b"),
    ("stimulus", "food", r"(?<!non[- ])(?<!non)\bfood|\bmeal|\bsnack"), ("stimulus", "drug_cue", r"alcohol|cigarette|smok|drug|cocaine|cannabis"),
    ("stimulus", "speech", r"\bspeech\b|\bspoken\b|\bsentences?\b"), ("stimulus", "tones", r"\btones?\b|\bbeeps?\b"),
    ("stimulus", "scrambled", r"scrambl"), ("modality", "visual", r"\bvisual\b"), ("modality", "auditory", r"\bauditory\b"),
    ("modality", "tactile", r"\btactile|\btouch"),
    ("intensity", "high", r"\bhigh (pain|intensity|heat)|\bpainful\b|\bstrong\b"), ("intensity", "low", r"\blow (pain|intensity|heat)|\bwarm\b|\bweak\b|\bnon[- ]?painful"),
    ("accuracy", "error", r"\berror|\bincorrect|\bwrong\b"), ("accuracy", "correct", r"\bcorrect\b"),
    ("cs", "cs_plus", r"\bcs\s*\+|\bcs[- ]?plus|\bcs\+"), ("cs", "cs_minus", r"\bcs\s*-|\bcs[- ]?minus|\bcs−"),
    ("probability", "high", r"high[- ]probab|\blikely\b"), ("probability", "low", r"low[- ]probab|\bunlikely\b"),
    ("delay", "immediate", r"\bimmediate|\bnow\b"), ("delay", "delayed", r"\bdelayed\b|\blater\b"),
    ("speed", "fast", r"\bfast\b|\bspeed"), ("speed", "slow", r"\bslow\b"),
    ("side", "left", r"\bleft\b"), ("side", "right", r"\bright\b"),
    ("arousal", "high", r"high[- ]arous"), ("arousal", "low", r"low[- ]arous|\bcalm\b"),
    ("trial_type", "catch", r"\bcatch\b|procatch"), ("trial_type", "null", r"\bnull\b"),
    ("stop_outcome", "success", r"(?<!un)successful(ly)? (stop|inhibit)|stop[- ]?success|(?<!un)\bsuccess"),
    ("stop_outcome", "failure", r"(failed|unsuccessful) (stop|inhibit)|stop[- ]?fail|\bfailure|\bfail(ed)? ?stop"),
    ("fluency_type", "phonemic", r"phonemic|letter fluency|\bfas\b|initial letter"), ("fluency_type", "semantic", r"semantic fluency|category fluency|animals? naming"),
    ("word_frequency", "high", r"high[- ]frequency"), ("word_frequency", "low", r"low[- ]frequency"),
    ("concreteness", "concrete", r"\bconcrete\b"), ("concreteness", "abstract", r"\babstract\b"),
    ("trial_type", "deviant", r"\bdeviant|\boddball\b|\brare\b|\btarget tones?"), ("trial_type", "standard", r"\bstandard\b|\bfrequent\b"),
    ("trial_type", "lure", r"\blures?\b"), ("trial_type", "pm_cue", r"prospective memory cue|\bpm cue"),
    ("mentalizing", "mental", r"mentaliz|theory[- ]of[- ]mind|\btom\b|false[- ]belief|intention"),
    ("mentalizing", "physical", r"\brandom (movement|motion)|false[- ]photo|physical (causal|story|stories|reasoning)"),
    # --- added 2026-10-30 from the Astra residue and Condition.stimulus_content (docs/astra-smoke-test.md, "Atoms v2")
    ("stimulus", "nonfood", r"non[- ]?food"), ("stimulus", "letters", r"\bletters?\b|\bconsonants?\b"),
    ("stimulus", "money", r"\bmoney\b|\bmonetary\b|\bcash\b|\bcoins?\b"), ("stimulus", "numbers", r"\bdigits?\b|\bnumbers?\b|\bnumerals?\b"),
    ("stimulus", "arrows", r"\barrows?\b"), ("stimulus", "animals", r"\banimals?\b"), ("stimulus", "music", r"\bmusic|\bmelod|\bsongs?\b"),
    ("stimulus", "shapes", r"\bgeometric\b|\bshapes?\b|\bfractals?\b"), ("stimulus", "gratings", r"\bgratings?\b|\bgabor"),
    ("stimulus", "characters", r"\bcharacters?\b"), ("stimulus", "symbols", r"\bsymbols?\b"), ("stimulus", "cards", r"\bcards?\b"),
    ("fairness", "unfair", r"\bunfair"), ("fairness", "fair", r"(?<!un)\bfair\b"),
    ("agent", "human", r"\bhuman\b(?! faces?)|\bperson\b"), ("agent", "computer", r"\bcomputer\b|\bmachine\b|\brobot"),
    ("risk", "risky", r"\brisky\b|\bgambles?\b|\brisk\b"), ("risk", "safe", r"\bsafe\b|\bsure (option|bet|gain)"),
    ("motion", "moving", r"\bmoving\b|\bmotion\b|\bdynamic\b"), ("motion", "static", r"\bstatic\b|\bstationary\b"),
    ("stimulation", "sham", r"\bsham\b"), ("stimulation", "active", r"\bverum\b|\breal (stimulation|tms|tdcs|acupuncture)|\bstim(ulation)?[- ]on\b"),
    ("motor_mode", "imagery", r"\bimager|\bimagin"), ("motor_mode", "observation", r"\bobserv"),
    ("motor_mode", "execution", r"\bexecut|\bmovements?\b|\btapping\b|\bmotor (task|response)"),
    ("engagement", "active", r"\bactive\b"), ("engagement", "passive", r"\bpassive\b(?! avoidance)"),
    ("language_process", "semantic", r"\bsemantic|\bmeaning(ful)?\b"), ("language_process", "phonological", r"\bphonolog|\brhym|\bsyllab"),
    ("language_process", "orthographic", r"\borthograph|\bletter case|\bfonts?\b"), ("language_process", "reading", r"\bread(ing)?\b"),
    ("language_process", "naming", r"\bnam(e|ing)\b"),
    ("expectancy", "unexpected", r"\bunexpected|\bunpredict|\bviolat|\bmispredict"), ("expectancy", "expected", r"(?<!un)\bexpected\b|(?<!un)\bpredictable\b|(?<!mis)\bpredicted\b"),
    ("level", "high", r"\bhigh\b"), ("level", "low", r"\blow\b"),
]
_COMPILED = [(a, v, re.compile(p, re.I)) for a, v, p in ATOMS]
_STOP = {"condition", "conditions", "trials", "trial", "block", "blocks", "task", "the", "and", "of", "vs", "versus", "with",
         "a", "an", "for", "in", "during", "type", "stimuli", "stimulus", "events", "event", "period", "phase"}


def normalize_condition(name):
    n = (name or "").strip()
    low = n.lower()
    kind = next((k for k, p in KIND if re.search(p, low)), "task_state")
    phases = [p for p, pat in PHASE if re.search(pat, low)]
    atoms, spans = [], []
    for axis, val, pat in _COMPILED:
        m = pat.search(low)
        if m and not any(a == axis for a, _ in atoms if axis in ("congruency", "cue_validity", "novelty", "switch")):
            atoms.append((axis, val)); spans.append(m.span())
    # residue: words not covered by any atom/phase/kind match
    words = []
    for w in re.finditer(r"[a-z][a-z'-]+", low):
        a, b = w.span()
        hit = any(m.start() < b and a < m.end() for _, _, pat in _COMPILED for m in pat.finditer(low))
        if not hit and w.group(0) not in _STOP and len(w.group(0)) > 2:
            words.append(w.group(0))
    return {"name": n, "kind": kind, "phases": phases, "atoms": [f"{a}:{v}" for a, v in atoms], "residue": " ".join(words)}


_SPLIT = re.compile(r"\s*(?:>|<|\bminus\b|\bvs\.?\b|\bversus\b|\bcompared (?:to|with)\b|\brelative to\b|\s-\s|−)\s*", re.I)


def normalize_contrast(text):
    """'Incongruent > Congruent' -> {'signature': {'congruency': ('incongruent', 'congruent')}, ...}.
    Sides split on >, <, minus, vs, versus, ' - '. '<' flips the sign."""
    t = (text or "").strip()
    parts = _SPLIT.split(t)
    if len(parts) < 2:
        c = normalize_condition(t)
        return {"text": t, "type": "main_effect_or_single", "pos": c, "neg": None, "signature": {a: (v, None) for a, v in (x.split(":") for x in c["atoms"])}}
    flip = "<" in t and ">" not in t
    pos, neg = normalize_condition(parts[0]), normalize_condition(parts[1])
    if flip:
        pos, neg = neg, pos
    pa = dict(x.split(":") for x in pos["atoms"]); na = dict(x.split(":") for x in neg["atoms"])
    sig = {ax: (pa.get(ax), na.get(ax)) for ax in set(pa) | set(na) if pa.get(ax) != na.get(ax)}
    if neg["kind"] in ("fixation", "rest", "control_state") and not na:
        sig["baseline"] = (None, neg["kind"])
    return {"text": t, "type": "difference", "pos": pos, "neg": neg, "signature": sig}


# Task-context rules: implied atoms and context-dependent readings (from the condition gold check).
_IMPLIED = {  # canonical task -> atoms every condition inherits
    "hedtsk_facial_emotion_recognition": ["stimulus:faces"], "hedtsk_face_processing": ["stimulus:faces"],
    "tct_emotional_face_matching": ["stimulus:faces"], "tct_implicit_emotional_face": ["stimulus:faces"],
    "tct_pain_stimulation": ["emotion:pain"], "tct_empathy_pain_observation": ["emotion:pain"],
    "tct_auditory_stimulation": ["modality:auditory"], "tct_auditory_discrimination": ["modality:auditory"],
    "hedtsk_lexical_decision": ["stimulus:words"], "hedtsk_verb_generation": ["stimulus:words"],
}
_REWARD_FAMILY = {"reward_and_value_choice"}; _CONFLICT_FAMILY = {"conflict_and_interference"}
_EXCLUSIVE = {"valence", "congruency", "cue_validity", "novelty", "switch", "load", "stimulus_pw", "fairness", "agent", "risk",
              "motion", "stimulation", "expectancy", "engagement", "level"}
#: a generic high / low means the manipulated quantity of the family: load in working memory, magnitude in reward,
#: intensity in pain and perception; elsewhere it stays `level`
_LEVEL_AXIS = {"working_memory_span": "load", "reward_and_value_choice": "magnitude", "perceptual_judgment": "intensity"}


def normalize_condition_ctx(name, task=None, family=None):
    c = normalize_condition(name)
    atoms = list(c["atoms"]); low = (name or "").lower()
    if re.search(r"\bneutral\b", low) and "valence:neutral" in atoms:
        if family in _REWARD_FAMILY:
            atoms = [a for a in atoms if a != "valence:neutral"] + ["outcome:no_outcome"]
        elif family in _CONFLICT_FAMILY and not re.search(r"emotion|face|valence", low):
            atoms = [a for a in atoms if a != "valence:neutral"] + ["congruency:neutral"]
    lv = _LEVEL_AXIS.get(family) or ("intensity" if task in ("tct_pain_stimulation", "tct_empathy_pain_observation") else None)
    if lv:
        named = {a.split(":")[0] for a in atoms}
        atoms = [(f"{lv}:{a.split(':')[1]}" if a.startswith("level:") else a) for a in atoms if not (a.startswith("level:") and lv in named)]
    specific = {a.split(":")[1] for a in atoms if a.split(":")[0] in ("load", "magnitude", "intensity", "arousal", "probability", "word_frequency")}
    atoms = [a for a in atoms if not (a.startswith("level:") and a.split(":")[1] in specific)]
    axes = {a.split(":")[0] for a in atoms}
    for a in _IMPLIED.get(task, []):
        if a.split(":")[0] not in axes:
            atoms.append(a)
    seen, out = set(), []
    for a in atoms:
        ax = a.split(":")[0]
        if ax in _EXCLUSIVE and ax in seen:
            continue
        seen.add(ax); out.append(a)
    if "stimulus:pseudowords" in out and "stimulus:words" in out:
        out.remove("stimulus:words")
    if re.search(r"\b(healthy )?controls?\b", low) and re.search(r"healthy|patients?|group|participants", low):
        c["kind"] = "not_a_condition"   # participant group, not a task condition
    if re.search(r"non[- ]?funny|\bsilence\b", low):
        c["kind"] = "control_state"
    c["atoms"] = out
    return c

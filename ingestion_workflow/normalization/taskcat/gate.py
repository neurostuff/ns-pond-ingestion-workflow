"""Combine assignment routes into one canonical task per mention.

Routes:  A lexical (taskcat.lexicon) | B procedure classifier (masked description embedding) |
         C citation lineage (purity-weighted vote over cited refs) | D author reuse (senior author's prior tasks).
Gate:    entity kind first (not_a_task / assessment / resting), so non-tasks never receive a paradigm.
Policy:  A wins unless A is a generic fallback (tct_*_generic) and B is specific with conf >= 0.7 in the same family;
         else B if conf >= TB; else C if conf >= TC; else D if B's top-3 contains D's label; else residue.
"""
import re

NOT_TASK = re.compile(
    r"\b(diffusion[- ]tensor|dti\b|dwi\b|tractograph|voxel[- ]based|vbm\b|morphometr|cortical thickness|"
    r"(functional |effective |structural )?connectivity( analysis| mapping)?$|connectivity analysis|"
    r"spectroscop|\bmrs\b|perfusion|cerebral blood flow|\bcbf\b|arterial spin|\basl\b|"
    r"\bpet\b(?! task)|fdg|spect\b|structural (mri|imaging|scan)|t1[- ]weighted|anatomical (scan|mri|imaging)|"
    r"(mri|fmri|imaging|image|data) (scan|acquisition|analysis|collection|protocol)s?$|magnetic resonance imaging|"
    r"meta[- ]analys|\bale\b|activation likelihood|404|not found|access denied|"
    r"(lesion|symptom) mapping|graph theor|independent component analysis|\bica\b|seed[- ]based|"
    r"amplitude of low|\balff\b|\breho\b|regional homogeneity|ultrasound|\bct\b scan|"
    r"\banalys[ie]s$|examination$|recordings?$|task[- ]based fmri|\btfmri\b|actigraph|injury model|lesion model|"
    r"homotopic|microstate|circuitry)", re.I)
NEUROPSYCH = re.compile(r"\b(neuropsycholog|battery|batteries|mmse|moca\b|mini[- ]mental|montreal cognitive|wechsler|"
                        r"\bwais\b|\bwisc\b|celf\b|cognitive (assessment|screening|evaluation|tests?)|matrics|cantab|"
                        r"\btest\b(?!.*\btask\b))", re.I)
TASKWORD = re.compile(r"\b(task|paradigm|game|experiment)\b", re.I)
ASSESS = re.compile(
    r"\b(questionnaire|inventory|scale\b|scales\b|interview|survey|self[- ]report|checklist|index\b|"
    r"mmse|moca\b|mini[- ]mental|montreal cognitive|beck|hamilton|panss|y-?bocs|bdi\b|stai\b|cdr\b|"
    r"neuropsycholog\w* (assessment|test|testing|battery|evaluation|examination)|clinical (assessment|evaluation|rating)|"
    r"ecological momentary|disease activity|fugl[- ]meyer|clinical (scale|rating))", re.I)
INTERVENTION = re.compile(r"\b(therapy|treatment|training program|intervention|stimulation therapy|rtms|tdcs|tms\b|"
                          r"drug administration|medication|psychotherapy|cbt\b|injection|endotoxin|gait training|"
                          r"(?<!task )training$)", re.I)
GENERIC = {"tct_working_memory_generic", "tct_inhibition_generic", "tct_interference_generic", "tct_reward_feedback",
           "tct_motor_execution", "hedtsk_face_processing", "tct_episodic_encoding"}


def gate(name, rest_flag=None, lexical=None):
    n = name or ""
    if rest_flag:
        return "resting_state"
    if lexical == "hedtsk_rest":
        return "resting_state"
    if NOT_TASK.search(n) and not lexical:
        return "not_a_task"
    if NEUROPSYCH.search(n) and not TASKWORD.search(n):
        return "neuropsychological_test"
    if lexical:
        return "experimental_task"
    if ASSESS.search(n) and not TASKWORD.search(n):
        return "questionnaire_or_scale"
    if INTERVENTION.search(n) and not TASKWORD.search(n):
        return "intervention_or_training"
    return "experimental_task"


def combine(A, B, C, D, family, TB=0.7, TC=0.7):
    """A: task id or None; B: (id, conf, top3) or None; C: (id, conf) or None; D: id or None.
    Returns (label, route, agreeing_routes)."""
    agree = []
    lab, route = None, None
    if A:
        lab, route = A, "A"
        if A in GENERIC and B and B[1] >= TB and B[0] not in GENERIC and family(B[0]) == family(A):
            lab, route = B[0], "A>B"
    elif B and B[1] >= TB:
        lab, route = B[0], "B"
    elif C and C[1] >= TC and B and C[0] == B[0]:
        lab, route = C[0], "C+B"      # citation route only decides together with B (alone: 25% on gold)
    if lab:
        if A == lab: agree.append("A")
        if B and B[0] == lab: agree.append("B")
        if C and C[0] == lab: agree.append("C")
        if D == lab: agree.append("D")
    return lab, route, agree

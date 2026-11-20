"""Final task label per task: the Astra build's policy (astra_task_labels.py, "cite:a").

    gate      not a task / questionnaire / intervention / neuropsych test -> no label; resting state -> hedtsk_rest
    name      taskcat lexicon on the task name
    citation  the citation route (`citations.py`), conf >= 0.7
    contrast  family from the task's `evidence` (records.evidence: conditions, their descriptions and stimulus
              content, contrast names, the task's text with lexicon names masked, the paper's abbreviations spelled
              out; classifier trained on name-labelled tasks); canonical kept only when taskcat's own label is in
              that family
    taskcat   the pipeline identity (A description / B procedure)

A label of tct_sensory_stimulation from any route becomes the stimulation of the sense the task names (taskcat.lexicon
`split_sensory`): route B and the citation index still know only the task before its split by sense.

The contrast classifier is the one fitted part. The Astra build refit it on every run and never saved it; here
`fit_contrast_classifier` makes it once per fit and `decide` only applies it.
"""
import json

from .records import INVENTORY
from .taskcat.lexicon import split_sensory

GATED = {"not_a_task", "questionnaire_or_scale", "intervention_or_training", "neuropsychological_test"}
CITE_CONF = 0.7
FAM = {t["id"]: t["family"] for t in json.load(open(INVENTORY))["tasks"]}


def fit_contrast_classifier(tasks):
    """(vectorizer, classifier) over the name-labelled tasks' `evidence`: TF-IDF 1-2 grams, min_df 2, balanced
    LR C=4."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression

    X, y = [], []
    for t in tasks:
        if t["route"] == "A:name" and t["family"] not in (None, "pseudo_tasks"):
            d = t["evidence"]
            if len(d.strip()) > 3:
                X.append(d); y.append(t["family"])
    vec = TfidfVectorizer(ngram_range=(1, 2), min_df=2, sublinear_tf=True)
    clf = LogisticRegression(max_iter=3000, C=4.0, class_weight="balanced").fit(vec.fit_transform(X), y)
    return vec, clf


def decide(t, route_c, lexicon, classifier):
    """One pass-one task row -> its final label row. `route_c` the task's citation vote ({} when none)."""
    vec, clf = classifier
    d = t["evidence"]
    # a route that names tct_sensory_stimulation (route B's fixed fit, a citation) gets the sense the task names
    sense = lambda k: split_sensory(k, t["name"], d)
    nm = lexicon.match(t["name"]); nm = sense(nm[0]) if nm else None
    c = route_c or {}
    cl = sense(c.get("label")) if c.get("conf", 0) >= CITE_CONF else None
    cf, cp = (None, 0.0)
    if len(d.strip()) > 3:
        pr = clf.predict_proba(vec.transform([d]))[0]; i = pr.argmax(); cf, cp = clf.classes_[i], float(pr[i])
    tc_lab = None if t["route"] == "gate" else sense(t["canonical"])
    if t["entity_kind"] in GATED:
        can, fam, by = None, None, "gate"
    elif t["entity_kind"] == "resting_state":
        can, fam, by = "hedtsk_rest", FAM["hedtsk_rest"], "rest"
    elif nm:
        can, fam, by = nm, FAM.get(nm), "name"
    elif cl:
        can, fam, by = cl, FAM.get(cl), "citation"
    elif cf:
        # taskcat's canonical when it is in the classifier's family, else the sense the task's name names, when that
        # sense's stimulation is in the family. The name only: evidence names a sense in passing ("odorless control",
        # "hand on the response box") far more often than a name does.
        named = split_sensory("tct_sensory_stimulation", t["name"])
        can = tc_lab if FAM.get(tc_lab) == cf else named if named != "tct_sensory_stimulation" and FAM.get(named) == cf \
            else None
        fam, by = cf, "contrast"
    else:
        can, fam, by = tc_lab, FAM.get(tc_lab), "taskcat" if tc_lab else None
    return {
        "study": t["study"], "task": t["task"], "name": t["name"], "entity_kind": t["entity_kind"],
        "canonical": can, "canonical_name": lexicon.names.get(can) if can else None, "family": fam, "decided_by": by,
        "routes": {"name": nm, "citation": c.get("label"), "citation_conf": c.get("conf"), "contrast": cf,
                   "contrast_p": round(cp, 3), "taskcat": tc_lab, "taskcat_route": t["route"]},
        "variant": t.get("variant") if t.get("canonical") == can else None, "qualifiers": t.get("qualifiers")}

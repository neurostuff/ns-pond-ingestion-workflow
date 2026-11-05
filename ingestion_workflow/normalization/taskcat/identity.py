"""taskcat's task identity (taskcat/cards.py `CardBuilder`, the part astra_normalize.py calls).

Routes:   A lexical on paradigm_term / name / description; B procedure classifier (TF-IDF, CPU) on the mention's
          procedure text with task names masked; B decides only >= TB. Card assembly (facets, citations, authors)
          is not ported: nothing in the normalization reads it.
"""
import json
import re

from .decompose import Decomposer
from .lexicon import Lexicon

NON_TASK_KINDS = {"questionnaire_or_scale", "intervention_or_training"}
KIND_DEFAULT = {
    "naturalistic": lambda t: "tct_story_listening" if re.search(r"stor(y|ies)|narrat|listen|audiobook|podcast", t) else "tct_naturalistic_viewing",
    "localizer": lambda t: ("tct_retinotopy" if re.search(r"retinot|wedge|ring|meridian|eccentric", t) else
                            "tct_motion_perception" if re.search(r"\bmt\b|motion", t) else
                            "tct_sound_categorization" if re.search(r"voice|auditory|sound", t) else
                            "tct_motor_execution" if re.search(r"motor|finger|hand", t) else "tct_visual_localizer"),
}


class CardBuilder:
    def __init__(self, route_b=None, TB=0.6):
        """`route_b`: the (vectorizer, classifier) pair of `routeB_tfidf.pkl`, or None for route A only."""
        self.L = Lexicon(); self.D = Decomposer(self.L); self.TB = TB
        self.B = route_b
        self.memo = None   # a dict: identity() answers a mention it has seen from it (one article's two passes)
        self._mask = re.compile("|".join(sorted({pt.pattern for pt, _, _ in self.L.patterns}, key=len, reverse=True)), re.I)

    def fam(self, k):
        return (self.L.family.get(k) or [None])[0] if k else None

    def procedure_text(self, t):
        parts = [t.get("description") or "", t.get("instructions") or "",
                 "Stimuli: " + "; ".join(s.get("content") or "" for s in t.get("stimuli") or []),
                 "Conditions: " + "; ".join(c.get("name") or "" for c in t.get("conditions") or [])]
        txt = " ".join(parts)
        for v in {t.get("name") or "", t.get("paradigm_term") or ""}:
            if len(v) > 3:
                txt = re.sub(re.escape(v), "[TASK]", txt, flags=re.I)
        return self._mask.sub("[TASK]", txt)

    def route_b(self, t):
        if not self.B:
            return None
        vec, clf = self.B
        pr = clf.predict_proba(vec.transform([self.procedure_text(t)]))[0]
        i = pr.argmax()
        return clf.classes_[i], float(pr[i])

    def identity(self, t):
        if self.memo is None:
            return self._identity(t)
        k = json.dumps(t, sort_keys=True, default=str)
        if k not in self.memo:
            self.memo[k] = self._identity(t)
        return self.memo[k]

    def _identity(self, t):
        kind = t.get("entity_kind")
        A, src = None, None
        for s, txt in (("paradigm_term", t.get("paradigm_term")), ("name", t.get("name"))):
            m = self.L.match(txt)
            if m:
                A, src = m, s; break
        B = self.route_b(t) if kind not in NON_TASK_KINDS else None
        if kind in NON_TASK_KINDS:
            return None, "gate", [], B
        if kind == "resting_state":
            return "hedtsk_rest", "kind", ["kind"], B
        if A:
            lab, route = A[0], f"A:{src}"
        elif B and B[1] >= self.TB:
            lab, route = B[0], "B"
        else:
            # a paradigm word in the description is weak evidence (descriptions cite other paradigms):
            # accept it only when route B is absent, agrees on family, or is unsure itself
            m = self.L.match(t.get("description"))
            if m and (not B or self.fam(B[0]) == self.fam(m[0]) or B[1] < 0.3):
                lab, route = m[0], "A:description"
            elif B and B[1] >= 0.4:
                lab, route = B[0], "B-low"
            else:
                lab, route = None, None
        if not lab and kind in KIND_DEFAULT:       # entity kind implies a generic paradigm
            txt = f"{t.get('name') or ''} {t.get('description') or ''}".lower()
            lab = KIND_DEFAULT[kind](txt); route = "kind" if lab else None
        agree = [r for r, v in (("A", A[0] if A else None), ("B", B[0] if B else None)) if v and v == lab]
        return lab, route, agree, B

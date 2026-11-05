"""`Group.medical_condition` -> MONDO / ONVOC, one record at a time (the Astra build's astra_medical_link.py and
astra_medical.py).

pondie's `medical_condition.normalize` reads a whole corpus, but nothing it decides for one paper depends on
another: the MONDO index is a vocabulary cache, and a paper's abbreviations come from its own text. So this runs
pondie's per-group loop on one record, then the build's two repairs for heads pondie could not place:

1. qualifier strip   a prevalence or history qualifier ("in 8 participants", "(4 patients)", "(69%)", "lifetime",
                     "history of", "comorbid") is removed from the head, which is linked again. The original
                     string keeps it, so the comorbid role still sees it.
2. review contained  a review row is accepted when its top candidate's label, folded, occurs whole-word inside the
                     folded head ("atypical anorexia nervosa" -> anorexia nervosa): the coarser concept the string
                     names, never a sibling.
Both mark `method` ("strip:<method>", "review-contained"). The role of each row:

    none      negation sentinel (healthy, no diagnosis)
    comorbid  the string carries a prevalence or history qualifier
    primary   otherwise
"""
import functools
import re
from dataclasses import asdict, replace

NUM = r"(\d+|one|two|three|four|five|six|seven|eight|nine|ten|a few|few|some|several|a subset|a minority|a small number|most|many)"
STRIP = [
    re.compile(r"\s*\([^)]*(\d|%|patients?|participants?|subjects?|cases?)[^)]*\)", re.I),   # (4 patients), (69%)
    re.compile(rf"[,;]?\s+in\s+{NUM}\b.*$", re.I),                                         # in 8 participants ...
    re.compile(r"[,;]?\s*\d+(\.\d+)?\s*%.*$"),                                              # , 24%
    re.compile(r"^\s*((lifetime|history of|past|prior|previous|comorbid|coincident|concurrent|current)\s+)+", re.I),
]
COMORBID = re.compile(r"\d+(\.\d+)?\s*%|\bin (\d+|one|two|three|a few|some|a subset|a minority)\b|\bsubset\b|"
                      r"\blifetime\b|\bcomorbid|\bhistory of\b|\bpast\b|\bremitted\b|\(\s*n\s*=\s*\d+\s*\)|\(\d+ (patients|participants|subjects)\)", re.I)


def strip(head):
    s = head
    for p in STRIP:
        s = p.sub("", s)
    return re.sub(r"\s+", " ", s).strip(" ,;:-")


def fold(s):
    s = re.sub(r"['’]s\b", "", s.lower())          # "Parkinson's disease" names MONDO's "Parkinson disease"
    return " " + re.sub(r"[^a-z0-9]+", " ", s).strip() + " "


@functools.lru_cache(maxsize=1)
def vocabularies():
    """MONDO, the MONDO->ONVOC crosswalk, ONVOC's disorder branches and the encoded MONDO index, loaded once."""
    from pondie.normalization.medical_condition import Index
    from pondie.vocabularies.folding import singular
    from pondie.vocabularies.mondo import load_mondo, onvoc_crosswalk
    from pondie.vocabularies.onvoc import load_onvoc

    vocab = load_mondo()
    singular("disorders")           # WordNet, loaded here rather than on the first record's first head
    return vocab, onvoc_crosswalk(), load_onvoc().scoped(("disorders",)), Index(vocab).build()


def versions():
    """What the linker ran with: MONDO release and whether pondie's negation parser was there."""
    from pondie.normalization._negation import available
    from pondie.vocabularies import fetch

    return {"mondo": fetch.version("mondo"), "negation_parser": bool(available())}


def abbreviations(text, study):
    """The short forms this paper defines (pondie's `Abbreviations.for_paper`), or None without a text."""
    from pondie.normalization import medical_condition as mc

    return mc._store().for_paper(text, study) if text else None


def links(record, study, text, store=None):
    """pondie Links for every group's `medical_condition` items, after the two repairs. `store` the paper's
    `abbreviations`, when the caller has them already."""
    from pondie.normalization import medical_condition as mc
    from pondie.normalization._records import value_of
    from pondie.vocabularies.phrases import NO_CONDITION, NOT_READ, absent, triage

    vocab, cw, onvoc, index = vocabularies()
    store = store if store is not None else abbreviations(text, study)
    out, staged = [], []
    for group in record.get("groups") or []:
        if not isinstance(group, dict):
            continue
        flag = value_of(group.get("is_healthy"))
        name = str(value_of(group.get("local_id")) or "")
        for item_index, item in mc._items(group):
            triaged = triage(item)
            sentinel, by_flag = triaged.sentinel, False
            if sentinel == NOT_READ and absent(item, flag):
                sentinel, by_flag = NO_CONDITION, True
            if sentinel:
                out.append(mc.Link(study=study, group=name, text=item, item=item_index, sentinel=sentinel,
                                   denied=triaged.denied, scope="flag" if by_flag else triaged.scope))
                continue
            for head in triaged.heads:
                expanded, seen = mc._expanded(head, store, study)
                staged.append((mc.Link(study=study, group=name, text=item, item=item_index, head=head,
                                       denied=triaged.denied, scope=triaged.scope, qualifiers=triaged.qualifiers,
                                       expansions=seen), expanded))

    def placed(row, node, method, score, others):
        oid, olab, via, path = mc.bridge(node, vocab, cw, onvoc)
        return replace(row, mondo=vocab.curie(node), label=vocab.labels[node], umls=vocab.umls.get(node, ""),
                       sctid=vocab.sctid.get(node, ""), method=method, score=round(score, 4), ancestors=path,
                       onvoc=olab, onvoc_id=oid, onvoc_via=via,
                       candidates=tuple(vocab.labels[n] for n in others[:5]))

    if staged:
        found = mc.link(sorted({q for _r, q in staged}), vocab, index)
        for row, query in staged:
            node, method, score, others = found.get(query, (None, "", 0.0, []))
            if node is None:
                out.append(replace(row, method=method, score=round(score, 4),
                                   candidates=tuple(vocab.labels[n] for n in others[:5])))
            else:
                out.append(placed(row, node, method, score, others))
    # the two repairs, on heads pondie left unplaced
    todo = [x for x in out if not x.sentinel and not x.mondo and x.head]
    stripped = {x.head: strip(x.head) for x in todo}
    relinked = mc.link(sorted({s for h, s in stripped.items() if s and s != h}), vocab, index) if todo else {}
    pending = {id(x) for x in todo}
    new = []
    for x in out:
        if id(x) not in pending:
            new.append(x); continue
        s = stripped[x.head]
        if s and s != x.head:
            node, method, score, others = relinked[s]
            if node is not None:
                new.append(placed(x, node, f"strip:{method}", score, others)); continue
            x = replace(x, method=f"strip:{method}", score=round(score, 4),
                        candidates=tuple(vocab.labels[n] for n in others[:5]))
        if x.method.endswith("review") and x.candidates:
            top = x.candidates[0]; node = mc._lexical(top, vocab)
            if node is not None and fold(top) in fold(s or x.head):
                new.append(placed(x, node, "review-contained", x.score, [])); continue
        new.append(x)
    return new


def role(link):
    return "none" if link.sentinel else ("comorbid" if COMORBID.search(link.text) else "primary")


def rows(links_):
    """The build's `medical_condition.jsonl` rows (without the corpus rollup query term)."""
    return [{**asdict(x), "role": role(x)} for x in links_ if x.text]

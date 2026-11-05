"""Fit once over a corpus; assign one article at a time against the saved fit.

A fit directory, named by the content hash of what it holds (`fit-<12 hex>`):

    manifest.json                       fit_id, parameters, model revisions, code and input hashes, pondie commit
    corpus.jsonl                        article -> sha256 of the record the fit read
    fixed/                              routeB_tfidf.pkl, citation_role_lineage_clf.pkl (copied, hash-checked)
    labels/contrast_classifier.pkl      the task-label policy's contrast route (TF-IDF + LR)
    citations/index.json.gz             ref -> {canonical: papers}, ref -> origin tasks, cited corpus papers' labels
    contrast_sets/                      sets.jsonl, members.jsonl, {target,baseline,task}.npy

The task ontology and lexicon are package data (`data/`), hashed into the manifest. Nothing refits implicitly:
`normalize_record` only reads a fit, and `refit` is `fit` over a new corpus, recording the fit it replaces.
"""
import functools
import gzip
import hashlib
import json
import multiprocessing as mp
import os
import pickle
import shutil
import tempfile
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import citations, contrasts, labels, medical, records, sidecars
from .taskcat.identity import CardBuilder

HERE = Path(__file__).resolve().parent

#: The fixed fits and inputs a fit is made from, by path inside the fixed bundle, with their sha256. They are the
#: Hugging Face model repo FIXED_REPO at FIXED_REVISION (private: a quoted-sentence table is in it), fetched by
#: `fixed_from_hub`; a local copy can be passed as `fixed_dir` (or NORMALIZE_FIXED_DIR) instead.
FIXED = {
    "routeB_tfidf.pkl": "bbd8b5709b38e9d36d436fb851238abdfef3adbdc254cd1876ba735768829cdb",
    "citation_role_lineage_clf.pkl": "969b996259dcce59a0206860c67a41b366287e0b59b2314fa302168dfdce07d4",
    "taskextractor/te_normalized.jsonl": "3520088410213b30bfb70967450210f1df215a54c243f0849cc6d3bc25e20bdf",
    "taskextractor/te_citations_ctx.jsonl": "0014f329953abe5236815d668d0eb69027d11e3d74180674b7b0b6cbfa63f89d",
    "taskextractor/discovered_origins.json": "c0ed8de866e70f9b8c27b4fe6c55bff395d4a6cdaf5468dff6351b3b29ca081e",
    "manifest.jsonl": "888ce6fcaa5f07e7b9c4816b08394d579c1fcfa7ae64ce7a2984f4e270d248b5",
}
FIXED_REPO = "neurostuff/astra-normalization-fixed"
#: The repo commit holding FIXED, pinned when it was uploaded. None: not uploaded yet.
FIXED_REVISION = None
#: The fixed files a fit directory carries for assignment; the rest are read only while fitting.
CARRIED = ("routeB_tfidf.pkl", "citation_role_lineage_clf.pkl")


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def code_hashes():
    """sha256 of this package's code and data: the rules a fit's rows were made by."""
    out = {}
    for p in sorted(HERE.rglob("*")):
        if p.is_file() and p.suffix in (".py", ".json", ".tsv") and "__pycache__" not in p.parts:
            out[str(p.relative_to(HERE))] = sha(p)
    return out


def pondie_commit():
    """The pondie commit installed: pip's record of a git install, else the checkout's HEAD."""
    from importlib import metadata

    try:
        url = json.loads(metadata.distribution("pondie").read_text("direct_url.json") or "{}")
        if (url.get("vcs_info") or {}).get("commit_id"):
            return url["vcs_info"]["commit_id"]
    except metadata.PackageNotFoundError:
        pass
    import subprocess

    import pondie

    where = Path(next(iter(pondie.__path__))).parent
    got = subprocess.run(["git", "-C", str(where), "rev-parse", "HEAD"], capture_output=True, text=True)
    return got.stdout.strip() or None


def vocab_versions():
    """The vocabularies the medical linker reads: MONDO's release, pondie's negation parser and abbreviation miner."""
    try:
        from pondie.vocabularies.abbreviations import detector

        miner = "scispacy" if detector() is not None else "builtin"
    except Exception:  # noqa: BLE001
        miner = None
    return {**medical.versions(), "abbreviation_miner": miner}


def verify_fixed(fixed_dir):
    fixed_dir = Path(fixed_dir)
    bad = {k: (sha(fixed_dir / k) if (fixed_dir / k).exists() else None) for k, h in FIXED.items()}
    bad = {k: got for k, got in bad.items() if got != FIXED[k]}
    if bad:
        raise ValueError(f"fixed fits in {fixed_dir} missing or changed: {sorted(bad)}")
    return fixed_dir


def fixed_from_hub(repo=FIXED_REPO, revision=None):
    """The fixed bundle from the Hub at a pinned revision, hash-checked. Reads the stored login or HF_TOKEN."""
    revision = revision or FIXED_REVISION
    if revision is None:
        raise ValueError(f"{repo} has no pinned revision yet: pass fixed_dir, or set NORMALIZE_FIXED_DIR")
    from huggingface_hub import snapshot_download

    return verify_fixed(snapshot_download(repo, revision=revision, allow_patterns=sorted(FIXED)))


def body_of(document):
    """The extraction record inside a document that wraps it as {"study": record}, or the record itself."""
    inner = document.get("study") if isinstance(document, dict) else None
    return inner if isinstance(inner, dict) and "tasks" not in document else document


# ------------------------------------------------------------------ the per-article passes, run in forked workers
_W = {}


def _pass_one(article):
    r = body_of(_W["records"][article])
    rows, _st = records.read(r, article, _W["cb"], _W["sch"])
    bib = citations.bib_from_references(_W["bibliographies"].get(article))
    refs = citations.task_refs(r, article, _W["texts"].get(article), bib)
    names = [(k["study"], k["task"], k["name"]) for k in rows["contrasts"]]
    return rows["tasks"], names, refs


def _pass_two(article):
    r = body_of(_W["records"][article])
    rows, _st = records.read(r, article, _W["cb"], _W["sch"], _W["final"])
    docs = contrasts.documents(r, article, rows["contrasts"], _W["texts"].get(article))
    return [{k: x for k, x in d.items() if k != "excerpt"} for d in contrasts.two_sided(docs)]


def _map(fn, articles, workers):
    if workers <= 1:
        return [fn(a) for a in articles]
    with ProcessPoolExecutor(workers, mp_context=mp.get_context("fork")) as ex:
        return list(ex.map(fn, articles, chunksize=20))


# ------------------------------------------------------------------ fit
def fit(records_, texts, bibliographies, out_dir, *, fixed_dir=None, identifiers=None, workers=1,
        T5=contrasts.T5, TASK_COS=contrasts.TASK_COS, embedding_revision=None, refit_of=None, rows_dir=None,
        log=print):
    """Fit over a corpus and save it. Returns the fit id, the name of the fit's directory under `out_dir`.

    records_, texts, bibliographies: mappings article id -> extraction record / the parse's text / ns-pond's
    `references` artifact payload (None when the article has none). They are read twice, so a lazy mapping
    that loads on access works. fixed_dir: a local copy of the fixed bundle; omitted, NORMALIZE_FIXED_DIR, else
    the Hub's pinned revision. identifiers: article -> {"pmid", "doi"}, so a corpus paper another paper cites
    lends that paper its labels; papers in the fixed manifest are known without it. rows_dir, given, receives
    the corpus's own rows (task_labels.jsonl, contrast_sets.jsonl), which the fit itself does not keep."""
    fixed_dir = fixed_dir or os.environ.get("NORMALIZE_FIXED_DIR")
    fixed_dir = verify_fixed(fixed_dir) if fixed_dir else fixed_from_hub()
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    articles = sorted(records_)
    route_b = pickle.load(open(fixed_dir / "routeB_tfidf.pkl", "rb"))
    lineage = pickle.load(open(fixed_dir / "citation_role_lineage_clf.pkl", "rb"))
    _W.update(records=records_, texts=texts, bibliographies=bibliographies, cb=CardBuilder(route_b),
              sch=records.schemas(), final={})
    log(f"pass one: {len(articles)} articles")
    tasks, names, refs = [], [], []
    for t, n, r in _map(_pass_one, articles, workers):
        tasks += t; names += n; refs += r
    citations.score_lineage(refs, lineage)
    jl = citations.load_jsonl
    paper_ids = [(r["dbid"], r) for r in jl(fixed_dir / "manifest.jsonl")]
    paper_ids += sorted((identifiers or {}).items())
    log("citation index")
    index = citations.build_index(refs, tasks, lineage, jl(fixed_dir / "taskextractor/te_normalized.jsonl"),
                                  jl(fixed_dir / "taskextractor/te_citations_ctx.jsonl"),
                                  json.load(open(fixed_dir / "taskextractor/discovered_origins.json")), paper_ids)
    route_c = {(t["study"], t["task"]): citations.vote(t, index) for t in refs}
    cn = defaultdict(list)
    for s, t, n in names:
        cn[(s, t)].append(n)
    log("contrast classifier")
    classifier = labels.fit_contrast_classifier(tasks, [{"study": s, "task": t, "name": n} for s, t, n in names])
    lexicon = _W["cb"].L
    final = {(t["study"], t["task"]): labels.decide(t, cn[(t["study"], t["task"])], route_c.get((t["study"], t["task"])),
                                                    lexicon, classifier) for t in tasks}
    _W["final"] = final
    log("pass two")
    docs = [d for ds in _map(_pass_two, articles, workers) for d in ds]
    encoder = contrasts.Encoder(embedding_revision)
    log(f"contrast sets: {len(docs)} two-sided contrasts")
    sets, members, arrays = contrasts.fit_sets(docs, encoder, T5, TASK_COS, log=log)

    tmp = Path(tempfile.mkdtemp(prefix=".fit-", dir=out_dir))
    (tmp / "fixed").mkdir()
    for k in CARRIED:
        shutil.copyfile(fixed_dir / k, tmp / "fixed" / k)
    (tmp / "labels").mkdir()
    with open(tmp / "labels/contrast_classifier.pkl", "wb") as f:
        pickle.dump(classifier, f, protocol=5)
    (tmp / "citations").mkdir()
    with open(tmp / "citations/index.json.gz", "wb") as raw, \
            gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:   # no time in the header
        gz.write(json.dumps(index, sort_keys=True).encode())
    contrasts.save_sets(tmp / "contrast_sets", sets, members, arrays)
    with open(tmp / "corpus.jsonl", "w") as f:
        for a in articles:
            f.write(json.dumps({"article": a, "record_sha256": digest(body_of(records_[a]))}) + "\n")
    files = {str(p.relative_to(tmp)): sha(p) for p in sorted(tmp.rglob("*")) if p.is_file()}
    import sklearn

    core = {"parameters": {"T5": T5, "TASK_COS": TASK_COS, "label_policy": "gate -> rest -> name -> citation "
                           f"(conf >= {labels.CITE_CONF}, p_lineage >= {citations.THR}) -> contrast -> taskcat",
                           "citation_origin_weight": citations.ORIGIN_W, "s2_cap": contrasts.S2_CAP},
            "models": {"embedding": {"name": contrasts.EMBEDDING_MODEL, "revision": encoder.revision},
                       "scikit_learn": sklearn.__version__},
            "vocab": vocab_versions(), "fixed_sha256": FIXED, "code_sha256": code_hashes(), "files_sha256": files,
            "pondie_commit": pondie_commit()}
    # the fit's fingerprint: a hash of what it holds and was made with, never of when it ran or what it assigned
    fit_id = "fit-" + digest(core)[:12]
    manifest = {"fit_id": fit_id, "fit_id_covers": sorted(core), **core, "refit_of": refit_of,
                "counts": {"articles": len(articles), "tasks": len(tasks), "two_sided_contrasts": len(docs),
                           "sets": len(sets), "set_members": len(members),
                           "task_labels": dict(Counter(x["decided_by"] or "none" for x in final.values()))}}
    (tmp / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    dest = out_dir / fit_id
    if dest.exists():
        shutil.rmtree(tmp)          # the same content, already saved
    else:
        tmp.rename(dest)
    log(f"saved {dest}")
    if rows_dir:
        rows_dir = Path(rows_dir); rows_dir.mkdir(parents=True, exist_ok=True)
        with open(rows_dir / "task_labels.jsonl", "w") as f:
            for x in final.values():
                f.write(json.dumps(x) + "\n")
        member = {m["id"]: (m["set"], m["orient"]) for m in members}
        with open(rows_dir / "contrast_sets.jsonl", "w") as f:
            for d in docs:
                got = member.get(d["id"])
                f.write(json.dumps({"id": d["id"], "family": d["family"], "set": got[0] if got else None,
                                    "orient": got[1] if got else None}) + "\n")
    _W.clear()
    return fit_id


def refit(records_, texts, bibliographies, out_dir, previous=None, **kw):
    """A new fit over a (grown) corpus. Never run implicitly; `previous` is the fit it replaces, recorded."""
    return fit(records_, texts, bibliographies, out_dir, refit_of=previous, **kw)


# ------------------------------------------------------------------ assign
class Fit:
    """A saved fit, loaded once per process for per-article assignment."""

    def __init__(self, fit_dir):
        d = Path(fit_dir)
        self.dir = d
        self.manifest = json.loads((d / "manifest.json").read_text())
        self.fit_id = self.manifest["fit_id"]
        self.cb = CardBuilder(pickle.load(open(d / "fixed/routeB_tfidf.pkl", "rb")))
        self.sch = records.schemas()
        self.lineage = pickle.load(open(d / "fixed/citation_role_lineage_clf.pkl", "rb"))
        self.classifier = pickle.load(open(d / "labels/contrast_classifier.pkl", "rb"))
        with gzip.open(d / "citations/index.json.gz", "rt") as f:
            self.index = json.load(f)
        p = self.manifest["parameters"]
        self.sets = contrasts.SetFit(d / "contrast_sets", p["T5"], p["TASK_COS"])
        self.revision = self.manifest["models"]["embedding"]["revision"]

    @functools.cached_property
    def encoder(self):
        return contrasts.Encoder(self.revision)


@functools.lru_cache(maxsize=2)
def load(fit_dir):
    return Fit(fit_dir)


def assign(r, article, text, bibliography, f, with_medical=True):
    """One record's derived rows under a fit: medical links, task labels, conditions, contrasts, set assignment."""
    links = medical.links(r, article, text) if with_medical else None
    rows, _ = records.read(r, article, f.cb, f.sch)
    refs = citations.score_lineage(citations.task_refs(r, article, text, citations.bib_from_references(bibliography)),
                                   f.lineage)
    route_c = {(t["study"], t["task"]): citations.vote(t, f.index) for t in refs}
    cn = defaultdict(list)
    for k in rows["contrasts"]:
        cn[(k["study"], k["task"])].append(k["name"])
    final = {(t["study"], t["task"]): labels.decide(t, cn[(t["study"], t["task"])], route_c.get((t["study"], t["task"])),
                                                    f.cb.L, f.classifier) for t in rows["tasks"]}
    rows2, _ = records.read(r, article, f.cb, f.sch, final)
    docs = contrasts.documents(r, article, rows2["contrasts"], text)
    assigned = f.sets.assign(docs, f.encoder)
    return {"medical": links, "labels": final, "conditions": rows2["conditions"], "contrasts": rows2["contrasts"],
            "analyses": rows2["analyses"], "route_c": route_c, "assigned": assigned}


def normalize_record(record, text, bibliography, fit_dir, *, article_id=None, allow_no_parser=False):
    """One article against a saved fit, with no corpus pass. The record's `normalized` sidecars are written in
    place and the record returned; every entry names the fit in its `mapping_tool`.

    text: the parse's text, the one the record's evidence offsets index; bibliography: ns-pond's `references`
    artifact payload or None. article_id names the paper (it is left out of its own citation votes); it defaults
    to the record's `local_id`."""
    _normalize(record, text, bibliography, fit_dir, article_id, allow_no_parser)
    return record


def normalize_records(articles, fit_dir, *, allow_no_parser=False):
    """`normalize_record` over (article_id, record, text, bibliography) items. Returns (records, manifest): the
    manifest names the fit used (`fit_id`, its fingerprint) and the code and vocabularies that assigned."""
    out, tally = [], Counter()
    for article, record, text, bibliography in articles:
        tally.update(_normalize(record, text, bibliography, fit_dir, article, allow_no_parser))
        out.append(record)
    f = load(str(Path(fit_dir).resolve()))
    return out, {"fit_id": f.fit_id, "assign_code_sha256": digest(code_hashes()), "vocab": vocab_versions(),
                 "pondie_commit": pondie_commit(), "articles": len(out), "tallies": dict(sorted(tally.items()))}


def _normalize(record, text, bibliography, fit_dir, article_id, allow_no_parser):
    r = body_of(record)
    local = r.get("local_id")
    article = article_id or str((local.get("value") if isinstance(local, dict) else local) or "")
    if not allow_no_parser and not medical.versions()["negation_parser"]:
        # without spaCy's en_core_web_sm pondie cannot read negation scope: "Dementia free" links to dementia
        raise RuntimeError("pondie's negation parser is missing: python -m spacy download en_core_web_sm")
    f = load(str(Path(fit_dir).resolve()))
    got = assign(r, article, text or "", bibliography, f)
    tally = Counter()
    for name, t in (("medical", sidecars.write_medical(r, got["medical"], f.fit_id)),
                    ("tasks", sidecars.write_tasks(r, {t: x for (_s, t), x in got["labels"].items()}, f.fit_id)),
                    ("conditions", sidecars.write_conditions(r, got["conditions"], f.fit_id)),
                    ("contrast_sets", sidecars.write_contrasts(r, got["contrasts"], got["assigned"], f.fit_id,
                                                               f.sets.T))):
        tally.update({f"{name}:{k}": v for k, v in t.items()})
    return tally

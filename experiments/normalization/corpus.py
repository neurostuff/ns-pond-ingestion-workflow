"""The Astra build's corpus as the mappings `ingestion_workflow.normalization.fit` reads.

records_all2/<id>.extraction.json, corpus_all2/<id>/processed/pubget/text.txt and the taskcat bibliographies
(<bib>/<id>.json), each loaded on access. A taskcat bibliography is turned into the shape of ns-pond's
`references` artifact (entries + in-text citations by marker text), the shape the library reads; the library
turns it back, so the citation route sees what the build saw.
"""
import json
from collections.abc import Mapping
from pathlib import Path


class Lazy(Mapping):
    def __init__(self, keys, load):
        self._keys = sorted(keys)
        self._set = set(self._keys)
        self._load = load

    def __getitem__(self, k):
        if k not in self._set:
            raise KeyError(k)
        return self._load(k)

    def get(self, k, default=None):
        return self._load(k) if k in self._set else default

    def __iter__(self):
        return iter(self._keys)

    def __len__(self):
        return len(self._keys)


def references_payload(bib):
    """A taskcat bibliography as an ns-pond `references` payload (no offsets: the build's had none)."""
    refs = [{"id": r["rid"], "position": r.get("order"), "provider": "source_xml", "label": None,
             "text": r.get("raw") or "", "doi": r.get("doi"), "pmid": r.get("pmid"), "pmcid": r.get("pmcid"),
             "title": r.get("title"), "year": int(r["year"]) if str(r.get("year") or "").isdigit() else None,
             # the library reads the first listed surname as the first author: keep the build's first_author first
             "authors": [r["first_author"]] + [a for a in (r.get("authors") or [])[1:]] if r.get("first_author") else []}
            for r in bib.get("refs") or []]
    cits = [{"text_span": {"text": x["text"]}, "sentence": None, "references": x["rids"], "method": "xref",
             "confidence": None, "marker_in_text": True} for x in bib.get("xrefs") or []]
    return {"source": bib.get("source"), "list_provider": "source", "references": refs, "citations": cits}


def astra_corpus(records_dir, corpus_dir, bib_dir, only=None):
    records_dir, corpus_dir, bib_dir = Path(records_dir), Path(corpus_dir), Path(bib_dir)
    ids = [p.name.split(".")[0] for p in records_dir.glob("*.extraction.json")]
    if only is not None:
        ids = [i for i in ids if i in only]

    def record(i):
        return json.loads((records_dir / f"{i}.extraction.json").read_text())

    def text(i):
        p = corpus_dir / i / "processed/pubget/text.txt"
        return p.read_text(errors="replace") if p.exists() else ""

    def bib(i):
        p = bib_dir / f"{i}.json"
        return references_payload(json.loads(p.read_text())) if p.exists() else None

    return Lazy(ids, record), Lazy(ids, text), Lazy(ids, bib)

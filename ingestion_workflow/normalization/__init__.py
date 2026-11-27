"""Task, condition, medical-condition and contrast-set normalization of extraction records.

The Astra build's normalization (taskcat + the astra_* scripts), split so that what needs a corpus is fitted once
and saved, and everything else runs one article at a time against the saved fit:

    fit(records, texts, bibliographies, out_dir, fixed_dir=...) -> fit_id
    normalize_record(record, text, bibliography, fit_dir) -> record, its `normalized` sidecars written
    normalize_records([(article_id, record, text, bibliography)], fit_dir) -> (records, manifest)
    refit(records, texts, bibliographies, out_dir, previous=fit_id) -> fit_id

Needs the `normalize` extra (pondie, scikit-learn, sentence-transformers).
"""
__all__ = ["FIXED", "Fit", "fit", "load", "normalize_record", "normalize_records", "refit"]


def __getattr__(name):
    # `api` imports pondie; the rule modules (records, labels, citations, contrasts) do not need it
    if name in __all__:
        from . import api

        return getattr(api, name)
    raise AttributeError(name)

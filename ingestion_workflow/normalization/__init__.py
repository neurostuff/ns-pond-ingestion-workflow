"""Task, condition, contrast, medical-condition and acquisition-modality normalization of extraction records,
one article at a time.

    prompts(record, text, model) -> {task id: prompt}       the task-design model's requests
    normalize_record(record, text, answers, model=..., version=..., article_id=...) -> record, sidecars written

`design` builds the prompt and reads the answer, `structure` reads each analysis's structure off the record,
`identity` assembles each contrast's identity, and `sidecars` writes them. Needs the `normalize` extra (pondie).
"""
__all__ = ["NoAnswer", "normalize_record", "prompts"]


def __getattr__(name):
    # `api` is the package's entry; pondie is imported only where sidecars are written
    if name in __all__:
        from . import api

        return getattr(api, name)
    raise AttributeError(name)

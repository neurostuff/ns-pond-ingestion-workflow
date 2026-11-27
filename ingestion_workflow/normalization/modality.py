"""A paper's acquisition modalities, as pondie's `Modality` normalizer reads its record's acquisitions.

pondie (`pondie.normalization.modality`, neurostuff/pondie#35) is the one writer of the `Modality` entry on
`acquisitions[].modality`, and normalize gives every record that entry through `apply_normalized`. Values are
study_schema's `Modality` spelling (`fMRI`, `iEEG`, `physiological`, ...), with a bare `MRI` refined from the
measures of the analyses that use it.
"""
from typing import List


def paper_modalities(document: dict) -> List[str]:
    """The mapped values pondie reads off a record's acquisitions, sorted: the paper's modalities.
    `document` is the record, or a document wrapping it as {"study": record}."""
    from pondie.normalization import modality

    inner = document.get("study") if isinstance(document, dict) else None
    record = inner if isinstance(inner, dict) and "tasks" not in document else document
    if not isinstance(record, dict):
        return []
    return sorted({d.value for d, _also in modality.read(record).values() if d})

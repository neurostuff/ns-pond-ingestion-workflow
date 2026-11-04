"""What the curation scripts share: the curated TSV's rows, and the package's matcher over them."""

import csv
import json
import os

from ingestion_workflow.data_sources.matching import (  # noqa: F401
    Dictionary,
    alias_pattern,
    key,
    section_at,
    sections,
    snippet,
)
from ingestion_workflow.data_sources.merge import CURATED, TIER_COLUMNS, split  # noqa: F401


def read_tsv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def read_lines(path):
    with open(path, encoding="utf-8") as f:
        return f.read().splitlines()


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def pick_text(paper_dir):
    """(size, source, path) of the paper's longest processed/<source>/text.txt, or None."""
    best = None
    processed = os.path.join(paper_dir, "processed")
    for source in os.listdir(processed) if os.path.isdir(processed) else []:
        path = os.path.join(processed, source, "text.txt")
        if os.path.isfile(path):
            size = os.path.getsize(path)
            if best is None or size > best[0]:
                best = (size, source, path)
    return best


def read_dictionary_rows(path=CURATED):
    return read_tsv(path)


def build_dictionary(rows):
    """(entries by id, full-name keys, acronyms, name regex, acronym regex) of curated rows."""
    found = Dictionary([(r["id"], alias, tier) for r in rows
                        for tier, column in TIER_COLUMNS.items() for alias in split(r[column])])
    return {r["id"]: r for r in rows}, found.names, found.acronyms, found.name_re, found.acr_re


def load_dictionary(path=CURATED):
    return build_dictionary(read_dictionary_rows(path))

"""ACE returns only the tables its parser called activation tables, and the
HTML scan adds the rest. The two must not overlap."""

from __future__ import annotations

from ingestion_workflow.extractors.ace_extractor import _table_fingerprint

# The same table as ACE rewrote it, and as it appears in the document.
ACE_REWRITE = (
    '<table><thead><tr><th class="colsep-0" scope="col">Region</th>'
    '<th class="colsep-0" scope="col">x</th></tr></thead>'
    '<tbody><tr><td>Middle Frontal</td><td>28</td></tr>'
    '<tr><td>Superior Frontal</td><td>-2</td></tr>'
    '<tr><td>Insula</td><td>33</td></tr></tbody></table>'
)
DOCUMENT = (
    '<table>\n  <thead><tr><th scope="col" class="colsep-0">Region</th>\n'
    '  <th scope="col" class="colsep-0">x</th></tr></thead>\n'
    '  <tbody><tr><td>Middle Frontal</td><td>28</td></tr>\n'
    '  <tr><td>Superior&nbsp;Frontal</td><td>-2</td></tr>\n'
    '  <tr><td>Insula</td><td>Empty Cell</td><td>33</td></tr></tbody></table>'
)


def test_the_same_table_matches_across_aces_rewrite():
    """The rewrite changes entities, whitespace and cell placeholders, so the
    tag-free text differs -- 497 against 557 characters on a real pair. The
    numbers do not, and that is what put the same table in the corpus twice."""
    assert _table_fingerprint(ACE_REWRITE) == _table_fingerprint(DOCUMENT)


def test_different_tables_do_not_match():
    other = ACE_REWRITE.replace("28", "44").replace("-2", "-9")
    assert _table_fingerprint(ACE_REWRITE) != _table_fingerprint(other)


def test_thousands_separators_do_not_split_a_number():
    """`5,658` and `5658` are one cluster size, not a 5 and a 658."""
    a = "<table><tr><td>1</td><td>2</td><td>5,658</td></tr></table>"
    b = "<table><tr><td>1</td><td>2</td><td>5658</td></tr></table>"
    assert _table_fingerprint(a) == _table_fingerprint(b)


def test_a_table_with_too_few_numbers_is_not_matched_by_number():
    """Otherwise every caption-only or layout table collapses into one."""
    a = "<table><tr><td>Results</td></tr></table>"
    b = "<table><tr><td>Discussion</td></tr></table>"
    assert not _table_fingerprint(a).startswith("n:")
    assert _table_fingerprint(a) != _table_fingerprint(b)

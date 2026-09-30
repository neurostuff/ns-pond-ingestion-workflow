"""ACE returns the tables its parser called activation tables, and the HTML
scan adds the rest. The two must not overlap, and neither may lose a sign."""

from __future__ import annotations

from ingestion_workflow.extractors.ace_extractor import _table_fingerprint
from ingestion_workflow.extractors.utils import _MINUS_CHARS, normalize_minus

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


# -- one table, two renderings -------------------------------------------

def test_the_same_table_matches_across_aces_rewrite():
    """The rewrite changes entities, whitespace and cell placeholders, so the
    tag-free text differs -- 497 against 557 characters on a real pair. The
    numbers do not, and matching on the text put the same table in the corpus
    twice: 72.6% of articles with more than one passing table carried the same
    numbers under two ids."""
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


# -- the minus sign -------------------------------------------------------

_ENTITY = ("<table><tr><td>A</td><td>&#x02212;45.2</td>"
           "<td>&#x02212;57.1</td><td>14.7</td></tr></table>")
_HYPHEN = "<table><tr><td>A</td><td>-45.2</td><td>-57.1</td><td>14.7</td></tr></table>"
_LITERAL = ("<table><tr><td>A</td><td>−45.2</td>"
            "<td>−57.1</td><td>14.7</td></tr></table>")


def test_one_table_matches_however_its_minus_is_written():
    """Elsevier writes 2,000 minus signs as U+2212 against 30 ASCII hyphens,
    and ACE's document HTML writes the entity `&#x02212;`. All three are the
    same table; disagreeing about that is what let a duplicate through."""
    assert _table_fingerprint(_ENTITY) == _table_fingerprint(_HYPHEN)
    assert _table_fingerprint(_LITERAL) == _table_fingerprint(_HYPHEN)


def test_an_entity_minus_is_not_dropped():
    """It survived tag-stripping as the literal text `&#x02212;`, so the sign
    vanished and a left-hemisphere focus was stored on the right. 13.3% of
    passed tables gained negative numbers once this was fixed."""
    assert "-45.2" in normalize_minus(_ENTITY)


def test_every_minus_like_character_normalises():
    """The list is the one the PDF path already used for Docling's glyphs."""
    for ch in _MINUS_CHARS:
        assert normalize_minus("<td>%s45</td>" % ch) == "<td>-45</td>"


def test_comparison_entities_are_left_alone():
    """A blanket unescape would turn `&lt;0.05&gt;` into `<0.05>`, which the
    serialiser then strips as a tag. `&lt;` appears 1,357 times in a 1,200
    table sample and `&#x0003c;` 739, so this is not a corner case."""
    assert normalize_minus("p &lt; 0.05 &gt; x") == "p &lt; 0.05 &gt; x"
    assert normalize_minus("&#x0003c;0.05&#x0003e;") == "&#x0003c;0.05&#x0003e;"


def test_the_stored_block_is_normalised():
    """Everything downstream reads the file, so a minus left as an entity
    there is a coordinate in the wrong hemisphere."""
    import inspect

    from ingestion_workflow.extractors import ace_extractor

    src = inspect.getsource(ace_extractor._unparsed_html_tables)
    assert "normalize_minus(block)" in src


def test_the_serialised_table_keeps_the_sign():
    """v19's training data holds no U+2212 at all -- 91.6% of rows use an
    ASCII hyphen -- so this puts production back on the model's distribution
    rather than moving it off."""
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    for markup in (_ENTITY, _LITERAL):
        out = CreateAnalysesService._serialise(markup, "t1")
        assert "-45.2" in out and "-57.1" in out

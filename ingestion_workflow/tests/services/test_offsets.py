"""Offset maps: a remapped span selects the same characters after the edit."""

import random
import re

import pytest
from ingestion_workflow.services import offsets
from ingestion_workflow.services.offsets import Chain, OffsetMap

ALPHABET = "ab \n-1\t"


def _random_edit(rng, text):
    """`text` with a few random replacements, insertions and deletions."""
    pieces, at = [], 0
    cuts = sorted(rng.sample(range(len(text) + 1), min(len(text) + 1, rng.randint(1, 6))))
    for cut in cuts:
        if cut < at:
            continue
        pieces.append(text[at:cut])
        drop = rng.randint(0, 3)
        pieces.append("".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 3))))
        at = min(len(text), cut + drop)
    pieces.append(text[at:])
    return "".join(pieces)


def _texts(seed):
    rng = random.Random(seed)
    old = "".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 60)))
    return rng, old, _random_edit(rng, old)


def _check(old, new, m, rng):
    for _ in range(40):
        start = rng.randint(0, len(old))
        end = rng.randint(start, len(old))
        got = m.span(start, end)
        if not m.touches(start, end):
            assert got is not None
            assert new[got[0]:got[1]] == old[start:end]
        elif got is not None:
            assert 0 <= got[0] <= got[1] <= len(new)


@pytest.mark.parametrize("seed", range(300))
def test_a_diffed_span_selects_the_same_characters(seed):
    rng, old, new = _texts(seed)
    m = offsets.diff(old, new)
    _check(old, new, m, rng)
    # and back: the inverse maps the new text's untouched spans home
    _check(new, old, m.inverse(), rng)


@pytest.mark.parametrize("seed", range(200))
def test_a_substituted_span_selects_the_same_characters(seed):
    rng, old, _ = _texts(seed)
    def repl(g):
        return " " if g.group().isspace() else "−"

    new, m = offsets.sub(r"\s{2,}|-(?=1)|\t", repl, old)
    assert new == re.sub(r"\s{2,}|-(?=1)|\t", repl, old)
    _check(old, new, m, rng)


@pytest.mark.parametrize("seed", range(100))
def test_a_chain_of_edits_maps_as_its_steps_do(seed):
    rng, old, mid = _texts(seed)
    new = _random_edit(rng, mid)
    first, second = offsets.diff(old, mid), offsets.diff(mid, new)
    chain = Chain([first, second])
    for _ in range(30):
        start = rng.randint(0, len(old))
        end = rng.randint(start, len(old))
        step = first.span(start, end)
        step = second.span(*step) if step is not None else None
        assert chain.span(start, end) == step


def test_a_span_inside_deleted_text_is_lost_not_shifted():
    old = "keep THIS out keep"
    new = "keep  keep"
    m = offsets.diff(old, new)
    assert m.span(old.index("THIS"), old.index("THIS") + 4) is None
    assert m.span(old.index("THIS") + 1, old.index("THIS") + 3) is None
    assert new[slice(*m.span(0, 4))] == "keep"
    tail = len(old) - 4
    assert new[slice(*m.span(tail, len(old)))] == "keep"


def test_insertions_at_a_span_end_stay_outside_it():
    m = OffsetMap([(3, 3, 3, 5)])  # "abcdef" -> "abcXYdef"
    new = "abcXYdef"
    assert new[slice(*m.span(0, 3))] == "abc"
    assert new[slice(*m.span(3, 6))] == "def"
    assert m.span(2, 4) == (2, 6) and m.touches(2, 4)
    assert not m.touches(0, 3) and not m.touches(3, 6)


def test_a_span_holding_a_whole_edit_covers_its_replacement():
    old, new = "x = 10-3 y", "x = 10−3 y"
    m = offsets.diff(old, new)
    assert m.touches(0, len(old))
    assert m.span(0, len(old)) == (0, len(new))


def test_touching_edits_merge():
    assert OffsetMap([(0, 2, 0, 1), (2, 4, 1, 1)]).edits == ((0, 4, 0, 1),)
    assert not OffsetMap()

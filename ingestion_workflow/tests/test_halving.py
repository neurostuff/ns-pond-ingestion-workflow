"""A refused batch is halved down to the items the provider refuses alone."""

import pytest
import requests

from ingestion_workflow.utils.http import refused
from ingestion_workflow.utils.isolation import halving


def _status(code):
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError(f"{code} for url", response=response)


def _provider(bad, code=400):
    """Answers each batch with its items, refusing any batch that holds a bad one."""
    asked = []

    def ask(batch):
        asked.append(list(batch))
        if any(item in bad for item in batch):
            raise _status(code)
        return list(batch)

    return ask, asked


def test_a_batch_the_provider_takes_is_asked_once():
    ask, asked = _provider(bad=set())
    answered, failed = halving(list(range(100)), ask, refused)

    assert asked == [list(range(100))] and failed == {}
    assert answered == [(list(range(100)), list(range(100)))]


@pytest.mark.parametrize("code", [400, 413, 414, 422])
def test_only_the_items_refused_alone_fail(code):
    ask, asked = _provider(bad={7, 61}, code=code)
    answered, failed = halving(list(range(100)), ask, refused)

    assert set(failed) == {7, 61} and str(code) in str(failed[7])
    assert sorted(i for batch, _ in answered for i in batch) == [i for i in range(100) if i not in (7, 61)]
    # halving, not one request per item: about two per level for each bad item
    assert len(asked) < 2 * 2 * 7 + 1


@pytest.mark.parametrize("code", [429, 500, 503])
def test_a_busy_provider_is_not_halved_but_raised(code):
    ask, asked = _provider(bad={3}, code=code)
    with pytest.raises(requests.HTTPError):
        halving(list(range(10)), ask, refused)
    assert asked == [list(range(10))]


def test_no_items_ask_nothing():
    ask, asked = _provider(bad=set())
    assert halving([], ask, refused) == ([], {})
    assert asked == []

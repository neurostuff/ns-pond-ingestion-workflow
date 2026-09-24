"""A host that is gone is paid for once, not once per table."""

from __future__ import annotations

import sys
import types

import pytest
from ingestion_workflow.extractors import ace_urls


@pytest.fixture
def fake_scrape(monkeypatch):
    """Stand in for `ace.scrape`, which the guard patches by attribute."""
    module = types.ModuleType("ace.scrape")
    module.calls = []

    def get_url(url, n_retries=5, timeout=10.0, verbose=False):
        module.calls.append(url)
        return None if "dead.example" in url else "<html/>"

    module.get_url = get_url
    ace_pkg = types.ModuleType("ace")
    ace_pkg.scrape = module
    monkeypatch.setitem(sys.modules, "ace", ace_pkg)
    monkeypatch.setitem(sys.modules, "ace.scrape", module)
    return module


def test_dead_host_is_attempted_only_until_it_is_presumed_gone(fake_scrape):
    tracker = ace_urls.install(ace_urls.DeadHosts(dead_after=3))

    for i in range(10):
        fake_scrape.get_url(f"https://dead.example/t{i}.html")

    assert len(fake_scrape.calls) == 3, "kept calling a host that never answers"
    assert "dead.example" in tracker.dead


def test_a_live_host_is_never_skipped(fake_scrape):
    ace_urls.install(ace_urls.DeadHosts(dead_after=3))

    results = [fake_scrape.get_url(f"https://live.example/t{i}.html") for i in range(10)]

    assert results == ["<html/>"] * 10
    assert len(fake_scrape.calls) == 10


def test_one_success_clears_the_count(fake_scrape):
    tracker = ace_urls.install(ace_urls.DeadHosts(dead_after=3))

    fake_scrape.get_url("https://flaky.example/dead.example")  # counts as failure
    fake_scrape.get_url("https://flaky.example/ok.html")  # succeeds, resets

    assert "flaky.example" not in tracker.dead


def test_retries_are_capped_below_aces_default(fake_scrape):
    seen = {}

    def get_url(url, n_retries=5, **kwargs):
        seen["n_retries"] = n_retries
        return "<html/>"

    fake_scrape.get_url = get_url
    ace_urls.install(ace_urls.DeadHosts(), n_retries=2)

    fake_scrape.get_url("https://live.example/t.html")

    assert seen["n_retries"] == 2


def test_moved_hosts_are_rewritten(monkeypatch):
    monkeypatch.setitem(ace_urls.HOST_MOVES, "old.example", "new.example")

    assert ace_urls.rewrite("https://old.example/a/b?c=1") == "https://new.example/a/b?c=1"
    assert ace_urls.rewrite("https://other.example/a") == "https://other.example/a"


def test_installing_twice_does_not_stack_wrappers(fake_scrape):
    ace_urls.install(ace_urls.DeadHosts(dead_after=3))
    ace_urls.install(ace_urls.DeadHosts(dead_after=3))

    for i in range(10):
        fake_scrape.get_url(f"https://dead.example/t{i}.html")

    assert len(fake_scrape.calls) == 3

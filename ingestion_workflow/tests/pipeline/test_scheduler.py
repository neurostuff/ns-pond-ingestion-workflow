"""End-to-end scheduler behaviour, with a stage we can make misbehave."""

from __future__ import annotations

from typing import Iterator, List, Sequence

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, fingerprint
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context, run_stages
from ingestion_workflow.pipeline.plan import StagePlan, Work


class CountingStage:
    """A stage that records every article it was actually asked to compute."""

    requires = None

    def __init__(self, name="download", version=1, fail: Sequence[str] = ()):
        self.name = name
        self.version = version
        self.fail = set(fail)
        self.executed: List[str] = []

    def fingerprint_for(self) -> str:
        return fingerprint(self.name, self.version)

    def plan(self, ctx, refs, artifacts, upstream) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = ctx.catalog.attempt_counts([r.id for r in refs], self.name, "test")
        for ref in refs:
            existing = artifacts.get(ref.id, {}).get("test")
            if ctx.is_fresh(existing, self.fingerprint_for()):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="test", fingerprint=self.fingerprint_for()))
        return plan

    def execute(self, ctx, works: List[Work]) -> Iterator[Outcome]:
        for work in works:
            self.executed.append(work.article_id)
            pmid = work.ref.identifier.pmid
            if pmid in self.fail:
                yield Outcome.failure(work.article_id, self.name, "test", "boom")
            else:
                yield Outcome(
                    article_id=work.article_id,
                    stage=self.name,
                    source="test",
                    fingerprint=work.fingerprint,
                    payload={"pmid": pmid},
                    summary={"pmid": pmid},
                )


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        data_root=tmp_path / "data", cache_root=tmp_path / "c", catalog_root=tmp_path / "cat"
    )
    with Catalog.open(settings.catalog_root) as catalog:
        refs = catalog.register_many([Identifier(pmid=str(n)) for n in range(10)])
        yield settings, catalog, refs


def test_a_second_run_does_no_work(env):
    settings, catalog, refs = env
    stage = CountingStage()
    ctx = Context(settings, catalog)

    first = run_stages(ctx, [stage], refs)
    assert first.stages["download"].ok == 10
    assert len(stage.executed) == 10

    stage.executed.clear()
    second = run_stages(ctx, [stage], refs)
    assert stage.executed == []
    assert second.stages["download"].fresh == 10


def test_a_version_bump_makes_everything_stale(env):
    settings, catalog, refs = env
    ctx = Context(settings, catalog)
    run_stages(ctx, [CountingStage(version=1)], refs)

    bumped = CountingStage(version=2)
    report = run_stages(ctx, [bumped], refs)
    assert len(bumped.executed) == 10
    assert report.stages["download"].fresh == 0


def test_only_failures_are_retried(env):
    settings, catalog, refs = env
    ctx = Context(settings, catalog, retry_after=__import__("datetime").timedelta(0))
    first = CountingStage(fail={"3", "7"})
    run_stages(ctx, [first], refs)

    retry = CountingStage(fail=set())
    report = run_stages(ctx, [retry], refs)
    assert sorted(catalog.identifier(a).pmid for a in retry.executed) == ["3", "7"]
    assert report.stages["download"].fresh == 8
    assert report.stages["download"].ok == 2


def test_attempts_stop_at_the_cap(env):
    settings, catalog, refs = env
    zero = __import__("datetime").timedelta(0)
    ctx = Context(settings, catalog, max_attempts=2, retry_after=zero)
    for _ in range(4):
        run_stages(ctx, [CountingStage(fail={str(n) for n in range(10)})], refs)

    counts = catalog.attempt_counts([r.id for r in refs], "download", "test")
    assert all(count <= 2 for count, _ in counts.values())


def test_dry_run_touches_nothing(env):
    settings, catalog, refs = env
    stage = CountingStage()
    report = run_stages(Context(settings, catalog), [stage], refs, dry_run=True)
    assert stage.executed == []
    assert report.stages["download"].planned == 10
    assert catalog.status_counts() == {}


def test_memory_does_not_scale_with_the_corpus(env):
    """The scheduler batches; a stage never sees the whole selection at once."""
    settings, catalog, _ = env
    refs = catalog.register_many([Identifier(pmid=f"b{n}") for n in range(1200)])

    sizes: List[int] = []

    class Watcher(CountingStage):
        def execute(self, ctx, works):
            sizes.append(len(works))
            return super().execute(ctx, works)

    run_stages(Context(settings, catalog), [Watcher()], refs)
    assert max(sizes) <= 500
    assert sum(sizes) == 1200


def test_each_stage_shows_progress_when_asked(env, monkeypatch):
    """These runs are unattended and hours long; silence reads as a hang."""
    made = []

    class FakeBar:
        def __init__(self, total, desc):
            self.total, self.desc, self.seen = total, desc, 0
            made.append(self)

        def update(self, n):
            self.seen += n

        def set_postfix_str(self, *a, **k):
            pass

        def close(self):
            self.closed = True

    import ingestion_workflow.pipeline.scheduler as sched

    monkeypatch.setattr(
        sched, "progress_bar", lambda settings, total, desc, **kw: FakeBar(total, desc)
    )

    settings, catalog, refs = env
    run_stages(Context(settings, catalog), [CountingStage()], refs)

    assert [b.desc for b in made] == ["download"]
    assert made[0].total == len(refs)
    assert made[0].seen == len(refs)
    assert made[0].closed


def test_a_dry_run_shows_no_progress_bar(env, monkeypatch):
    import ingestion_workflow.pipeline.scheduler as sched

    made = []
    monkeypatch.setattr(
        sched, "progress_bar", lambda *a, **k: made.append(1) or None
    )
    settings, catalog, refs = env
    run_stages(Context(settings, catalog), [CountingStage()], refs, dry_run=True)
    assert made == []

"""Papers uploaded `neurostore_upload_workers` at once: neurostore ingests one per API worker
(production runs 8), and one at a time left all but one of them idle."""

import threading
import time

from ingestion_workflow.catalog import ArticleRef, Artifact, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages.upload import UploadStage
from ingestion_workflow.services.neurostore_ingest import NeurostoreUnavailable


class _Ingester:
    """Answers each paper after a pause, counting how many it holds at once."""

    def __init__(self, fail_on=None):
        self.lock = threading.Lock()
        self.now = self.most = 0
        self.sent = []
        self.fail_on = fail_on

    def upload(self, path):
        with self.lock:
            self.now += 1
            self.most = max(self.most, self.now)
            self.sent.append(path.name)
        try:
            time.sleep(0.05)
            if path.name == self.fail_on:
                raise NeurostoreUnavailable("neurostore did not answer")
            return {"status": "uploaded", "summary": {"base_study_id": f"bs-{path.name}"}}
        finally:
            with self.lock:
                self.now -= 1

    def close(self):
        pass


def _works(n):
    return [(Work(ref=ArticleRef(id=f"a{i}", identifier=Identifier(pmid=str(1000 + i))),
                  source="", fingerprint="fp",
                  upstream=Artifact(article_id=f"a{i}", stage="sync",
                                    summary={"path": f"/pond/s{i}", "analyses": 1})),
             {"analyses": 1}) for i in range(n)]


def _stage(tmp_path, workers, ingester):
    stage = UploadStage(Settings(data_root=tmp_path, neurostore_upload_workers=workers))
    stage._ingester = ingester
    return stage


def test_papers_are_sent_as_many_at_once_as_the_setting_allows(tmp_path):
    ingester = _Ingester()
    learned = []
    outcomes = list(_stage(tmp_path, 4, ingester)._send_all(_works(12), learned))

    assert ingester.most == 4
    assert sorted(o.article_id for o in outcomes) == sorted(f"a{i}" for i in range(12))
    assert {o.status for o in outcomes} == {Status.OK}
    assert sorted(learned) == sorted((f"a{i}", "neurostore", f"bs-s{i}") for i in range(12))


def test_one_worker_sends_one_paper_at_a_time(tmp_path):
    ingester = _Ingester()
    list(_stage(tmp_path, 1, ingester)._send_all(_works(5), []))

    assert ingester.most == 1
    assert ingester.sent == [f"s{i}" for i in range(5)]


def test_a_stop_starts_no_other_paper_and_records_those_under_way(tmp_path):
    """Neurostore down on the third paper: the papers already sent are recorded, the third
    and every one not yet started stay pending, and nothing records a failure."""
    ingester = _Ingester(fail_on="s2")
    stage = _stage(tmp_path, 3, ingester)
    outcomes = list(stage._send_all(_works(10), []))

    assert stage._stopped and "did not answer" in stage._stopped
    assert {o.status for o in outcomes} == {Status.OK}
    assert "a2" not in {o.article_id for o in outcomes}
    assert len(ingester.sent) <= 3 + 2  # the three in flight when it stopped, and no more after
    assert len(outcomes) == len(ingester.sent) - 1


def test_uploads_under_way_together_ask_auth0_for_one_token(monkeypatch):
    """Two papers sent at once that find the token expired ask for one new token, not two;
    and a 401 on a token another upload has already replaced asks for none."""
    from ingestion_workflow.services import neurostore_ingest as ni

    ingester = object.__new__(ni.NeurostoreIngester)
    ingester._token_lock = threading.Lock()
    ingester._token, ingester._expires = "old", 0.0
    ingester._clock = lambda: 1000.0
    asked = []

    def ask():
        asked.append(1)
        time.sleep(0.05)
        ingester._token, ingester._expires = f"new-{len(asked)}", 1000.0 + 3600

    monkeypatch.setattr(ingester, "_ask_token", ask)
    threads = [threading.Thread(target=ingester._current_token) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert asked == [1]
    assert ingester._current_token(refused="old") == "new-1" and asked == [1]
    assert ingester._current_token(refused="new-1") == "new-2" and len(asked) == 2

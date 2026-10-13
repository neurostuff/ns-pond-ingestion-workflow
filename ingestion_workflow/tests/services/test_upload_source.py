"""Which extractor produced a study, and what that key decides.

`source` is not a label. neurostore holds one study version per source, so it
decides whether an upload updates a row or adds one, and whether `discover`
thinks a base study is still work.
"""

from __future__ import annotations

import inspect

from ingestion_workflow.config import Settings
from ingestion_workflow.services.discover import LLM_SOURCE, unprocessed_base_studies


def test_there_is_no_default_source():
    """It used to default to 'llm'. Under the default 'update' behaviour that
    made a forgotten setting destructive: the study lookup resolved to another
    extractor's version and the reconciliation deleted its analyses to put
    these in their place. There is no source that is safe to guess, so silence
    is refused rather than filled in."""
    assert Settings.model_fields["upload_source"].default is None
    assert LLM_SOURCE == "llm"      # discover's own default is unchanged


def test_upload_refuses_to_start_without_a_source():
    import pytest

    from ingestion_workflow.services.upload import (
        UploadSourceNotSet,
        resolve_upload_source,
    )

    with pytest.raises(UploadSourceNotSet):
        resolve_upload_source(Settings())
    with pytest.raises(UploadSourceNotSet):
        resolve_upload_source(Settings(upload_source="   "))
    assert resolve_upload_source(Settings(upload_source="nuextract-v19")) == "nuextract-v19"



def test_upload_refuses_to_start_without_a_token(tmp_path, monkeypatch):
    """The upload stage posts through the ingester, which takes no source; what it will not
    start without is its admin token. Planning needs none, and the first article sent
    records the stop before anything goes out."""
    from types import SimpleNamespace

    import pytest
    from ingestion_workflow.catalog import Artifact, Status
    from ingestion_workflow.models.ids import Identifier
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.upload import UploadStage
    from ingestion_workflow.services.neurostore_ingest import NeurostoreAuthError

    monkeypatch.chdir(tmp_path)  # no .env here
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    stage = UploadStage(Settings())
    monkeypatch.setattr(stage, "held_back", lambda *a: {})
    synced = Artifact("a1", "sync", status=Status.OK, fingerprint="sy",
                      summary={"base_study_id": "B1", "path": str(tmp_path / "B1"), "analyses": 2})
    work = Work(ref=SimpleNamespace(id="a1", identifier=Identifier(pmid="1")), source="",
                fingerprint="f", upstream=synced)
    ctx = SimpleNamespace(catalog=SimpleNamespace(artifacts=lambda ids, s: {},
                                                  add_aliases=lambda learned: None))
    [stopped] = stage.execute(ctx, [work])
    assert stopped.status is Status.FAILED and stopped.error.startswith("NeurostoreAuthError")
    with pytest.raises(NeurostoreAuthError):
        list(stage.execute(ctx, [work]))


def test_upload_takes_the_source_from_settings_not_a_constant():
    """It used to be hardcoded, which made the extractor unrecordable."""
    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._get_or_create_study)
    assert "resolve_upload_source(self.settings)" in src
    assert 'payload.source = payload.source or "llm"' not in src


def test_upload_matches_an_existing_version_by_source():
    """This is why a new extractor adds a version rather than overwriting:
    the lookup is keyed on source."""
    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._get_or_create_study)
    assert "version.source == payload.source" in src


def test_discover_asks_about_a_source_it_is_given():
    """Otherwise pointing the pipeline at a new extractor would leave discover
    asking about the old one, and it would report no work to do."""
    params = inspect.signature(unprocessed_base_studies).parameters
    assert "source" in params
    assert params["source"].default == LLM_SOURCE


def test_the_cli_passes_the_configured_source_to_discover():
    """The parameter is only useful if the one real caller uses it."""
    from ingestion_workflow.cli import main

    src = inspect.getsource(main._discover_from_neurostore)
    assert "source=_upload_source(settings)" in src


def test_a_configured_source_survives_settings_construction():
    settings = Settings(upload_source="nuextract-v19")
    assert settings.upload_source == "nuextract-v19"


def test_the_payload_default_does_not_shadow_the_setting():
    """`payload.source or settings.upload_source` only reaches the setting if
    the payload default is falsy. A default of "llm" is truthy and would win
    silently, which is how this was wrong the first time."""
    from ingestion_workflow.models.upload import StudyPayload

    assert StudyPayload().source is None


def test_an_explicit_payload_source_still_wins():
    """A caller that names the source means it."""
    from ingestion_workflow.models.upload import StudyPayload

    payload = StudyPayload(source="hand-curated")
    assert (payload.source or "llm") == "hand-curated"


def test_the_neurostore_api_is_part_of_the_upload_fingerprint():
    """A different API is a different neurostore, not a relabelling of the same one: pointing
    the run at production after staging sends every parse there, rather than leaving every
    article uploaded to staging looking fresh."""
    from ingestion_workflow.catalog import Artifact
    from ingestion_workflow.pipeline.stages.upload import UploadStage

    synced = Artifact("a1", "sync", fingerprint="sy")
    staging = UploadStage(Settings(neurostore_env="staging")).fingerprint_for(synced)
    production = UploadStage(Settings(neurostore_env="production")).fingerprint_for(synced)
    assert staging != production
    assert UploadStage(Settings(neurostore_env="staging")).fingerprint_for(synced) == staging

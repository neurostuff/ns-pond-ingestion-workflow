"""The fixed bundle is taken only when every file matches its pinned hash, from disk or the Hub."""

import hashlib

import pytest

pytest.importorskip("pondie")

from ingestion_workflow.normalization import api  # noqa: E402


@pytest.fixture
def bundle(tmp_path, monkeypatch):
    files = {"a.pkl": b"route b", "taskextractor/b.jsonl": b'{"mid": 1}\n'}
    for name, data in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_bytes(data)
    monkeypatch.setattr(api, "FIXED", {k: hashlib.sha256(v).hexdigest() for k, v in files.items()})
    return tmp_path


def test_a_matching_bundle_is_taken(bundle):
    assert api.verify_fixed(bundle) == bundle


def test_a_changed_or_missing_file_is_refused(bundle):
    (bundle / "a.pkl").write_bytes(b"something else")
    (bundle / "taskextractor/b.jsonl").unlink()
    with pytest.raises(ValueError, match=r"\['a.pkl', 'taskextractor/b.jsonl'\]"):
        api.verify_fixed(bundle)


def test_the_hub_is_read_at_the_pinned_revision_only(bundle, monkeypatch):
    import huggingface_hub

    asked = {}

    def snapshot(repo, revision=None, allow_patterns=None):
        asked.update(repo=repo, revision=revision, allow_patterns=allow_patterns)
        return str(bundle)

    monkeypatch.setattr(huggingface_hub, "snapshot_download", snapshot)
    monkeypatch.setattr(api, "FIXED_REVISION", None)
    with pytest.raises(ValueError, match="no pinned revision"):
        api.fixed_from_hub()
    monkeypatch.setattr(api, "FIXED_REVISION", "0123abc")
    assert api.fixed_from_hub() == bundle
    assert asked == {"repo": api.FIXED_REPO, "revision": "0123abc",
                     "allow_patterns": ["a.pkl", "taskextractor/b.jsonl"]}

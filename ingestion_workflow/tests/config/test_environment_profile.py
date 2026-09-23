"""Switching deployment must be one setting, not six."""

from __future__ import annotations

import json

import pytest

from ingestion_workflow.config import (
    NEUROSTORE_PROFILES,
    NeurostoreEnv,
    environment_profile,
    load_settings,
)

COUPLED = (
    "upload_ssh_host",
    "upload_ssh_user",
    "upload_remote_bind_host",
    "upload_remote_container_network",
    "upload_local_forward_port",
)


@pytest.fixture()
def config(tmp_path):
    def build(**extra):
        path = tmp_path / f"c{len(list(tmp_path.iterdir()))}.yaml"
        path.write_text(
            json.dumps(
                {
                    "data_root": str(tmp_path / "d"),
                    "cache_root": str(tmp_path / "c"),
                    "catalog_root": str(tmp_path / "k"),
                    "ns_pond_root": str(tmp_path / "p"),
                    **extra,
                }
            ),
            encoding="utf-8",
        )
        return path

    return build


def test_every_profile_sets_the_whole_coupled_group():
    """A profile that moves only some of these leaves a mismatched pair, which
    is what main shipped: a staging ssh host with a production container name."""
    for name, profile in NEUROSTORE_PROFILES.items():
        assert set(profile) == set(COUPLED), f"{name} is incomplete"


def test_profiles_are_internally_consistent():
    staging = NEUROSTORE_PROFILES[NeurostoreEnv.STAGING.value]
    production = NEUROSTORE_PROFILES[NeurostoreEnv.PRODUCTION.value]
    assert "staging" in staging["upload_remote_bind_host"]
    assert staging["upload_ssh_host"] == "neurostore.xyz"
    assert production["upload_ssh_host"] == "neurostore.org"
    assert "staging" not in production["upload_remote_bind_host"]


def test_profiles_do_not_share_a_forward_port():
    """Two environments open at once must not collide on localhost."""
    ports = [p["upload_local_forward_port"] for p in NEUROSTORE_PROFILES.values()]
    assert len(set(ports)) == len(ports)


def test_the_default_is_staging(config):
    settings = load_settings(config())
    assert settings.neurostore_env is NeurostoreEnv.STAGING
    assert settings.upload_ssh_host == "neurostore.xyz"


@pytest.mark.parametrize("name", sorted(NEUROSTORE_PROFILES))
def test_one_setting_moves_the_whole_group(config, name):
    settings = load_settings(config(neurostore_env=name))
    for field, expected in NEUROSTORE_PROFILES[name].items():
        assert getattr(settings, field) == expected, field


def test_an_explicit_value_still_wins(config):
    settings = load_settings(
        config(neurostore_env="production", upload_local_forward_port=7000)
    )
    assert settings.upload_local_forward_port == 7000
    assert settings.upload_ssh_host == "neurostore.org"


def test_a_cli_override_wins_over_the_profile(config):
    settings = load_settings(
        config(neurostore_env="production"), overrides={"upload_ssh_host": "localhost"}
    )
    assert settings.upload_ssh_host == "localhost"
    assert settings.upload_remote_bind_host == "store-store-pgsql17-1"


def test_an_environment_variable_wins_over_the_profile(config, monkeypatch):
    monkeypatch.setenv("UPLOAD_SSH_USER", "someone-else")
    assert "upload_ssh_user" not in environment_profile({"neurostore_env": "production"})
    settings = load_settings(config(neurostore_env="production"))
    assert settings.upload_ssh_user == "someone-else"


def test_an_unknown_environment_is_rejected(config):
    with pytest.raises(ValueError, match="Unknown neurostore_env"):
        environment_profile({"neurostore_env": "prod"})

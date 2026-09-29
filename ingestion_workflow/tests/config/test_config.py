

def test_a_shared_cache_beside_a_private_catalog_is_warned_about(tmp_path, caplog):
    """`catalog_root` defaults to a relative path, so a run started from
    another directory gets its own catalog while still filling the shared
    cache. The stage succeeds, the cache fills, and the catalog never hears of
    it -- which is how a re-extraction of 131,667 articles went unrecorded."""
    import logging
    import os

    from ingestion_workflow.config import Settings

    shared = tmp_path / "shared"
    here = tmp_path / "elsewhere"
    here.mkdir()
    cwd = os.getcwd()
    os.chdir(here)
    try:
        with caplog.at_level(logging.WARNING):
            Settings(cache_root=str(shared / "cache")).ensure_directories()
        assert "catalog_root is relative" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.WARNING):
            Settings(cache_root=str(shared / "cache"),
                     catalog_root=str(shared / "catalog")).ensure_directories()
        assert "catalog_root is relative" not in caplog.text
    finally:
        os.chdir(cwd)


def test_both_roots_are_logged_so_a_run_can_be_traced_to_its_catalog(tmp_path, caplog):
    import logging

    from ingestion_workflow.config import Settings

    with caplog.at_level(logging.INFO):
        Settings(cache_root=str(tmp_path / "c"),
                 catalog_root=str(tmp_path / "cat"),
                 data_root=str(tmp_path / "d")).ensure_directories()
    assert "catalog_root=" in caplog.text and "cache_root=" in caplog.text

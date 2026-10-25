import pytest
from ingestion_workflow.catalog import Catalog
@pytest.fixture()
def checked(tmp_path, evidence):
    """A catalog that checks what it registers against the evidence."""
    with Catalog.open(tmp_path / "checked", evidence=evidence) as cat:
        yield cat

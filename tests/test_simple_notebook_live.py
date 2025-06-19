import os
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from ingen_fab.notebook_utils.simple_notebook import SimpleFabricNotebook

REQUIRED_ENV_VARS = [
    "FABRIC_WORKSPACE_ID",
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_SECRET",
]

@pytest.mark.e2e
def test_run_notebook_live():
    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        pytest.skip(f"Missing environment vars: {', '.join(missing)}")

    notebook = SimpleFabricNotebook(os.environ["FABRIC_WORKSPACE_ID"])
    status = notebook.run_and_wait(
        "codex_live_notebook",
        "print('hello world')",
        poll_interval=10,
        timeout=120,
    )
    assert status == "Succeeded"

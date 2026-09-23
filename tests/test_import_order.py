"""Every entry module must import cleanly on its own, in a fresh interpreter.

The credential factory (ingen_fab.az_cli.credentials) is imported by fabric_api.utils, and
onelake_utils imports fabric_api.utils; an eager import in az_cli/__init__ turned that into a
circular import whenever fabric_api.utils was imported first. The CLI and the test suite
happen to import in the other order, so this is checked in a subprocess per module.
"""

import subprocess
import sys

import pytest

FIRST_IMPORTS = [
    "import ingen_fab.fabric_api.utils",
    "import ingen_fab.az_cli.credentials",
    "import ingen_fab.az_cli.onelake_utils",
    "from ingen_fab.az_cli import OneLakeUtils",
    "import ingen_fab.fabric_cicd.promotion_utils",
]


@pytest.mark.parametrize("statement", FIRST_IMPORTS)
def test_module_imports_first_in_a_fresh_interpreter(statement):
    proc = subprocess.run(
        [sys.executable, "-c", statement],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]

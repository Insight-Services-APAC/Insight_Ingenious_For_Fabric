import pathlib
import sys
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ingen_fab.notebook_utils.fabric_cli_notebook import FabricCLINotebook


def test_cli_notebook_workflow():
    nb = FabricCLINotebook("Metcash_Test.Workspace")

    with mock.patch("subprocess.run") as run:
        run.side_effect = [
            mock.Mock(stdout="", returncode=0),
            mock.Mock(stdout="Started job 0bfcc2a7-468d-473f-92e4-9a2a799f2522", returncode=0),
            mock.Mock(stdout="Completed", returncode=0),
        ]

        nb.upload(pathlib.Path("sample.ipynb"), "codex_simple")
        job_id = nb.run("codex_simple")
        status = nb.status("codex_simple", job_id)

        assert job_id == "0bfcc2a7-468d-473f-92e4-9a2a799f2522"
        assert status == "Completed"

        expected_calls = [
            mock.call(
                [
                    "fab",
                    "import",
                    "Metcash_Test.Workspace/codex_simple.Notebook",
                    "-i",
                    "sample.ipynb",
                ],
                check=True,
                capture_output=True,
                text=True,
            ),
            mock.call(
                [
                    "fab",
                    "job",
                    "run",
                    "Metcash_Test.Workspace/codex_simple.Notebook",
                ],
                check=True,
                capture_output=True,
                text=True,
            ),
            mock.call(
                [
                    "fab",
                    "job",
                    "run-status",
                    "Metcash_Test.Workspace/codex_simple.Notebook",
                    "--id",
                    "0bfcc2a7-468d-473f-92e4-9a2a799f2522",
                ],
                check=True,
                capture_output=True,
                text=True,
            ),
        ]
        run.assert_has_calls(expected_calls)

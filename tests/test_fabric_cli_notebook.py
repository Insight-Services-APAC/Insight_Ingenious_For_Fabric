import json
import pathlib
import re
from unittest import mock

from ingen_fab.notebook_utils.fabric_cli_notebook import FabricCLINotebook

JOB_ID = "0bfcc2a7-468d-473f-92e4-9a2a799f2522"


def test_cli_notebook_workflow():
    """upload/run/status shell out to the Fabric CLI; run() hands back the raw CLI
    output and the caller extracts the job id (see notebook_commands.run_simple_notebook)."""
    nb = FabricCLINotebook("Metcash_Test")

    with mock.patch("subprocess.run") as run:
        run.side_effect = [
            mock.Mock(stdout="", returncode=0),
            mock.Mock(stdout=f"Job instance '{JOB_ID}' created", returncode=0),
            mock.Mock(stdout="Completed", returncode=0),
        ]

        nb.upload(pathlib.Path("sample.ipynb"), "codex_simple")
        run_output = nb.run("codex_simple")
        job_id = re.search(r"Job instance '([a-f0-9-]{36})'", run_output).group(1)
        status = nb.status("codex_simple", job_id)

        assert job_id == JOB_ID
        assert status == "Completed"

        expected_calls = [
            mock.call(
                [
                    "fab",
                    "import",
                    "Metcash_Test.Workspace/codex_simple.Notebook",
                    "-i",
                    "sample.ipynb",
                    "--format",
                    ".py",
                    "-f",
                ],
                check=True,
                capture_output=True,
                text=True,
            ),
            mock.call(
                [
                    "fab",
                    "job",
                    "start",
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
                    JOB_ID,
                ],
                check=True,
                capture_output=True,
                text=True,
            ),
        ]
        run.assert_has_calls(expected_calls)


def test_run_returns_none_when_cli_prints_nothing():
    nb = FabricCLINotebook("ws")
    with mock.patch("subprocess.run", return_value=mock.Mock(stdout="", returncode=0)):
        assert nb.run("nb") is None


def test_generate_functional_test_notebook(tmp_path):
    """The platform-testing templates need notebook_name, guid and test_scripts; the
    generator supplies them and writes a .platform file plus notebook-content.py."""
    nb = FabricCLINotebook("ws")

    folder = nb.generate_functional_test_notebook(
        notebook_name="smoke", test_scripts="print('hi')", output_dir=tmp_path
    )

    assert folder == tmp_path / "smoke"
    platform = json.loads((folder / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["displayName"] == "smoke"
    assert len(platform["config"]["logicalId"]) == 36
    content = (folder / "notebook-content.py").read_text(encoding="utf-8")
    assert "print('hi')" in content
    # varlib tokens survive rendering for fabric-cicd to substitute at deploy time
    assert "{{varlib:config_workspace_id}}" in content

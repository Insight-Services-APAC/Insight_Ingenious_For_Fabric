import re
import subprocess
from pathlib import Path
from typing import Optional


class FabricCLINotebook:
    """Wrapper around the Fabric CLI for notebook operations."""

    def __init__(self, workspace_name: str) -> None:
        self.workspace_name = workspace_name

    def _run_cmd(self, *args: str) -> str:
        result = subprocess.run(
            ["fab", *args], check=True, capture_output=True, text=True
        )
        return result.stdout

    def upload(self, notebook_path: Path, notebook_name: str) -> None:
        """Upload a notebook to the workspace."""
        self._run_cmd(
            "import",
            f"{self.workspace_name}/{notebook_name}.Notebook",
            "-i",
            str(notebook_path),
        )

    def run(self, notebook_name: str) -> Optional[str]:
        """Run the notebook and return the job id if found."""
        output = self._run_cmd(
            "job",
            "run",
            f"{self.workspace_name}/{notebook_name}.Notebook",
        )
        match = re.search(r"([0-9a-fA-F-]{36})", output)
        return match.group(1) if match else None

    def status(self, notebook_name: str, job_id: str) -> str:
        """Return the CLI status output for a job."""
        output = self._run_cmd(
            "job",
            "run-status",
            f"{self.workspace_name}/{notebook_name}.Notebook",
            "--id",
            job_id,
        )
        return output.strip()

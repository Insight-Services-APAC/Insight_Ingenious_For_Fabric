import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import jinja2
import requests
from azure.identity import DefaultAzureCredential


class FabricCLINotebook:
    """Wrapper around the Fabric CLI for notebook operations."""

    def __init__(self, workspace_name: str) -> None:
        self.workspace_name = workspace_name
        template_dir = Path(__file__).resolve().parent / "templates" / "platform_testing"
        self.jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
        self.platform_file_template = self.jinja_env.get_template("platform_file_template.json.jinja")
        self.notebook_content_template = self.jinja_env.get_template("notebook-content_template-python.py.jinja")

    def generate_functional_test_notebook(self, notebook_name: str = "test_notebook"):
        """Generate a functional test notebook using the Fabric CLI."""
        # Render the jinja templates
        platform_file_content = self.platform_file_template.render(guid=str(uuid.uuid4()), notebook_name=notebook_name)
        notebook_content = self.notebook_content_template.render(
            content="print('Hello, Fabric!')",
        )
        # Create a temporary directory for the notebook
        temp_dir = Path.cwd() / "output" / Path("codex_test_notebook")
        temp_dir.mkdir(parents=True, exist_ok=True)
        notebook_path = temp_dir / "notebook-content.py"
        # Write the notebook content to a file
        with open(notebook_path, "w", encoding="utf-8") as f:
            f.write(notebook_content)
        # Write the platform file content to a file
        platform_file_path = temp_dir / ".platform"
        with open(platform_file_path, "w", encoding="utf-8") as f:
            f.write(platform_file_content)

    def upload(self, notebook_path: Path, notebook_name: str, format: str = ".py") -> None:
        """Upload a notebook to the workspace."""
        cmd = [
            "fab",
            "import",
            f"{self.workspace_name}.Workspace/{notebook_name}.Notebook",
            "-i",
            str(notebook_path),
            "--format",
            format,
            "-f",
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)

    def run(self, notebook_name: str) -> Optional[str]:
        """Run the notebook and return the job id if found."""
        cmd = [
            "fab",
            "job",
            "start",
            f"{self.workspace_name}.Workspace/{notebook_name}.Notebook",
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return result.stdout.strip() if result.stdout else None

    def status(self, notebook_name: str, job_id: str) -> str:
        """Return the CLI status output for a job."""
        cmd = [
            "fab",
            "job",
            "run-status",
            f"{self.workspace_name}.Workspace/{notebook_name}.Notebook",
            "--id",
            job_id,
        ]

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return result.stdout.strip()

    def delete_item(self, item_name: str, item_type: str) -> bool:
        """Delete an item from the workspace."""
        cmd = [
            "fab",
            "delete",
            f"{self.workspace_name}.Workspace/{item_name}.{item_type}",
            "-f",
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def list_items(self) -> list[dict[str, Any]]:
        """List all items in the workspace."""
        cmd = ["fab", "list", f"{self.workspace_name}.Workspace", "--output", "json"]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)

        if result.stdout:
            import json

            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return []
        return []


class FabricLivyNotebook:
    """Wrapper around the Fabric Livy endpoint for notebook operations."""

    def __init__(self, workspace_id: str, lakehouse_id: str = None) -> None:
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.credential = DefaultAzureCredential()
        self.base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        if lakehouse_id:
            self.livy_url = f"{self.base_url}/{lakehouse_id}/livyapi/versions/2023-12-01/sessions"

    def _get_token(self) -> str:
        """Get authentication token for Fabric API."""
        scope = "https://api.fabric.microsoft.com/.default"
        token = self.credential.get_token(scope)
        return token.token

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with authentication."""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def create_session(self) -> str:
        """Create a new Spark session and return session ID."""
        if not self.lakehouse_id:
            raise ValueError("Lakehouse ID is required to create a Spark session")

        payload = {
            "name": f"fabric-notebook-session-{int(time.time())}",
            "kind": "pyspark",
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        }

        response = requests.post(self.livy_url, headers=self._get_headers(), json=payload)
        response.raise_for_status()

        session_data = response.json()
        return session_data["id"]

    def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get the status of a Spark session."""
        response = requests.get(f"{self.livy_url}/{session_id}", headers=self._get_headers())
        response.raise_for_status()
        return response.json()

    def wait_for_session_ready(self, session_id: str, timeout: int = 300) -> bool:
        """Wait for session to be ready, returns True if ready, False if timeout."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            status = self.get_session_status(session_id)
            state = status.get("state", "unknown")

            if state == "idle":
                return True
            elif state in ["dead", "error", "killed"]:
                raise RuntimeError(f"Session failed with state: {state}")

            time.sleep(5)

        return False

    def execute_code(self, session_id: str, code: str) -> Dict[str, Any]:
        """Execute code in the Spark session."""
        payload = {"code": code, "kind": "pyspark"}

        response = requests.post(
            f"{self.livy_url}/{session_id}/statements",
            headers=self._get_headers(),
            json=payload,
        )
        response.raise_for_status()

        return response.json()

    def get_statement_status(self, session_id: str, statement_id: int) -> Dict[str, Any]:
        """Get the status and result of a statement."""
        response = requests.get(
            f"{self.livy_url}/{session_id}/statements/{statement_id}",
            headers=self._get_headers(),
        )
        response.raise_for_status()
        return response.json()

    def wait_for_statement_completion(self, session_id: str, statement_id: int, timeout: int = 300) -> Dict[str, Any]:
        """Wait for statement to complete and return the result."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            result = self.get_statement_status(session_id, statement_id)
            state = result.get("state", "unknown")

            if state == "available":
                return result
            elif state in ["error", "cancelled"]:
                return result

            time.sleep(2)

        raise TimeoutError(f"Statement {statement_id} did not complete within {timeout} seconds")

    def run_notebook_code(self, code: str, timeout: int = 600) -> Dict[str, Any]:
        """
        Run notebook code using Livy endpoint.
        Returns execution result with output and any errors.
        """
        try:
            # Create session
            session_id = self.create_session()
            print(f"Created session: {session_id}")

            # Wait for session to be ready
            if not self.wait_for_session_ready(session_id):
                raise TimeoutError("Session did not become ready in time")

            print("Session is ready, executing code...")

            # Execute code
            statement = self.execute_code(session_id, code)
            statement_id = statement["id"]

            # Wait for completion
            result = self.wait_for_statement_completion(session_id, statement_id, timeout)

            return {
                "session_id": session_id,
                "statement_id": statement_id,
                "state": result.get("state"),
                "output": result.get("output", {}),
                "progress": result.get("progress", 0),
            }

        except Exception as e:
            return {"error": str(e), "state": "error"}
        finally:
            # Clean up session if it exists
            try:
                if "session_id" in locals():
                    self.delete_session(session_id)
            except:  # noqa: E722
                pass  # Ignore cleanup errors

    def delete_session(self, session_id: str) -> None:
        """Delete a Spark session."""
        try:
            response = requests.delete(f"{self.livy_url}/{session_id}", headers=self._get_headers())
            response.raise_for_status()
            print(f"Deleted session: {session_id}")
        except Exception as e:
            print(f"Warning: Could not delete session {session_id}: {e}")

    def run_template_notebook(self, code: str, timeout: int = 600) -> Dict[str, Any]:
        """
        Run a notebook using the template format with error handling.
        This works with the notebook-content_template.py.jinja format.
        """
        # The template already includes try/catch with mssparkutils.notebook.exit()
        # We can execute the content directly
        wrapped_code = f"""
import traceback
from notebookutils import mssparkutils

try:
    {code}
except Exception as e:
    # Get full stack trace
    error_message = f"Error occurred: {{str(e)}}\\n\\nFull stack trace:\\n{{traceback.format_exc()}}"
    print(error_message)
    # Exit notebook with error details
    mssparkutils.notebook.exit(error_message)
"""

        return self.run_notebook_code(wrapped_code, timeout)

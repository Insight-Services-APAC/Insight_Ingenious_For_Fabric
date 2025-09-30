"""
Test documentation accuracy against codebase implementation.

This test suite validates that documentation claims match the actual implementation
through static code analysis. It does NOT execute any commands or code, only
analyzes the source files.
"""

import re
from pathlib import Path
from typing import Set

import pytest


class TestDocumentationAccuracy:
    """Test suite for validating documentation against implementation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        self.root_dir = Path(__file__).parent.parent.parent
        self.docs_dir = self.root_dir / "docs"
        self.cli_file = self.root_dir / "ingen_fab" / "cli.py"
        self.readme_file = self.root_dir / "README.md"

    def test_cli_commands_exist(self):
        """Verify all documented CLI commands exist in the implementation."""
        # Parse CLI file to extract command groups and commands
        cli_commands = self._extract_cli_commands()

        # Parse documentation to find documented commands
        documented_commands = self._extract_documented_commands()

        # Verify all documented commands exist
        missing_commands = documented_commands - cli_commands
        assert not missing_commands, f"Commands documented but not implemented: {missing_commands}"

    def test_environment_variables_documented(self):
        """Verify all environment variables used in code are documented."""
        # Find all environment variable references in code
        env_vars_in_code = self._find_environment_variables()

        # Find documented environment variables
        documented_env_vars = self._extract_documented_env_vars()

        # Check for undocumented variables (excluding test-specific ones)
        undocumented = env_vars_in_code - documented_env_vars - {"HOME", "PATH", "USER", "PWD", "SHELL", "TERM", "LANG"}
        assert not undocumented, f"Environment variables used but not documented: {undocumented}"

    def test_package_commands_accuracy(self):
        """Verify package commands match implementation."""
        cli_source = self.cli_file.read_text()

        # Check for package subcommands
        assert "package_app.add_typer" in cli_source, "Package app should have subcommands"
        assert "ingest_app" in cli_source, "Ingest package should exist"
        assert "synapse_app" in cli_source, "Synapse package should exist"
        assert "extract_app" in cli_source, "Extract package should exist"

    def test_python_version_requirement(self):
        """Verify Python version requirement matches pyproject.toml."""
        pyproject_path = self.root_dir / "pyproject.toml"
        if pyproject_path.exists():
            content = pyproject_path.read_text()
            # Extract Python version requirement
            match = re.search(r'requires-python\s*=\s*"([^"]+)"', content)
            if match:
                version_req = match.group(1)

                # Check README mentions correct version
                readme_content = self.readme_file.read_text()
                assert "Python 3.12" in readme_content or version_req in readme_content, (
                    f"README should mention Python version requirement: {version_req}"
                )

    def test_project_structure_documentation(self):
        """Verify documented project structure matches actual structure."""
        # Key directories that should exist
        expected_dirs = [
            "ingen_fab/cli_utils",
            "ingen_fab/ddl_scripts",
            "ingen_fab/notebook_utils",
            "ingen_fab/packages",
            "ingen_fab/python_libs",
            "sample_project",
            "docs",
            "tests",
        ]

        for dir_path in expected_dirs:
            full_path = self.root_dir / dir_path
            assert full_path.exists(), f"Expected directory not found: {dir_path}"

    def test_deploy_get_metadata_command(self):
        """Verify deploy get-metadata command replaced extract lakehouse-metadata."""
        cli_source = self.cli_file.read_text()

        # Check deploy get-metadata exists
        assert (
            '@deploy_app.command("get-metadata")' in cli_source
            or '@deploy_app.command(name="get-metadata")' in cli_source
        ), "deploy get-metadata command should exist"

        # Check old command is removed/commented
        assert (
            "lakehouse-metadata (now under deploy get-metadata)" in cli_source
            or "# Removed: lakehouse-metadata" in cli_source
        ), "Old lakehouse-metadata command should be marked as moved"

    def test_dbt_commands_exist(self):
        """Verify dbt commands are properly implemented."""
        cli_source = self.cli_file.read_text()

        # Check dbt app exists
        assert "dbt_app = typer.Typer()" in cli_source, "DBT app should be defined"
        assert 'name="dbt"' in cli_source, "DBT command group should be registered"

        # Check specific dbt commands
        assert (
            '@dbt_app.command("create-notebooks")' in cli_source
            or '@dbt_app.command(name="create-notebooks")' in cli_source
        ), "dbt create-notebooks command should exist"

    def test_sample_project_structure(self):
        """Verify sample_project has expected structure."""
        sample_project = self.root_dir / "sample_project"

        expected_items = [
            "fabric_workspace_items/config/var_lib.VariableLibrary",
            "fabric_workspace_items/lakehouses",
            "fabric_workspace_items/warehouses",
            "ddl_scripts/Lakehouses",
            "ddl_scripts/Warehouses",
            "platform_manifest_development.yml",
        ]

        for item in expected_items:
            full_path = sample_project / item
            assert full_path.exists(), f"Sample project missing: {item}"

    def test_dependency_groups_documented(self):
        """Verify dependency groups match pyproject.toml."""
        pyproject_path = self.root_dir / "pyproject.toml"
        if pyproject_path.exists():
            content = pyproject_path.read_text()

            # Check for dependency-groups section (uv format)
            if "[dependency-groups]" in content:
                assert "dev = [" in content, "dev dependency group should exist"
                assert "docs = [" in content, "docs dependency group should exist"
                assert "dbt = [" in content, "dbt dependency group should exist"

    def test_cli_help_snippets_accuracy(self):
        """Verify CLI help snippets match actual command structure."""
        snippet_dir = self.docs_dir / "snippets" / "cli"
        if snippet_dir.exists():
            for snippet_file in snippet_dir.glob("*_help.md"):
                # Extract command from filename (e.g., "ddl_help.md" -> "ddl")
                command = snippet_file.stem.replace("_help", "")
                if command != "root":
                    # Verify command exists in CLI
                    cli_source = self.cli_file.read_text()
                    assert f"{command}_app = typer.Typer()" in cli_source or f'name="{command}"' in cli_source, (
                        f"Command {command} referenced in snippets should exist in CLI"
                    )

    def test_readme_no_outdated_extract_command(self):
        """Verify README doesn't contain outdated extract lakehouse-metadata command."""
        readme_content = self.readme_file.read_text()
        assert "extract lakehouse-metadata" not in readme_content, (
            "README should not contain outdated 'extract lakehouse-metadata' command"
        )
        assert "deploy get-metadata" in readme_content, "README should document the new 'deploy get-metadata' command"

    # Helper methods for parsing and extraction

    def _extract_cli_commands(self) -> Set[str]:
        """Extract all CLI commands from the CLI module."""
        commands = set()
        cli_source = self.cli_file.read_text()

        # Find main command groups
        for match in re.finditer(r'name="([^"]+)".*help=', cli_source):
            commands.add(match.group(1))

        # Find individual commands
        for match in re.finditer(r'@(\w+_app)\.command\("?([^")\s]+)"?\)', cli_source):
            app_name = match.group(1).replace("_app", "")
            cmd_name = match.group(2) if match.group(2) else "default"
            commands.add(f"{app_name} {cmd_name}")

        return commands

    def _extract_documented_commands(self) -> Set[str]:
        """Extract commands mentioned in documentation."""
        commands = set()

        # Check README
        readme_content = self.readme_file.read_text()
        for match in re.finditer(r"ingen_fab\s+(\w+)(?:\s+(\w+))?", readme_content):
            if match.group(2):
                commands.add(f"{match.group(1)} {match.group(2)}")
            else:
                commands.add(match.group(1))

        # Check CLI reference
        cli_ref = self.docs_dir / "user_guide" / "cli_reference.md"
        if cli_ref.exists():
            content = cli_ref.read_text()
            for match in re.finditer(r"####?\s+`?(\w+)(?:\s+(\w+))?`?", content):
                if match.group(2):
                    commands.add(f"{match.group(1)} {match.group(2)}")
                else:
                    commands.add(match.group(1))

        return commands

    def _find_environment_variables(self) -> Set[str]:
        """Find all environment variables referenced in the codebase."""
        env_vars = set()

        # Search Python files for os.environ and os.getenv
        for py_file in self.root_dir.rglob("*.py"):
            if ".venv" in str(py_file) or "__pycache__" in str(py_file):
                continue

            try:
                content = py_file.read_text()
                # Find os.environ.get("VAR") or os.getenv("VAR")
                for match in re.finditer(r'os\.(?:environ\.get|getenv)\s*\(\s*["\']([^"\']+)["\']', content):
                    env_vars.add(match.group(1))
                # Find os.environ["VAR"]
                for match in re.finditer(r'os\.environ\s*\[\s*["\']([^"\']+)["\']', content):
                    env_vars.add(match.group(1))
            except Exception:
                continue

        return env_vars

    def _extract_documented_env_vars(self) -> Set[str]:
        """Extract environment variables from documentation."""
        env_vars = set()

        # Check environment variables documentation
        env_doc = self.docs_dir / "user_guide" / "environment_variables.md"
        if env_doc.exists():
            content = env_doc.read_text()
            # Find variables in markdown table
            for match in re.finditer(r"\|\s*`([A-Z_]+)`\s*\|", content):
                env_vars.add(match.group(1))

        # Also check README
        readme_content = self.readme_file.read_text()
        for match in re.finditer(r"export\s+([A-Z_]+)=", readme_content):
            env_vars.add(match.group(1))

        return env_vars


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])

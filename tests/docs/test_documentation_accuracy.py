"""
Test documentation accuracy by validating key claims and examples.

This test module validates that documentation examples and claims match
the actual implementation. It performs static validation without executing
commands.
"""

import ast
import re
from pathlib import Path
from typing import List, Tuple

import pytest


class DocumentationValidator:
    """Validates documentation against codebase implementation."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.docs_dir = self.project_root / "docs"
        self.cli_file = self.project_root / "ingen_fab" / "cli.py"

    def extract_commands_from_cli(self) -> dict:
        """Extract command structure from CLI module."""
        commands = {"main_commands": [], "subcommands": {}}

        with open(self.cli_file, "r") as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "add_typer"
                    and len(node.args) >= 1
                ):
                    # Extract subcommand registration
                    for keyword in node.keywords:
                        if keyword.arg == "name":
                            if isinstance(keyword.value, ast.Constant):
                                command_name = keyword.value.value
                                commands["main_commands"].append(command_name)

            # Look for @app.command decorators
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if (
                        isinstance(decorator, ast.Attribute)
                        and decorator.attr == "command"
                    ):
                        # This is a command function
                        commands.setdefault("functions", []).append(node.name)

        return commands

    def extract_code_blocks_from_markdown(self, file_path: Path) -> List[str]:
        """Extract code blocks from markdown files."""
        with open(file_path, "r") as f:
            content = f.read()

        # Extract bash code blocks
        bash_blocks = re.findall(r"```bash\n(.*?)\n```", content, re.DOTALL)
        # Extract generic code blocks that might contain commands
        generic_blocks = re.findall(r"```\n(ingen_fab.*?)\n```", content, re.DOTALL)

        return bash_blocks + generic_blocks

    def validate_command_syntax(self, command: str) -> Tuple[bool, str]:
        """Validate that a command has correct syntax."""
        # Skip comments and empty lines
        command = command.strip()
        if not command or command.startswith("#"):
            return True, ""

        # Only validate ingen_fab commands
        if not command.startswith("ingen_fab"):
            return True, ""

        # Check for known command patterns
        valid_patterns = [
            r"ingen_fab init new",
            r"ingen_fab init workspace",
            r"ingen_fab ddl compile",
            r"ingen_fab deploy deploy",
            r"ingen_fab deploy delete-all",
            r"ingen_fab deploy upload-python-libs",
            r"ingen_fab notebook",
            r"ingen_fab test",
            r"ingen_fab package ingest",
            r"ingen_fab package synapse",
            r"ingen_fab libs compile",
        ]

        for pattern in valid_patterns:
            if re.match(pattern, command):
                return True, ""

        return False, f"Command doesn't match known patterns: {command}"

    def validate_import_statements(self, file_path: Path) -> List[str]:
        """Validate Python import statements in documentation."""
        errors = []
        with open(file_path, "r") as f:
            content = f.read()

        # Extract Python code blocks
        python_blocks = re.findall(r"```python\n(.*?)\n```", content, re.DOTALL)

        for block in python_blocks:
            # Check for imports
            imports = re.findall(
                r"^(?:from|import)\s+(.+?)(?:\s+import|\s*$)", block, re.MULTILINE
            )
            for imp in imports:
                # Skip relative imports in examples
                if imp.startswith("."):
                    continue

                # Check if it's a project import
                if "ingen_fab" in imp or any(
                    lib in imp
                    for lib in ["lakehouse_utils", "warehouse_utils", "ddl_utils"]
                ):
                    # For injected libraries (without ingen_fab prefix), these are valid in notebook context
                    if not imp.startswith("ingen_fab") and any(
                        lib in imp
                        for lib in ["lakehouse_utils", "warehouse_utils", "ddl_utils"]
                    ):
                        continue  # These are valid in generated notebooks

                    # For explicit imports, check they use correct prefix
                    if "common" in imp or "python" in imp or "pyspark" in imp:
                        if not imp.startswith("ingen_fab.python_libs"):
                            errors.append(
                                f"Import should start with 'ingen_fab.python_libs': {imp}"
                            )

        return errors


class TestDocumentationAccuracy:
    """Test suite for documentation accuracy."""

    @pytest.fixture
    def validator(self):
        return DocumentationValidator()

    def test_cli_commands_exist(self, validator):
        """Test that documented CLI commands actually exist."""
        commands = validator.extract_commands_from_cli()

        # Expected main commands based on documentation
        expected_commands = [
            "deploy",
            "init",
            "ddl",
            "test",
            "notebook",
            "package",
            "libs",
        ]

        for cmd in expected_commands:
            assert cmd in commands["main_commands"], f"Command '{cmd}' not found in CLI"

    def test_readme_command_examples(self, validator):
        """Test that README.md command examples are valid."""
        readme_path = validator.project_root / "README.md"
        code_blocks = validator.extract_code_blocks_from_markdown(readme_path)

        errors = []
        for block in code_blocks:
            for line in block.split("\n"):
                if line.strip().startswith("ingen_fab"):
                    valid, error = validator.validate_command_syntax(line)
                    if not valid:
                        errors.append(error)

        assert not errors, f"Invalid commands in README.md: {errors}"

    def test_cli_reference_commands(self, validator):
        """Test that CLI reference documentation matches implementation."""
        cli_ref_path = validator.docs_dir / "user_guide" / "cli_reference.md"
        code_blocks = validator.extract_code_blocks_from_markdown(cli_ref_path)

        errors = []
        for block in code_blocks:
            for line in block.split("\n"):
                if line.strip().startswith("ingen_fab"):
                    valid, error = validator.validate_command_syntax(line)
                    if not valid:
                        errors.append(error)

        assert not errors, f"Invalid commands in CLI reference: {errors}"

    def test_python_library_imports(self, validator):
        """Test that Python library import examples are correct."""
        python_libs_doc = validator.docs_dir / "developer_guide" / "python_libraries.md"
        errors = validator.validate_import_statements(python_libs_doc)

        assert not errors, f"Invalid imports in python_libraries.md: {errors}"

    def test_no_nonexistent_options(self, validator):
        """Test that documentation doesn't reference non-existent options."""
        # Check for known removed/non-existent options
        nonexistent_patterns = [
            r"--version",  # No version flag
            r"--dry-run",  # No dry-run option
            r"ingen_fab run",  # No run command
        ]

        docs_files = list(validator.docs_dir.rglob("*.md"))
        docs_files.append(validator.project_root / "README.md")

        errors = []
        for doc_file in docs_files:
            with open(doc_file, "r") as f:
                content = f.read()

            for pattern in nonexistent_patterns:
                if re.search(pattern, content):
                    errors.append(
                        f"Found non-existent pattern '{pattern}' in {doc_file}"
                    )

        assert not errors, f"References to non-existent features: {errors}"

    def test_ddl_compile_options(self, validator):
        """Test that DDL compile command options are correct."""
        # Valid options for ddl compile
        valid_output_modes = ["fabric_workspace_repo", "local"]
        valid_generation_modes = ["Warehouse", "Lakehouse"]

        docs_files = list(validator.docs_dir.rglob("*.md"))
        docs_files.append(validator.project_root / "README.md")

        errors = []
        for doc_file in docs_files:
            with open(doc_file, "r") as f:
                content = f.read()

            # Find ddl compile commands
            ddl_commands = re.findall(
                r"ingen_fab ddl compile.*?(?=\n(?![\s\\]))", content, re.DOTALL
            )

            for cmd in ddl_commands:
                # Check output-mode
                output_mode_match = re.search(r"--output-mode\s+(\S+)", cmd)
                if output_mode_match:
                    mode = output_mode_match.group(1)
                    if mode not in valid_output_modes:
                        errors.append(f"Invalid output-mode '{mode}' in {doc_file}")

                # Check generation-mode
                gen_mode_match = re.search(r"--generation-mode\s+(\S+)", cmd)
                if gen_mode_match:
                    mode = gen_mode_match.group(1)
                    if mode not in valid_generation_modes:
                        errors.append(f"Invalid generation-mode '{mode}' in {doc_file}")

        assert not errors, f"Invalid DDL compile options: {errors}"

    def test_project_structure_accuracy(self, validator):
        """Test that documented project structure matches templates."""
        template_dir = validator.project_root / "ingen_fab" / "project_templates"

        # Check that documented paths exist in templates
        expected_paths = [
            "ddl_scripts/Lakehouses/Config/001_Initial_Creation",
            "ddl_scripts/Warehouses/Config_WH/001_Initial_Creation",
            "ddl_scripts/Warehouses/Sample_WH/001_Initial_Creation",
            "fabric_workspace_items/config/var_lib.VariableLibrary",
        ]

        errors = []
        for path in expected_paths:
            full_path = template_dir / path
            if not full_path.exists():
                errors.append(f"Expected path doesn't exist in templates: {path}")

        assert not errors, f"Project structure mismatches: {errors}"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])

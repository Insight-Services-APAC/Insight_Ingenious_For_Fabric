import json
import os
import shutil
import uuid
from pathlib import Path

import typer
import yaml
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.fabric_api.utils import FabricApiUtils
from ingen_fab.python_libs.common.utils.path_utils import PathUtils


def init_solution(project_name: str | None, path: Path, with_samples: bool = False):
    console = Console()

    # Determine project name and path
    if project_name is None:
        # Try to get project name from FABRIC_WORKSPACE_REPO_DIR environment variable
        fabric_workspace_repo_dir = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
        if fabric_workspace_repo_dir:
            # Use the basename of the FABRIC_WORKSPACE_REPO_DIR as project name
            # and the parent directory as the path
            workspace_path = Path(fabric_workspace_repo_dir)
            project_name = workspace_path.name
            project_path = workspace_path
            ConsoleStyles.print_info(
                console,
                f"Using FABRIC_WORKSPACE_REPO_DIR for project: {project_name} at {project_path}",
            )
        else:
            ConsoleStyles.print_error(
                console,
                "❌ Project name is required when FABRIC_WORKSPACE_REPO_DIR is not set.",
            )
            ConsoleStyles.print_info(
                console,
                "💡 Either set FABRIC_WORKSPACE_REPO_DIR or use --project-name option.",
            )
            raise typer.Exit(code=1)
    else:
        # Use the provided project name and path
        project_path = Path(path) / project_name

    # Get the templates directory using the new path utilities
    try:
        if with_samples:
            # Use sample_project as the template source
            templates_dir = PathUtils.get_package_resource_path("sample_project")
            ConsoleStyles.print_info(
                console,
                "Using sample_project as template (includes sample data and configurations)",
            )
        else:
            # Use the default project_templates
            templates_dir = PathUtils.get_template_path("project")
    except FileNotFoundError as e:
        ConsoleStyles.print_error(console, f"❌ Project templates not found: {e}")
        return

    if not templates_dir.exists():
        ConsoleStyles.print_error(
            console, f"❌ Templates directory does not exist: {templates_dir}"
        )
        return

    # Create the project directory
    project_path.mkdir(parents=True, exist_ok=True)

    ConsoleStyles.print_info(
        console, f"Creating new Fabric project '{project_name}' at {project_path}"
    )

    # Copy all template files and directories
    for item in templates_dir.iterdir():
        dest = project_path / item.name
        if item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
            ConsoleStyles.print_info(console, f"  ✓ Copied directory: {item.name}")
        else:
            shutil.copy2(item, dest)
            ConsoleStyles.print_info(console, f"  ✓ Copied file: {item.name}")

    # Process template files that need variable substitution
    _process_template_files(project_path, project_name, console)

    ConsoleStyles.print_success(
        console, f"✓ Initialized Fabric solution '{project_name}' at {project_path}"
    )
    # Get the absolute path for the instructions
    absolute_project_path = project_path.resolve()

    ConsoleStyles.print_info(console, "\nNext steps:")
    ConsoleStyles.print_info(console, "1. Set environment variables:")
    ConsoleStyles.print_info(
        console, f"   export FABRIC_WORKSPACE_REPO_DIR='{absolute_project_path}'"
    )
    ConsoleStyles.print_info(console, "   export FABRIC_ENVIRONMENT='development'")
    ConsoleStyles.print_info(
        console,
        "2. Update variable values in fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/",
    )
    ConsoleStyles.print_info(
        console,
        "   - Replace placeholder GUIDs with your actual workspace and lakehouse IDs",
    )
    ConsoleStyles.print_info(
        console, "3. Create additional DDL scripts in ddl_scripts/ as needed"
    )
    ConsoleStyles.print_info(console, "4. Generate DDL notebooks:")
    ConsoleStyles.print_info(
        console,
        "   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse",
    )
    ConsoleStyles.print_info(console, "5. Deploy to Fabric:")
    ConsoleStyles.print_info(
        console, "   ingen_fab deploy deploy --environment development"
    )


def _process_template_files(project_path: Path, project_name: str, console: Console):
    """Process template files that need variable substitution"""

    # Process README.md for project name substitution
    readme_path = project_path / "README.md"
    if readme_path.exists():
        content = readme_path.read_text()
        content = content.replace("{project_name}", project_name)
        readme_path.write_text(content)
        ConsoleStyles.print_info(console, "  ✓ Processed template: README.md")

    # Find and process all .platform files
    platform_files = list(project_path.rglob("*.platform"))

    if platform_files:
        ConsoleStyles.print_info(
            console, f"  Found {len(platform_files)} .platform files to process"
        )

        for platform_file in platform_files:
            try:
                # Generate a unique GUID for each .platform file
                unique_guid = str(uuid.uuid4())

                # Read and parse the JSON content
                content = platform_file.read_text(encoding="utf-8")

                # Replace GUID placeholders and any project name references
                content = content.replace("REPLACE_WITH_UNIQUE_GUID", unique_guid)
                content = content.replace("{project_name}", project_name)

                # For files that might have hardcoded GUIDs, replace them with new ones
                # This handles cases where template files already have GUIDs that should be unique per project
                try:
                    json_data = json.loads(content)
                    if "config" in json_data and "logicalId" in json_data["config"]:
                        # If logicalId exists and is not already "REPLACE_WITH_UNIQUE_GUID",
                        # it might be a hardcoded GUID that should be replaced
                        existing_logical_id = json_data["config"]["logicalId"]
                        if (
                            existing_logical_id != "REPLACE_WITH_UNIQUE_GUID"
                            and len(existing_logical_id) > 30
                        ):
                            # This looks like a GUID, replace it with a new one
                            json_data["config"]["logicalId"] = unique_guid
                            content = json.dumps(json_data, indent=2)
                except json.JSONDecodeError:
                    # If it's not valid JSON, just do string replacement
                    pass

                platform_file.write_text(content, encoding="utf-8")

                # Get relative path for display
                relative_path = platform_file.relative_to(project_path)
                ConsoleStyles.print_info(
                    console,
                    f"  ✓ Processed .platform file: {relative_path} (GUID: {unique_guid[:8]}...)",
                )

            except Exception as e:
                relative_path = platform_file.relative_to(project_path)
                ConsoleStyles.print_error(
                    console,
                    f"  ❌ Failed to process .platform file: {relative_path} - {str(e)}",
                )
    else:
        ConsoleStyles.print_info(console, "  No .platform files found to process")


def init_workspace(
    ctx: typer.Context, workspace_name: str, create_if_not_exists: bool = False
):
    """Initialize workspace configuration by looking up workspace ID from name."""
    console = Console()

    # Get project path and environment from context
    project_path = Path(ctx.obj["fabric_workspace_repo_dir"])
    environment = str(ctx.obj["fabric_environment"])

    ConsoleStyles.print_info(
        console, f"Looking up workspace '{workspace_name}' in Fabric..."
    )

    try:
        # Create FabricApiUtils without workspace_id to avoid circular dependency
        fabric_api = FabricApiUtils(
            environment=environment,
            project_path=project_path,
            workspace_id="placeholder",  # We'll look it up, not read from config
        )

        # Look up workspace by name
        workspace_id = fabric_api.get_workspace_id_from_name(workspace_name)

        workspace_created = False
        if not workspace_id:
            if create_if_not_exists:
                ConsoleStyles.print_info(
                    console,
                    f"Workspace '{workspace_name}' not found. Creating new workspace...",
                )
                try:
                    workspace_id = fabric_api.create_workspace(workspace_name)
                    workspace_created = True
                except Exception as e:
                    if "already exists" in str(e):
                        ConsoleStyles.print_error(
                            console,
                            f"❌ Workspace '{workspace_name}' already exists but was not found in the list",
                        )
                        ConsoleStyles.print_info(
                            console,
                            "💡 This might be a permissions issue or the workspace might be in a different capacity",
                        )
                    ConsoleStyles.print_error(
                        console, f"❌ Failed to create workspace: {str(e)}"
                    )
                    raise typer.Exit(code=1)
            else:
                ConsoleStyles.print_error(
                    console,
                    f"❌ Workspace '{workspace_name}' not found in your Fabric environment.",
                )
                ConsoleStyles.print_info(
                    console,
                    "💡 Make sure you have access to the workspace and the name is spelled correctly.",
                )
                ConsoleStyles.print_info(
                    console,
                    f"💡 Or use --create-if-not-exists flag to create the workspace: ingen_fab init workspace --workspace-name '{workspace_name}' --create-if-not-exists",
                )
                raise typer.Exit(code=1)

        if workspace_created:
            ConsoleStyles.print_success(
                console,
                f"✓ Created workspace '{workspace_name}' with ID: {workspace_id}",
            )
        else:
            ConsoleStyles.print_success(
                console, f"✓ Found workspace '{workspace_name}' with ID: {workspace_id}"
            )

        # Q&A workflow for workspace configuration
        console.print("\n[cyan]Workspace Configuration[/cyan]")
        console.print("────────────────────────")

        is_single_workspace = os.environ.get("IS_SINGLE_WORKSPACE",'')

        if is_single_workspace == '':
            ConsoleStyles.print_info(
                console,
                "💡 Environment variable IS_SINGLE_WORKSPACE not set, prompting user.",
            )
            is_single_workspace = typer.confirm(
                "\nIs this a single workspace deployment?\n"
                "(All components - config, raw, edw - will be in the same workspace)",
                default=True,
            )

        if is_single_workspace:
            ConsoleStyles.print_info(
                console, "\n✓ Single workspace deployment selected"
            )
            ConsoleStyles.print_info(
                console, f"  All workspace IDs will be set to: {workspace_id}"
            )

            # Update all workspace IDs in the valueSet
            _update_all_workspace_ids(project_path, environment, workspace_id, console)

            # Check for existing lakehouses and warehouses
            ConsoleStyles.print_info(
                console, "\nChecking for existing artifacts in workspace..."
            )
            _check_and_update_artifacts(
                fabric_api, project_path, environment, workspace_id, console
            )
        else:
            ConsoleStyles.print_info(console, "\n✓ Multi-workspace deployment selected")
            ConsoleStyles.print_warning(
                console, "⚠️  This is a complex deployment scenario."
            )
            ConsoleStyles.print_info(
                console,
                "You will need to manually configure the following workspace IDs in:",
            )
            ConsoleStyles.print_info(
                console,
                f"  {project_path}/fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/{environment}.json",
            )
            # Dynamically find all workspace ID variables to show to user
            workspace_id_vars = []
            valueset_path = (
                project_path
                / "fabric_workspace_items"
                / "config"
                / "var_lib.VariableLibrary"
                / "valueSets"
                / f"{environment}.json"
            )
            try:
                with open(valueset_path, "r", encoding="utf-8") as f:
                    valueset_data = json.load(f)
                if "variableOverrides" in valueset_data:
                    for override in valueset_data["variableOverrides"]:
                        var_name = override.get("name", "")
                        if var_name.endswith("_workspace_id"):
                            workspace_id_vars.append(var_name)
            except Exception:
                # Fall back to common workspace IDs if we can't read the file
                workspace_id_vars = [
                    "fabric_deployment_workspace_id",
                    "config_workspace_id",
                    "config_wh_workspace_id",
                    "raw_workspace_id",
                    "edw_workspace_id",
                ]

            ConsoleStyles.print_info(console, "\nWorkspace IDs to configure:")
            for var in sorted(workspace_id_vars):
                if var == "fabric_deployment_workspace_id":
                    ConsoleStyles.print_info(console, f"  - {var} (already set)")
                else:
                    ConsoleStyles.print_info(console, f"  - {var}")

            # Still update just the deployment workspace ID
            _update_valueset_workspace_id(
                project_path, environment, workspace_id, console
            )

        ConsoleStyles.print_success(
            console,
            f"\n✓ Successfully configured workspace '{workspace_name}' for environment '{environment}'",
        )

    except typer.Exit:
        raise
    except Exception as e:
        ConsoleStyles.print_error(
            console, f"❌ Failed to initialize workspace: {str(e)}"
        )
        raise typer.Exit(code=1)


def _update_valueset_workspace_id(
    project_path: Path, environment: str, workspace_id: str, console: Console
):
    """Update the fabric_deployment_workspace_id in the valueSet configuration."""
    valueset_path = (
        project_path
        / "fabric_workspace_items"
        / "config"
        / "var_lib.VariableLibrary"
        / "valueSets"
        / f"{environment}.json"
    )

    if not valueset_path.exists():
        ConsoleStyles.print_error(
            console, f"❌ ValueSet file not found: {valueset_path}"
        )
        ConsoleStyles.print_info(
            console,
            "💡 Make sure you're running this command from a valid Fabric project directory.",
        )
        raise typer.Exit(code=1)

    try:
        # Read current valueSet configuration
        with open(valueset_path, "r", encoding="utf-8") as f:
            valueset_data = json.load(f)

        # Update the fabric_deployment_workspace_id in variableOverrides
        if "variableOverrides" in valueset_data:
            variable_overrides = valueset_data["variableOverrides"]

            # Find the fabric_deployment_workspace_id override
            workspace_id_override = None
            for override in variable_overrides:
                if override.get("name") == "fabric_deployment_workspace_id":
                    workspace_id_override = override
                    break

            if workspace_id_override:
                old_value = workspace_id_override.get("value")
                workspace_id_override["value"] = workspace_id

                # Write updated configuration back
                with open(valueset_path, "w", encoding="utf-8") as f:
                    json.dump(valueset_data, f, indent=2, ensure_ascii=False)

                ConsoleStyles.print_info(
                    console,
                    f"✓ Updated fabric_deployment_workspace_id in {environment}.json",
                )
                if old_value != workspace_id:
                    ConsoleStyles.print_info(console, f"  Changed from: {old_value}")
                    ConsoleStyles.print_info(console, f"  Changed to: {workspace_id}")
            else:
                ConsoleStyles.print_error(
                    console,
                    "❌ fabric_deployment_workspace_id not found in variableOverrides",
                )
                raise typer.Exit(code=1)
        else:
            ConsoleStyles.print_error(
                console, "❌ variableOverrides not found in valueSet configuration"
            )
            raise typer.Exit(code=1)

    except json.JSONDecodeError as e:
        ConsoleStyles.print_error(
            console, f"❌ Invalid JSON in valueSet file: {str(e)}"
        )
        raise typer.Exit(code=1)
    except Exception as e:
        ConsoleStyles.print_error(
            console, f"❌ Failed to update valueSet configuration: {str(e)}"
        )
        raise typer.Exit(code=1)


def _update_all_workspace_ids(
    project_path: Path, environment: str, workspace_id: str, console: Console
):
    """Update all workspace IDs in the valueSet configuration for single workspace deployment."""
    valueset_path = (
        project_path
        / "fabric_workspace_items"
        / "config"
        / "var_lib.VariableLibrary"
        / "valueSets"
        / f"{environment}.json"
    )

    if not valueset_path.exists():
        ConsoleStyles.print_error(
            console, f"❌ ValueSet file not found: {valueset_path}"
        )
        ConsoleStyles.print_info(
            console,
            "💡 Make sure you're running this command from a valid Fabric project directory.",
        )
        raise typer.Exit(code=1)

    try:
        # Read current valueSet configuration
        with open(valueset_path, "r", encoding="utf-8") as f:
            valueset_data = json.load(f)

        # Update each workspace ID in variableOverrides
        if "variableOverrides" in valueset_data:
            variable_overrides = valueset_data["variableOverrides"]
            updated_vars = []

            # Dynamically find and update all variables ending with _workspace_id
            for override in variable_overrides:
                var_name = override.get("name", "")
                if var_name.endswith("_workspace_id"):
                    old_value = override.get("value")
                    override["value"] = workspace_id
                    updated_vars.append(var_name)

                    if old_value != workspace_id:
                        ConsoleStyles.print_info(
                            console,
                            f"  ✓ Updated {var_name}: {old_value} → {workspace_id}",
                        )
                    else:
                        ConsoleStyles.print_info(
                            console, f"  ✓ {var_name} already set to: {workspace_id}"
                        )

            # Report how many workspace ID variables were updated
            if updated_vars:
                ConsoleStyles.print_info(
                    console,
                    f"\n  Updated {len(updated_vars)} workspace ID variable(s):",
                )
                for var in sorted(updated_vars):
                    ConsoleStyles.print_info(console, f"    - {var}")
            else:
                ConsoleStyles.print_warning(
                    console,
                    "  ⚠️  No variables ending with '_workspace_id' were found",
                )

            # Write updated configuration back
            with open(valueset_path, "w", encoding="utf-8") as f:
                json.dump(valueset_data, f, indent=2, ensure_ascii=False)

            ConsoleStyles.print_success(
                console, f"\n✓ Updated all workspace IDs in {environment}.json"
            )
        else:
            ConsoleStyles.print_error(
                console, "❌ variableOverrides not found in valueSet configuration"
            )
            raise typer.Exit(code=1)

    except json.JSONDecodeError as e:
        ConsoleStyles.print_error(
            console, f"❌ Invalid JSON in valueSet file: {str(e)}"
        )
        raise typer.Exit(code=1)
    except Exception as e:
        ConsoleStyles.print_error(
            console, f"❌ Failed to update valueSet configuration: {str(e)}"
        )
        raise typer.Exit(code=1)


def _check_and_update_artifacts(
    fabric_api: FabricApiUtils,
    project_path: Path,
    environment: str,
    workspace_id: str,
    console: Console,
):
    """Check for existing lakehouses and warehouses in the workspace and update their IDs."""
    valueset_path = (
        project_path
        / "fabric_workspace_items"
        / "config"
        / "var_lib.VariableLibrary"
        / "valueSets"
        / f"{environment}.json"
    )

    try:
        # Read current valueSet configuration
        with open(valueset_path, "r", encoding="utf-8") as f:
            valueset_data = json.load(f)

        # Get workspace name from ID
        try:
            workspace_name = fabric_api.get_workspace_name_from_id(workspace_id)
        except Exception as e:
            ConsoleStyles.print_warning(
                console, f"  ⚠️  Could not get workspace name: {str(e)}"
            )
            workspace_name = None

        # Get existing lakehouses, warehouses, and notebooks in the workspace
        try:
            lakehouses = fabric_api.list_lakehouses(workspace_id)
            warehouses = fabric_api.list_warehouses(workspace_id)
            notebooks = fabric_api.list_notebooks(workspace_id)
        except Exception as e:
            ConsoleStyles.print_warning(
                console, f"  ⚠️  Could not list workspace artifacts: {str(e)}"
            )
            return

        # Create lookup dictionaries by name
        lakehouse_by_name = {lh["displayName"]: lh["id"] for lh in lakehouses}
        warehouse_by_name = {wh["displayName"]: wh["id"] for wh in warehouses}
        notebook_by_name = {nb["displayName"]: nb["id"] for nb in notebooks}

        ConsoleStyles.print_info(
            console,
            f"  Found {len(lakehouses)} lakehouses, {len(warehouses)} warehouses, and {len(notebooks)} notebooks",
        )

        # Track what needs to be deployed
        missing_artifacts = []
        updated_artifacts = []

        # Update artifact IDs in variableOverrides
        if "variableOverrides" in valueset_data:
            variable_overrides = valueset_data["variableOverrides"]

            # Create a map of variable names to their values for easy lookup
            var_map = {var["name"]: var for var in variable_overrides}

            # Update config_workspace_name if we have the workspace name and it exists in config
            if workspace_name and "config_workspace_name" in var_map:
                old_name = var_map["config_workspace_name"]["value"]
                var_map["config_workspace_name"]["value"] = workspace_name
                updated_artifacts.append("Workspace name (config_workspace_name)")
                if old_name != workspace_name:
                    ConsoleStyles.print_info(
                        console,
                        f"  ✓ Updated config_workspace_name: {old_name} → {workspace_name}",
                    )

            # Dynamically discover and update lakehouse IDs
            # Find all variables ending with _lakehouse_id and their corresponding _lakehouse_name
            lakehouse_id_vars = [
                var_name
                for var_name in var_map.keys()
                if var_name.endswith("_lakehouse_id")
            ]

            if lakehouse_id_vars:
                ConsoleStyles.print_info(
                    console,
                    f"\n  Checking {len(lakehouse_id_vars)} lakehouse variables dynamically:",
                )
                for var in sorted(lakehouse_id_vars):
                    ConsoleStyles.print_info(console, f"    - {var}")

            for id_var in lakehouse_id_vars:
                # Derive the corresponding name variable
                name_var = id_var.replace("_lakehouse_id", "_lakehouse_name")

                if name_var in var_map:
                    lakehouse_name = var_map[name_var]["value"]
                    # Skip empty lakehouse names (optional lakehouses)
                    if not lakehouse_name:
                        continue
                    if lakehouse_name in lakehouse_by_name:
                        # Update the ID
                        old_id = var_map[id_var]["value"]
                        new_id = lakehouse_by_name[lakehouse_name]
                        var_map[id_var]["value"] = new_id
                        updated_artifacts.append(
                            f"Lakehouse '{lakehouse_name}' ({id_var})"
                        )
                        if old_id != new_id:
                            ConsoleStyles.print_info(
                                console, f"  ✓ Updated {id_var}: {old_id} → {new_id}"
                            )
                    else:
                        missing_artifacts.append(f"Lakehouse '{lakehouse_name}'")
                else:
                    ConsoleStyles.print_warning(
                        console, f"  ⚠️  Found {id_var} but no corresponding {name_var}"
                    )

            # Dynamically discover and update warehouse IDs
            # Find all variables ending with _warehouse_id and their corresponding _warehouse_name
            warehouse_id_vars = [
                var_name
                for var_name in var_map.keys()
                if var_name.endswith("_warehouse_id")
            ]

            if warehouse_id_vars:
                ConsoleStyles.print_info(
                    console,
                    f"\n  Checking {len(warehouse_id_vars)} warehouse variables dynamically:",
                )
                for var in sorted(warehouse_id_vars):
                    ConsoleStyles.print_info(console, f"    - {var}")

            for id_var in warehouse_id_vars:
                # Derive the corresponding name variable
                name_var = id_var.replace("_warehouse_id", "_warehouse_name")

                if name_var in var_map:
                    warehouse_name = var_map[name_var]["value"]
                    # Skip empty warehouse names (optional warehouses)
                    if not warehouse_name:
                        continue
                    if warehouse_name in warehouse_by_name:
                        # Update the ID
                        old_id = var_map[id_var]["value"]
                        new_id = warehouse_by_name[warehouse_name]
                        var_map[id_var]["value"] = new_id
                        updated_artifacts.append(
                            f"Warehouse '{warehouse_name}' ({id_var})"
                        )
                        if old_id != new_id:
                            ConsoleStyles.print_info(
                                console, f"  ✓ Updated {id_var}: {old_id} → {new_id}"
                            )
                    else:
                        missing_artifacts.append(f"Warehouse '{warehouse_name}'")
                else:
                    ConsoleStyles.print_warning(
                        console, f"  ⚠️  Found {id_var} but no corresponding {name_var}"
                    )

            # Dynamically discover and update notebook IDs
            # Find all variables ending with _notebook_id and their corresponding _notebook_name
            notebook_id_vars = [
                var_name
                for var_name in var_map.keys()
                if var_name.endswith("_notebook_id")
            ]

            if notebook_id_vars:
                ConsoleStyles.print_info(
                    console,
                    f"\n  Checking {len(notebook_id_vars)} notebook variables dynamically:",
                )
                for var in sorted(notebook_id_vars):
                    ConsoleStyles.print_info(console, f"    - {var}")

            for id_var in notebook_id_vars:
                # Derive the corresponding name variable
                name_var = id_var.replace("_notebook_id", "_notebook_name")

                if name_var in var_map:
                    notebook_name = var_map[name_var]["value"]
                    # Skip empty notebook names (optional notebooks)
                    if not notebook_name:
                        continue
                    if notebook_name in notebook_by_name:
                        # Update the ID
                        old_id = var_map[id_var]["value"]
                        new_id = notebook_by_name[notebook_name]
                        var_map[id_var]["value"] = new_id
                        updated_artifacts.append(
                            f"Notebook '{notebook_name}' ({id_var})"
                        )
                        if old_id != new_id:
                            ConsoleStyles.print_info(
                                console, f"  ✓ Updated {id_var}: {old_id} → {new_id}"
                            )
                    else:
                        missing_artifacts.append(f"Notebook '{notebook_name}'")
                else:
                    ConsoleStyles.print_warning(
                        console, f"  ⚠️  Found {id_var} but no corresponding {name_var}"
                    )

            # Write updated configuration back if any changes were made
            if updated_artifacts:
                with open(valueset_path, "w", encoding="utf-8") as f:
                    json.dump(valueset_data, f, indent=2, ensure_ascii=False)
                ConsoleStyles.print_success(
                    console, f"\n✓ Updated {len(updated_artifacts)} artifact IDs"
                )

            # Report missing artifacts
            if missing_artifacts:
                ConsoleStyles.print_warning(
                    console,
                    "\n⚠️  The following artifacts were not found in the workspace:",
                )
                for artifact in missing_artifacts:
                    ConsoleStyles.print_warning(console, f"  - {artifact}")
                ConsoleStyles.print_info(
                    console, "\n💡 To create missing lakehouses and warehouses, run:"
                )
                ConsoleStyles.print_info(
                    console, f"   ingen_fab deploy deploy --environment {environment}"
                )
                ConsoleStyles.print_info(
                    console, "💡 Notebooks need to be created manually in Fabric or via DDL compilation"
                )
            else:
                ConsoleStyles.print_success(
                    console, "\n✓ All expected artifacts (lakehouses, warehouses, notebooks) found and configured!"
                )

    except Exception as e:
        ConsoleStyles.print_error(console, f"❌ Failed to check artifacts: {str(e)}")


def init_storage_config():
    """Initialize lakehouse and warehouse artifacts from storage_config.yaml.
    
    Reads the storage_config.yaml file and creates Fabric artifact folders
    under fabric_workspace_items/lakehouses and fabric_workspace_items/warehouses
    for each lakehouse and warehouse defined.
    """
    console = Console()
    
    # Get the fabric_workspace_repo_dir from environment
    fabric_workspace_repo_dir = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
    if not fabric_workspace_repo_dir:
        ConsoleStyles.print_error(
            console,
            "❌ FABRIC_WORKSPACE_REPO_DIR environment variable is not set.",
        )
        ConsoleStyles.print_info(
            console,
            "💡 Please ensure you're in a Fabric workspace project directory.",
        )
        raise typer.Exit(code=1)
    
    workspace_path = Path(fabric_workspace_repo_dir)
    storage_config_path = workspace_path / "fabric_config" / "storage_config.yaml"
    
    # Check if storage_config.yaml exists
    if not storage_config_path.exists():
        ConsoleStyles.print_error(
            console,
            f"❌ storage_config.yaml not found at {storage_config_path}",
        )
        raise typer.Exit(code=1)
    
    # Read storage_config.yaml
    try:
        with open(storage_config_path, "r", encoding="utf-8") as f:
            storage_config = yaml.safe_load(f)
    except Exception as e:
        ConsoleStyles.print_error(
            console,
            f"❌ Failed to read storage_config.yaml: {str(e)}",
        )
        raise typer.Exit(code=1)
    
    # Get template directories
    try:
        templates_base = PathUtils.get_package_resource_path(
            "project_templates/fabric_config"
        )
        lakehouse_template_dir = templates_base / "template.Lakehouse"
        warehouse_template_dir = templates_base / "template.Warehouse"
        sqldatabase_template_dir = templates_base / "template.SQLDatabase"
    except FileNotFoundError as e:
        ConsoleStyles.print_error(
            console,
            f"❌ Template directories not found: {e}",
        )
        raise typer.Exit(code=1)
    
    if not lakehouse_template_dir.exists():
        ConsoleStyles.print_error(
            console,
            f"❌ Lakehouse template not found: {lakehouse_template_dir}",
        )
        raise typer.Exit(code=1)
    
    # Extract lakehouse and warehouse names from the storage configuration
    lakehouse_names: list[str] = []
    warehouse_names: list[str] = []
    sqldatabase_names: list[str] = []
    
    # Values to skip
    skip_values = {"none", "DO_NOT_CREATE", ""}
    
    if "storage" in storage_config:
        for storage_item in storage_config["storage"]:
            # Process lakehouses
            if "lakehouse" in storage_item:
                for key, value in storage_item.items():
                    if key != "lakehouse" and value and value not in skip_values:
                        lakehouse_names.append(value)
            
            # Process warehouses
            if "warehouses" in storage_item:
                for key, value in storage_item.items():
                    if key != "warehouses" and value and value not in skip_values:
                        warehouse_names.append(value)
            
            # Process SQL databases
            if "sqldatabases" in storage_item:
                for key, value in storage_item.items():
                    if key != "sqldatabases" and value and value not in skip_values:
                        sqldatabase_names.append(value)
    
    if not lakehouse_names and not warehouse_names and not sqldatabase_names:
        ConsoleStyles.print_warning(
            console,
            "⚠️  No lakehouses, warehouses, or SQL databases found in storage_config.yaml",
        )
        raise typer.Exit(code=0)
    
    if lakehouse_names:
        ConsoleStyles.print_info(
            console,
            f"Found {len(lakehouse_names)} lakehouse(s): {', '.join(lakehouse_names)}",
        )
    
    if warehouse_names:
        ConsoleStyles.print_info(
            console,
            f"Found {len(warehouse_names)} warehouse(s): {', '.join(warehouse_names)}",
        )
    
    if sqldatabase_names:
        ConsoleStyles.print_info(
            console,
            f"Found {len(sqldatabase_names)} SQL database(s): {', '.join(sqldatabase_names)}",
        )
    
    # Process lakehouses
    lakehouse_created = 0
    lakehouse_skipped = 0
    
    if lakehouse_names:
        ConsoleStyles.print_info(console, "\n📦 Processing lakehouses...")
        lakehouses_dir = workspace_path / "fabric_workspace_items" / "lakehouses"
        lakehouses_dir.mkdir(parents=True, exist_ok=True)
        
        for lakehouse_name in lakehouse_names:
            result = _create_artifact_from_template(
                console=console,
                artifact_name=lakehouse_name,
                artifact_type="Lakehouse",
                template_dir=lakehouse_template_dir,
                output_dir=lakehouses_dir,
                name_placeholder="REPLACE_WITH_LAKEHOUSE_NAME",
            )
            if result:
                lakehouse_created += 1
            else:
                lakehouse_skipped += 1
    
    # Process warehouses
    warehouse_created = 0
    warehouse_skipped = 0
    
    if warehouse_names:
        if not warehouse_template_dir.exists():
            ConsoleStyles.print_warning(
                console,
                f"⚠️  Warehouse template not found: {warehouse_template_dir}",
            )
            ConsoleStyles.print_info(
                console,
                "   Skipping warehouse creation.",
            )
        else:
            ConsoleStyles.print_info(console, "\n🏢 Processing warehouses...")
            warehouses_dir = workspace_path / "fabric_workspace_items" / "warehouses"
            warehouses_dir.mkdir(parents=True, exist_ok=True)
            
            for warehouse_name in warehouse_names:
                result = _create_artifact_from_template(
                    console=console,
                    artifact_name=warehouse_name,
                    artifact_type="Warehouse",
                    template_dir=warehouse_template_dir,
                    output_dir=warehouses_dir,
                    name_placeholder="REPLACE_WITH_WAREHOUSE_NAME",
                )
                if result:
                    warehouse_created += 1
                else:
                    warehouse_skipped += 1
    
    # Process SQL databases
    sqldatabase_created = 0
    sqldatabase_skipped = 0
    
    if sqldatabase_names:
        if not sqldatabase_template_dir.exists():
            ConsoleStyles.print_warning(
                console,
                f"⚠️  SQL Database template not found: {sqldatabase_template_dir}",
            )
            ConsoleStyles.print_info(
                console,
                "   Skipping SQL database creation.",
            )
        else:
            ConsoleStyles.print_info(console, "\n🗄️  Processing SQL databases...")
            sqldatabases_dir = workspace_path / "fabric_workspace_items" / "SQLDatabases"
            sqldatabases_dir.mkdir(parents=True, exist_ok=True)
            
            for sqldatabase_name in sqldatabase_names:
                result = _create_artifact_from_template(
                    console=console,
                    artifact_name=sqldatabase_name,
                    artifact_type="SQLDatabase",
                    template_dir=sqldatabase_template_dir,
                    output_dir=sqldatabases_dir,
                    name_placeholder="REPLACE_WITH_SQLDATABASE_NAME",
                )
                if result:
                    sqldatabase_created += 1
                else:
                    sqldatabase_skipped += 1
    
    # Update variables.json with lakehouse and warehouse variable definitions
    ConsoleStyles.print_info(console, "\n📝 Updating variable library definitions...")
    updated_variables = _update_variables_json(
        console=console,
        workspace_path=workspace_path,
        lakehouse_names=lakehouse_names,
        warehouse_names=warehouse_names,
        sqldatabase_names=sqldatabase_names,
    )
    
    # Update valueSet JSON files with lakehouse and warehouse variables
    ConsoleStyles.print_info(console, "\n📝 Updating variable library valueSets...")
    updated_valuesets = _update_valueset_files(
        console=console,
        workspace_path=workspace_path,
        lakehouse_names=lakehouse_names,
        warehouse_names=warehouse_names,
        sqldatabase_names=sqldatabase_names,
    )
    
    # Print summary
    ConsoleStyles.print_info(console, "\n" + "="*60)
    
    total_created = lakehouse_created + warehouse_created + sqldatabase_created
    total_skipped = lakehouse_skipped + warehouse_skipped + sqldatabase_skipped
    
    if total_created > 0:
        ConsoleStyles.print_success(
            console,
            f"✓ Successfully created {total_created} artifact folder(s)",
        )
        if lakehouse_created > 0:
            ConsoleStyles.print_info(
                console,
                f"  • {lakehouse_created} lakehouse(s)",
            )
        if warehouse_created > 0:
            ConsoleStyles.print_info(
                console,
                f"  • {warehouse_created} warehouse(s)",
            )
        if sqldatabase_created > 0:
            ConsoleStyles.print_info(
                console,
                f"  • {sqldatabase_created} SQL database(s)",
            )
    
    if total_skipped > 0:
        ConsoleStyles.print_info(
            console,
            f"ℹ Skipped {total_skipped} existing artifact folder(s)",
        )
    
    if updated_variables:
        ConsoleStyles.print_success(
            console,
            "✓ Updated variables.json with new variable definitions",
        )
    
    if updated_valuesets > 0:
        ConsoleStyles.print_success(
            console,
            f"✓ Updated {updated_valuesets} valueSet file(s) with new variables",
        )
    
    ConsoleStyles.print_info(
        console,
        "\n💡 Next steps:",
    )
    ConsoleStyles.print_info(
        console,
        "   1. Review the generated artifacts in fabric_workspace_items/",
    )
    ConsoleStyles.print_info(
        console,
        "   2. Review the updated variables in valueSets/",
    )
    ConsoleStyles.print_info(
        console,
        "   3. Deploy to Fabric: ingen_fab deploy deploy",
    )


def _create_artifact_from_template(
    console: Console,
    artifact_name: str,
    artifact_type: str,
    template_dir: Path,
    output_dir: Path,
    name_placeholder: str,
) -> bool:
    """Create a Fabric artifact folder from a template.
    
    Args:
        console: Rich console for output
        artifact_name: Name of the artifact (e.g., "lh_bronze")
        artifact_type: Type of artifact (e.g., "Lakehouse", "Warehouse")
        template_dir: Path to the template directory
        output_dir: Path to the output directory
        name_placeholder: Placeholder string to replace in templates
        
    Returns:
        True if created, False if skipped
    """
    artifact_folder = output_dir / f"{artifact_name}.{artifact_type}"
    
    # Check if folder already exists
    if artifact_folder.exists():
        ConsoleStyles.print_warning(
            console,
            f"  ⚠️  Skipped {artifact_name}: folder already exists",
        )
        return False
    
    # Create the artifact folder
    artifact_folder.mkdir(parents=True, exist_ok=True)
    
    # Generate a unique logical ID for this artifact
    logical_id = str(uuid.uuid4())
    
    # Copy all template files and process them
    for template_file in template_dir.iterdir():
        if template_file.is_file():
            dest_file = artifact_folder / template_file.name
            
            # Read template content
            content = template_file.read_text(encoding="utf-8")
            
            # Replace placeholders
            content = content.replace(name_placeholder, artifact_name)
            content = content.replace(
                "00000000-0000-0000-0000-000000000000",
                logical_id,
            )
            
            # Write to destination
            dest_file.write_text(content, encoding="utf-8")
    
    ConsoleStyles.print_success(
        console,
        f"  ✓ Created {artifact_name}.{artifact_type} (ID: {logical_id[:8]}...)",
    )
    return True


def _update_variables_json(
    console: Console,
    workspace_path: Path,
    lakehouse_names: list[str],
    warehouse_names: list[str],
    sqldatabase_names: list[str],
) -> bool:
    """Update the variables.json file with lakehouse, warehouse, and SQL database variable definitions.
    
    Args:
        console: Rich console for output
        workspace_path: Path to the workspace directory
        lakehouse_names: List of lakehouse names to add
        warehouse_names: List of warehouse names to add
        sqldatabase_names: List of SQL database names to add
        
    Returns:
        True if updated, False otherwise
    """
    variables_file = (
        workspace_path
        / "fabric_workspace_items"
        / "config"
        / "var_lib.VariableLibrary"
        / "variables.json"
    )
    
    if not variables_file.exists():
        ConsoleStyles.print_warning(
            console,
            f"  ⚠️  variables.json not found: {variables_file}",
        )
        return False
    
    try:
        # Read the variables.json file
        with open(variables_file, "r", encoding="utf-8") as f:
            variables_data = json.load(f)
        
        if "variables" not in variables_data:
            ConsoleStyles.print_warning(
                console,
                "  ⚠️  No 'variables' key found in variables.json",
            )
            return False
        
        variables = variables_data["variables"]
        
        # Get existing variable names to avoid duplicates
        existing_vars = {var["name"] for var in variables}
        
        # Track if we added any variables
        added_vars = []
        
        # Add lakehouse variable definitions
        for lakehouse_name in lakehouse_names:
            vars_to_add = [
                {
                    "name": f"{lakehouse_name}_workspace_id",
                    "note": f"GUID of the workspace containing {lakehouse_name} lakehouse.",
                    "type": "String",
                    "value": "",
                },
                {
                    "name": f"{lakehouse_name}_lakehouse_name",
                    "note": f"Name of the {lakehouse_name} lakehouse.",
                    "type": "String",
                    "value": "",
                },
                {
                    "name": f"{lakehouse_name}_lakehouse_id",
                    "note": f"GUID of the {lakehouse_name} lakehouse.",
                    "type": "String",
                    "value": "",
                },
            ]
            
            for var in vars_to_add:
                if var["name"] not in existing_vars:
                    variables.append(var)
                    added_vars.append(var["name"])
                    existing_vars.add(var["name"])
        
        # Add warehouse variable definitions
        for warehouse_name in warehouse_names:
            vars_to_add = [
                {
                    "name": f"{warehouse_name}_workspace_id",
                    "note": f"GUID of the workspace containing {warehouse_name} warehouse.",
                    "type": "String",
                    "value": "",
                },
                {
                    "name": f"{warehouse_name}_warehouse_name",
                    "note": f"Name of the {warehouse_name} warehouse.",
                    "type": "String",
                    "value": "",
                },
                {
                    "name": f"{warehouse_name}_warehouse_id",
                    "note": f"GUID of the {warehouse_name} warehouse.",
                    "type": "String",
                    "value": "",
                },
            ]
            
            for var in vars_to_add:
                if var["name"] not in existing_vars:
                    variables.append(var)
                    added_vars.append(var["name"])
                    existing_vars.add(var["name"])
        
        # Add SQL database variable definitions
        for sqldatabase_name in sqldatabase_names:
            vars_to_add = [
                {
                    "name": f"{sqldatabase_name}_workspace_id",
                    "note": f"GUID of the workspace containing {sqldatabase_name} SQL database.",
                    "type": "String",
                    "value": "",
                },
                {
                    "name": f"{sqldatabase_name}_sqldatabase_name",
                    "note": f"Name of the {sqldatabase_name} SQL database.",
                    "type": "String",
                    "value": "",
                },
                {
                    "name": f"{sqldatabase_name}_sqldatabase_id",
                    "note": f"GUID of the {sqldatabase_name} SQL database.",
                    "type": "String",
                    "value": "",
                },
            ]
            
            for var in vars_to_add:
                if var["name"] not in existing_vars:
                    variables.append(var)
                    added_vars.append(var["name"])
                    existing_vars.add(var["name"])
        
        # Only write if we added variables
        if added_vars:
            with open(variables_file, "w", encoding="utf-8") as f:
                json.dump(variables_data, f, indent=2, ensure_ascii=False)
            
            ConsoleStyles.print_success(
                console,
                f"  ✓ Added {len(added_vars)} variable definition(s) to variables.json",
            )
            return True
        else:
            ConsoleStyles.print_info(
                console,
                "  • variables.json already up to date",
            )
            return False
    
    except json.JSONDecodeError as e:
        ConsoleStyles.print_error(
            console,
            f"  ❌ Failed to parse variables.json: {str(e)}",
        )
        return False
    except Exception as e:
        ConsoleStyles.print_error(
            console,
            f"  ❌ Failed to update variables.json: {str(e)}",
        )
        return False


def _update_valueset_files(
    console: Console,
    workspace_path: Path,
    lakehouse_names: list[str],
    warehouse_names: list[str],
    sqldatabase_names: list[str],
) -> int:
    """Update all valueSet JSON files with lakehouse, warehouse, and SQL database variables.
    
    Args:
        console: Rich console for output
        workspace_path: Path to the workspace directory
        lakehouse_names: List of lakehouse names to add
        warehouse_names: List of warehouse names to add
        sqldatabase_names: List of SQL database names to add
        
    Returns:
        Number of valueSet files updated
    """
    valueset_dir = (
        workspace_path
        / "fabric_workspace_items"
        / "config"
        / "var_lib.VariableLibrary"
        / "valueSets"
    )
    
    if not valueset_dir.exists():
        ConsoleStyles.print_warning(
            console,
            f"  ⚠️  ValueSet directory not found: {valueset_dir}",
        )
        return 0
    
    # Find all .json files in the valueSet directory
    valueset_files = list(valueset_dir.glob("*.json"))
    
    if not valueset_files:
        ConsoleStyles.print_warning(
            console,
            "  ⚠️  No valueSet JSON files found",
        )
        return 0
    
    updated_count = 0
    
    for valueset_file in valueset_files:
        try:
            # Read the valueSet JSON file
            with open(valueset_file, "r", encoding="utf-8") as f:
                valueset_data = json.load(f)
            
            if "variableOverrides" not in valueset_data:
                ConsoleStyles.print_warning(
                    console,
                    f"  ⚠️  Skipped {valueset_file.name}: no variableOverrides",
                )
                continue
            
            variable_overrides = valueset_data["variableOverrides"]
            
            # Get existing variable names to avoid duplicates
            existing_vars = {var["name"] for var in variable_overrides}
            
            # Track if we added any variables
            added_vars = []
            
            # Add lakehouse variables
            for lakehouse_name in lakehouse_names:
                vars_to_add = [
                    {
                        "name": f"{lakehouse_name}_workspace_id",
                        "value": f"REPLACE_WITH_{lakehouse_name.upper()}_WORKSPACE_GUID",
                    },
                    {
                        "name": f"{lakehouse_name}_lakehouse_name",
                        "value": lakehouse_name,
                    },
                    {
                        "name": f"{lakehouse_name}_lakehouse_id",
                        "value": f"REPLACE_WITH_{lakehouse_name.upper()}_LAKEHOUSE_GUID",
                    },
                ]
                
                for var in vars_to_add:
                    if var["name"] not in existing_vars:
                        variable_overrides.append(var)
                        added_vars.append(var["name"])
                        existing_vars.add(var["name"])
            
            # Add warehouse variables
            for warehouse_name in warehouse_names:
                vars_to_add = [
                    {
                        "name": f"{warehouse_name}_workspace_id",
                        "value": f"REPLACE_WITH_{warehouse_name.upper()}_WORKSPACE_GUID",
                    },
                    {
                        "name": f"{warehouse_name}_warehouse_name",
                        "value": warehouse_name,
                    },
                    {
                        "name": f"{warehouse_name}_warehouse_id",
                        "value": f"REPLACE_WITH_{warehouse_name.upper()}_WAREHOUSE_GUID",
                    },
                ]
                
                for var in vars_to_add:
                    if var["name"] not in existing_vars:
                        variable_overrides.append(var)
                        added_vars.append(var["name"])
                        existing_vars.add(var["name"])
            
            # Add SQL database variables
            for sqldatabase_name in sqldatabase_names:
                vars_to_add = [
                    {
                        "name": f"{sqldatabase_name}_workspace_id",
                        "value": f"REPLACE_WITH_{sqldatabase_name.upper()}_WORKSPACE_GUID",
                    },
                    {
                        "name": f"{sqldatabase_name}_sqldatabase_name",
                        "value": sqldatabase_name,
                    },
                    {
                        "name": f"{sqldatabase_name}_sqldatabase_id",
                        "value": f"REPLACE_WITH_{sqldatabase_name.upper()}_SQLDATABASE_GUID",
                    },
                ]
                
                for var in vars_to_add:
                    if var["name"] not in existing_vars:
                        variable_overrides.append(var)
                        added_vars.append(var["name"])
                        existing_vars.add(var["name"])
            
            # Only write if we added variables
            if added_vars:
                with open(valueset_file, "w", encoding="utf-8") as f:
                    json.dump(valueset_data, f, indent=2, ensure_ascii=False)
                
                ConsoleStyles.print_success(
                    console,
                    f"  ✓ Updated {valueset_file.name} (added {len(added_vars)} variables)",
                )
                updated_count += 1
            else:
                ConsoleStyles.print_info(
                    console,
                    f"  • {valueset_file.name} already up to date",
                )
        
        except json.JSONDecodeError as e:
            ConsoleStyles.print_error(
                console,
                f"  ❌ Failed to parse {valueset_file.name}: {str(e)}",
            )
        except Exception as e:
            ConsoleStyles.print_error(
                console,
                f"  ❌ Failed to update {valueset_file.name}: {str(e)}",
            )
    
    return updated_count

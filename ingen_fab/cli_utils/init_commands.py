import json
import os
import shutil
import uuid
from pathlib import Path

import typer
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.fabric_api.utils import FabricApiUtils
from ingen_fab.python_libs.common.utils.path_utils import PathUtils


def init_solution(project_name: str | None, path: Path):
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

        # Get existing lakehouses and warehouses in the workspace
        try:
            lakehouses = fabric_api.list_lakehouses(workspace_id)
            warehouses = fabric_api.list_warehouses(workspace_id)
        except Exception as e:
            ConsoleStyles.print_warning(
                console, f"  ⚠️  Could not list workspace artifacts: {str(e)}"
            )
            return

        # Create lookup dictionaries by name
        lakehouse_by_name = {lh["displayName"]: lh["id"] for lh in lakehouses}
        warehouse_by_name = {wh["displayName"]: wh["id"] for wh in warehouses}

        ConsoleStyles.print_info(
            console,
            f"  Found {len(lakehouses)} lakehouses and {len(warehouses)} warehouses",
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
                    console, "\n💡 To create these artifacts, run:"
                )
                ConsoleStyles.print_info(
                    console, f"   ingen_fab deploy deploy --environment {environment}"
                )
            else:
                ConsoleStyles.print_success(
                    console, "\n✓ All expected artifacts found and configured!"
                )

    except Exception as e:
        ConsoleStyles.print_error(console, f"❌ Failed to check artifacts: {str(e)}")

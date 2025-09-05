"""DBT Profile Manager for ingen_fab.

This module manages dbt profile files, ensuring they exist and contain
the correct configuration based on the current FABRIC_ENVIRONMENT.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

console = Console()


class DBTProfileManager:
    """Manages dbt profile configuration for Fabric Spark notebooks."""

    def __init__(self, workspace_dir: Path, environment: str, dbt_project_dir: Optional[str] = None):
        """Initialize the DBT Profile Manager.

        Args:
            workspace_dir: Path to the fabric workspace repository
            environment: Current FABRIC_ENVIRONMENT value
            dbt_project_dir: Optional relative path to dbt project directory within workspace
        """
        self.workspace_dir = workspace_dir
        self.environment = environment
        self.dbt_project_dir = dbt_project_dir
        self.profile_path = Path.home() / ".dbt" / "profiles.yml"
        self.var_lib_path = (
            workspace_dir
            / "fabric_workspace_items"
            / "config"
            / "var_lib.VariableLibrary"
            / "valueSets"
        )
        self._profile_name = None  # Cache for profile name

    def get_workspace_config(self) -> Dict[str, Any]:
        """Read workspace configuration from variable library.

        Returns:
            Dictionary containing workspace configuration values
        """
        env_config_file = self.var_lib_path / f"{self.environment}.json"

        if not env_config_file.exists():
            raise FileNotFoundError(
                f"Environment configuration not found: {env_config_file}\n"
                f"Available environments: {', '.join([f.stem for f in self.var_lib_path.glob('*.json')])}"
            )

        with env_config_file.open("r", encoding="utf-8") as f:
            config = json.load(f)

        # Convert to a dictionary for easier access
        values = {}
        for override in config.get("variableOverrides", []):
            values[override["name"]] = override["value"]

        return values

    def find_dbt_projects(self) -> list[Path]:
        """Find all dbt project directories in the workspace.

        Returns:
            List of paths to directories containing dbt_project.yml files
        """
        dbt_projects = []
        
        # Search for all dbt_project.yml files
        for dbt_config in self.workspace_dir.rglob("dbt_project.yml"):
            # Exclude any in build or site-packages directories
            if "build" not in dbt_config.parts and "site-packages" not in dbt_config.parts:
                dbt_projects.append(dbt_config.parent)
        
        return sorted(dbt_projects)

    def get_profile_name(self) -> str:
        """Get the dbt profile name from dbt_project.yml or use default.

        Returns:
            The profile name to use for this dbt project
        """
        # Return cached value if available
        if self._profile_name:
            return self._profile_name
        
        # Default profile name for backward compatibility
        default_profile = "fabric-spark-testnb"
        
        # Try to find dbt_project.yml
        dbt_project_path = None
        
        if self.dbt_project_dir:
            # Use specified dbt project directory
            potential_path = self.workspace_dir / self.dbt_project_dir / "dbt_project.yml"
            if potential_path.exists():
                dbt_project_path = potential_path
            else:
                console.print(
                    f"[yellow]Warning: dbt_project.yml not found at {potential_path}[/yellow]"
                )
        else:
            # Auto-detect dbt projects
            dbt_projects = self.find_dbt_projects()
            
            if len(dbt_projects) == 1:
                # Use the only dbt project found
                dbt_project_path = dbt_projects[0] / "dbt_project.yml"
                console.print(
                    f"[cyan]Auto-detected dbt project at: {dbt_projects[0].relative_to(self.workspace_dir)}[/cyan]"
                )
            elif len(dbt_projects) > 1:
                # Multiple projects found, use the first one but warn
                console.print(
                    f"[yellow]Multiple dbt projects found. Using: {dbt_projects[0].relative_to(self.workspace_dir)}[/yellow]"
                )
                console.print("[yellow]Consider specifying --dbt-project-dir to select a specific project.[/yellow]")
                dbt_project_path = dbt_projects[0] / "dbt_project.yml"
        
        # Read profile name from dbt_project.yml if found
        if dbt_project_path and dbt_project_path.exists():
            try:
                with dbt_project_path.open("r", encoding="utf-8") as f:
                    dbt_config = yaml.safe_load(f)
                    profile_name = dbt_config.get("profile")
                    
                    if profile_name:
                        console.print(
                            f"[green]Using dbt profile name from {dbt_project_path.relative_to(self.workspace_dir)}: '{profile_name}'[/green]"
                        )
                        self._profile_name = profile_name
                        return profile_name
            except Exception as e:
                console.print(
                    f"[yellow]Warning: Could not read dbt_project.yml: {e}[/yellow]"
                )
        
        # Use default if no profile name found
        console.print(
            f"[cyan]Using default dbt profile name: '{default_profile}'[/cyan]"
        )
        self._profile_name = default_profile
        return default_profile

    def get_available_lakehouses(
        self, values: Dict[str, Any]
    ) -> Dict[str, Dict[str, str]]:
        """Extract all available lakehouse configurations from the values.

        Args:
            values: Dictionary of configuration values

        Returns:
            Dictionary with lakehouse identifiers as keys and their details as values
        """
        lakehouses = {}

        # Find all lakehouse-related variables
        # Pattern: {prefix}_lakehouse_id, {prefix}_lakehouse_name, {prefix}_workspace_id, {prefix}_workspace_name
        lakehouse_prefixes = set()

        for key in values.keys():
            if "_lakehouse_id" in key:
                prefix = key.replace("_lakehouse_id", "")
                lakehouse_prefixes.add(prefix)

        for prefix in lakehouse_prefixes:
            lakehouse_id = values.get(f"{prefix}_lakehouse_id", "")
            lakehouse_name = values.get(f"{prefix}_lakehouse_name", prefix)
            workspace_id = values.get(f"{prefix}_workspace_id", "")

            # Try to find workspace name - it might be under different patterns
            workspace_name = (
                values.get(f"{prefix}_workspace_name", "")
                or values.get("config_workspace_name", "")
                or values.get("workspace_name", "")
            )

            # Skip if essential values are missing or are placeholders
            if (
                lakehouse_id
                and workspace_id
                and "REPLACE_WITH" not in lakehouse_id
                and "REPLACE_WITH" not in workspace_id
            ):
                lakehouses[prefix] = {
                    "lakehouse_id": lakehouse_id,
                    "lakehouse_name": lakehouse_name,
                    "workspace_id": workspace_id,
                    "workspace_name": workspace_name,
                    "prefix": prefix,
                }

        return lakehouses

    def prompt_for_lakehouse_selection(
        self, lakehouses: Dict[str, Dict[str, str]]
    ) -> Dict[str, str]:
        """Prompt the user to select a lakehouse from available options.

        Args:
            lakehouses: Dictionary of available lakehouse configurations

        Returns:
            Selected lakehouse configuration
        """
        if not lakehouses:
            raise ValueError(
                "No valid lakehouse configurations found in the environment file."
            )

        # If only one lakehouse is available, use it automatically
        if len(lakehouses) == 1:
            selected = list(lakehouses.values())[0]
            console.print(
                f"[green]Using the only available lakehouse: {selected['lakehouse_name']}[/green]"
            )
            return selected

        # Display available lakehouses
        console.print("\n[bold]Available Lakehouse Configurations:[/bold]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("#", style="cyan", width=3)
        table.add_column("Prefix", style="cyan")
        table.add_column("Lakehouse Name", style="green")
        table.add_column("Workspace Name", style="yellow")
        table.add_column("Lakehouse ID", style="dim")

        options = list(lakehouses.values())
        for idx, config in enumerate(options, 1):
            table.add_row(
                str(idx),
                config["prefix"],
                config["lakehouse_name"],
                config["workspace_name"] or "N/A",
                config["lakehouse_id"][:8] + "..."
                if len(config["lakehouse_id"]) > 8
                else config["lakehouse_id"],
            )

        console.print(table)

        # Prompt for selection
        while True:
            choice = Prompt.ask(
                "\nSelect a lakehouse configuration by number",
                default="1",
                choices=[str(i) for i in range(1, len(options) + 1)],
            )

            try:
                idx = int(choice) - 1
                if 0 <= idx < len(options):
                    selected = options[idx]
                    console.print(
                        f"\n[green]Selected: {selected['lakehouse_name']}[/green]"
                    )
                    return selected
            except (ValueError, IndexError):
                pass

            console.print("[red]Invalid selection. Please try again.[/red]")

    def get_saved_lakehouse_preference(
        self, existing_config: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Get the saved lakehouse preference from existing profile.

        Args:
            existing_config: Existing profile configuration

        Returns:
            The lakehouse prefix if saved, None otherwise
        """
        profile_name = self.get_profile_name()
        if not existing_config or profile_name not in existing_config:
            return None

        fabric_config = existing_config[profile_name]
        outputs = fabric_config.get("outputs", {})
        target_config = outputs.get("my_project_target", {})

        # Get the lakehouse name from the standard lakehouse field
        saved_lakehouse_name = target_config.get("lakehouse", None)
        if not saved_lakehouse_name:
            return None
            
        # Find which prefix corresponds to this lakehouse name in current environment
        try:
            values = self.get_workspace_config()
            available_lakehouses = self.get_available_lakehouses(values)
            
            for prefix, lakehouse_config in available_lakehouses.items():
                if lakehouse_config["lakehouse_name"] == saved_lakehouse_name:
                    return prefix
        except Exception:
            # If we can't read workspace config, return None
            pass
            
        return None

    def generate_profile_config(
        self,
        ask_for_selection: bool = True,
        existing_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Generate the dbt profile configuration based on current environment.

        Args:
            ask_for_selection: Whether to prompt user for lakehouse selection
            existing_config: Existing profile configuration to check for saved preferences

        Returns:
            Dictionary containing the dbt profile configuration
        """
        values = self.get_workspace_config()

        # Get available lakehouses
        available_lakehouses = self.get_available_lakehouses(values)
        selected_prefix = None
        selected_lakehouse = None

        if available_lakehouses:
            # Always check for saved preference first
            saved_prefix = self.get_saved_lakehouse_preference(existing_config)

            if saved_prefix and saved_prefix in available_lakehouses:
                # Use saved preference if it's still valid
                console.print(
                    f"[cyan]Using previously selected lakehouse: {available_lakehouses[saved_prefix]['lakehouse_name']}[/cyan]"
                )
                selected_lakehouse = available_lakehouses[saved_prefix]
                selected_prefix = saved_prefix
            elif ask_for_selection:
                # Only prompt if ask_for_selection is True and no valid saved preference
                selected_lakehouse = self.prompt_for_lakehouse_selection(
                    available_lakehouses
                )
                selected_prefix = selected_lakehouse["prefix"]
            else:
                # No saved preference and not asking for selection
                # Use the first available lakehouse as a fallback
                if available_lakehouses:
                    first_lakehouse = list(available_lakehouses.values())[0]
                    console.print(
                        f"[yellow]Using first available lakehouse: {first_lakehouse['lakehouse_name']}[/yellow]"
                    )
                    selected_lakehouse = first_lakehouse
                    selected_prefix = first_lakehouse["prefix"]

        if selected_lakehouse:
            workspace_id = selected_lakehouse["workspace_id"]
            workspace_name = selected_lakehouse["workspace_name"]
            lakehouse_id = selected_lakehouse["lakehouse_id"]
            lakehouse_name = selected_lakehouse["lakehouse_name"]
        else:
            selected_lakehouse = self.prompt_for_lakehouse_selection(
                    available_lakehouses
                )
            selected_prefix = selected_lakehouse["prefix"]
            
            if selected_lakehouse:
                workspace_id = selected_lakehouse["workspace_id"]
                workspace_name = selected_lakehouse["workspace_name"]
                lakehouse_id = selected_lakehouse["lakehouse_id"]
                lakehouse_name = selected_lakehouse["lakehouse_name"]

                # Check for placeholder values
                if "REPLACE_WITH" in workspace_id or "REPLACE_WITH" in lakehouse_id:
                    console.print(
                        f"[yellow]Warning: Environment '{self.environment}' contains placeholder values.[/yellow]"
                    )
                    console.print("Please update the following file with actual values:")
                    console.print(f"  {self.var_lib_path / f'{self.environment}.json'}")

        profile_name = self.get_profile_name()
        profile_config = {
            profile_name: {
                "outputs": {
                    "my_project_target": {
                        "authentication": "CLI",
                        "connect_retries": 0,
                        "connect_timeout": 10,
                        "endpoint": "https://api.fabric.microsoft.com/v1",
                        "lakehouse": lakehouse_name,
                        "lakehousedatapath": "/lakehouse",
                        "lakehouseid": lakehouse_id,
                        "log_lakehouse": "Lakehouse",
                        "method": "livy",
                        "retry_all": True,
                        "schema": "dbo",
                        "threads": 1,
                        "type": "fabricsparknb",
                        "workspaceid": workspace_id,
                        "workspacename": workspace_name,
                    }
                },
                "target": "my_project_target",
            }
        }

        return profile_config

    def read_existing_profile(self) -> Optional[Dict[str, Any]]:
        """Read existing dbt profile if it exists.

        Returns:
            Existing profile configuration or None if not found
        """
        if not self.profile_path.exists():
            return None

        try:
            with self.profile_path.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception as e:
            console.print(
                f"[yellow]Warning: Could not read existing profile: {e}[/yellow]"
            )
            return None

    def write_profile(self, config: Dict[str, Any]) -> None:
        """Write the dbt profile configuration to file.

        Args:
            config: Profile configuration to write
        """
        # Ensure the .dbt directory exists
        self.profile_path.parent.mkdir(parents=True, exist_ok=True)

        with self.profile_path.open("w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    def check_and_update_profile(self, ask_confirmation: bool = True) -> bool:
        """Check and update the dbt profile configuration.

        Args:
            ask_confirmation: Whether to ask for user confirmation before making changes

        Returns:
            True if profile was updated or already correct, False if user declined
        """
        existing_config = self.read_existing_profile()
        new_config = self.generate_profile_config(
            ask_for_selection=ask_confirmation, existing_config=existing_config
        )

        if existing_config is None:
            # No profile exists, create it
            console.print(
                Panel.fit(
                    f"[yellow]No dbt profile found at {self.profile_path}[/yellow]\n"
                    f"Creating new profile for environment: [bold]{self.environment}[/bold]",
                    title="DBT Profile Setup",
                    border_style="yellow",
                )
            )

            if ask_confirmation:
                console.print("\n[bold]Profile to be created:[/bold]")
                console.print(yaml.dump(new_config, default_flow_style=False))

                if not Confirm.ask("Create this dbt profile?", default=True):
                    console.print("[red]Profile creation cancelled.[/red]")
                    return False

            self.write_profile(new_config)
            console.print(
                f"[green]✓ Created dbt profile at {self.profile_path}[/green]"
            )
            return True

        # Get the profile name
        profile_name = self.get_profile_name()
        
        # Check if profile exists and matches
        if profile_name in existing_config:
            existing_fabric = existing_config[profile_name]
            new_fabric = new_config[profile_name]

            # Check if configuration matches
            if existing_fabric == new_fabric:
                console.print(
                    f"[green]✓ DBT profile already configured correctly for environment: {self.environment}[/green]"
                )
                return True

            # Configuration differs, needs update
            console.print(
                Panel.fit(
                    f"[yellow]DBT profile exists but needs updating for environment: {self.environment}[/yellow]",
                    title="DBT Profile Update",
                    border_style="yellow",
                )
            )

            if ask_confirmation:
                console.print(
                    f"\n[bold]Current {profile_name} configuration:[/bold]"
                )
                console.print(
                    yaml.dump(
                        {profile_name: existing_fabric},
                        default_flow_style=False,
                    )
                )
                console.print("\n[bold]New configuration:[/bold]")
                console.print(
                    yaml.dump(
                        {profile_name: new_fabric}, default_flow_style=False
                    )
                )

                if not Confirm.ask(
                    f"Update the {profile_name} configuration?", default=True
                ):
                    console.print("[red]Profile update cancelled.[/red]")
                    return False

            # Update the configuration
            existing_config[profile_name] = new_fabric
            self.write_profile(existing_config)
            console.print(
                f"[green]✓ Updated dbt profile at {self.profile_path}[/green]"
            )
            return True

        else:
            # Profile doesn't exist, add it
            console.print(
                Panel.fit(
                    f"[yellow]Adding {profile_name} configuration to existing dbt profile[/yellow]\n"
                    f"Environment: [bold]{self.environment}[/bold]",
                    title="DBT Profile Update",
                    border_style="yellow",
                )
            )

            if ask_confirmation:
                console.print("\n[bold]Configuration to be added:[/bold]")
                console.print(yaml.dump(new_config, default_flow_style=False))

                if not Confirm.ask(
                    "Add this configuration to your dbt profile?", default=True
                ):
                    console.print("[red]Profile update cancelled.[/red]")
                    return False

            # Add the new configuration
            existing_config.update(new_config)
            self.write_profile(existing_config)
            console.print(
                f"[green]✓ Added {profile_name} to dbt profile at {self.profile_path}[/green]"
            )
            return True


def ensure_dbt_profile(ctx: typer.Context, ask_confirmation: bool = True, dbt_project_dir: Optional[str] = None) -> bool:
    """Ensure dbt profile exists and is configured correctly.

    This function should be called before any dbt command execution.

    Args:
        ctx: Typer context containing workspace configuration
        ask_confirmation: Whether to ask for user confirmation
        dbt_project_dir: Optional relative path to dbt project directory

    Returns:
        True if profile is ready, False if user declined or error occurred
    """
    workspace_dir = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    if not workspace_dir:
        console.print("[red]Fabric workspace repo dir not provided.[/red]")
        return False

    workspace_dir = Path(workspace_dir)
    environment = os.getenv("FABRIC_ENVIRONMENT", "local")

    try:
        manager = DBTProfileManager(workspace_dir, environment, dbt_project_dir)
        return manager.check_and_update_profile(ask_confirmation)
    except FileNotFoundError as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]Error managing dbt profile: {e}[/red]")
        return False


def ensure_dbt_profile_for_exec(ctx: typer.Context, dbt_project_dir: Optional[str] = None) -> bool:
    """Ensure dbt profile exists for exec command with special behavior.

    This function is specifically for the 'dbt exec' command and:
    1. Always prompts for lakehouse selection if saved info is missing/invalid
    2. Notifies user of chosen lakehouse when using saved preference
    3. Never fails silently - always ensures user knows what's happening

    Args:
        ctx: Typer context containing workspace configuration
        dbt_project_dir: Optional relative path to dbt project directory

    Returns:
        True if profile is ready, False if error occurred
    """
    workspace_dir = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    if not workspace_dir:
        console.print("[red]Fabric workspace repo dir not provided.[/red]")
        return False

    workspace_dir = Path(workspace_dir)
    environment = os.getenv("FABRIC_ENVIRONMENT", "local")

    try:
        manager = DBTProfileManager(workspace_dir, environment, dbt_project_dir)

        # Check if we have valid saved configuration
        existing_config = manager.read_existing_profile()
        values = manager.get_workspace_config()
        available_lakehouses = manager.get_available_lakehouses(values)

        # Check if saved preference is still valid
        saved_prefix = manager.get_saved_lakehouse_preference(existing_config)
        has_valid_saved_preference = (
            saved_prefix
            and saved_prefix in available_lakehouses
            and existing_config
            and manager.get_profile_name() in existing_config
        )

        if has_valid_saved_preference and saved_prefix:
            # Notify user of the chosen lakehouse
            selected_lakehouse = available_lakehouses[saved_prefix]
            console.print(
                f"[cyan]Using saved lakehouse preference: "
                f"{selected_lakehouse['lakehouse_name']} "
                f"(Environment: {environment})[/cyan]"
            )
            # Still need to check if profile needs updating
            return manager.check_and_update_profile(ask_confirmation=False)
        else:
            # No valid saved preference - always prompt interactively
            console.print(
                f"[yellow]No valid lakehouse preference found for environment '{environment}'. "
                f"Please select a lakehouse:[/yellow]"
            )
            return manager.check_and_update_profile(ask_confirmation=True)

    except FileNotFoundError as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]Error managing dbt profile: {e}[/red]")
        return False

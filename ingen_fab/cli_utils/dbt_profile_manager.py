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
from rich.prompt import Confirm

console = Console()


class DBTProfileManager:
    """Manages dbt profile configuration for Fabric Spark notebooks."""

    def __init__(self, workspace_dir: Path, environment: str):
        """Initialize the DBT Profile Manager.

        Args:
            workspace_dir: Path to the fabric workspace repository
            environment: Current FABRIC_ENVIRONMENT value
        """
        self.workspace_dir = workspace_dir
        self.environment = environment
        self.profile_path = Path.home() / ".dbt" / "profiles.yml"
        self.var_lib_path = (
            workspace_dir
            / "fabric_workspace_items"
            / "config"
            / "var_lib.VariableLibrary"
            / "valueSets"
        )

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

    def generate_profile_config(self) -> Dict[str, Any]:
        """Generate the dbt profile configuration based on current environment.

        Returns:
            Dictionary containing the dbt profile configuration
        """
        values = self.get_workspace_config()

        # Get the sample_lh values as they're most commonly used for dbt
        workspace_id = values.get("sample_lh_workspace_id", "")
        workspace_name = values.get(
            "config_workspace_name", ""
        )  # Use workspace name from config
        lakehouse_id = values.get("sample_lh_lakehouse_id", "")
        lakehouse_name = values.get("sample_lh_lakehouse_name", "sample_lh")

        # Check for placeholder values
        if "REPLACE_WITH" in workspace_id or "REPLACE_WITH" in lakehouse_id:
            console.print(
                f"[yellow]Warning: Environment '{self.environment}' contains placeholder values.[/yellow]"
            )
            console.print("Please update the following file with actual values:")
            console.print(f"  {self.var_lib_path / f'{self.environment}.json'}")

        profile_config = {
            "fabric-spark-testnb": {
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
        new_config = self.generate_profile_config()
        existing_config = self.read_existing_profile()

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

        # Check if fabric-spark-testnb exists and matches
        if "fabric-spark-testnb" in existing_config:
            existing_fabric = existing_config["fabric-spark-testnb"]
            new_fabric = new_config["fabric-spark-testnb"]

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
                    "\n[bold]Current fabric-spark-testnb configuration:[/bold]"
                )
                console.print(
                    yaml.dump(
                        {"fabric-spark-testnb": existing_fabric},
                        default_flow_style=False,
                    )
                )
                console.print("\n[bold]New configuration:[/bold]")
                console.print(
                    yaml.dump(
                        {"fabric-spark-testnb": new_fabric}, default_flow_style=False
                    )
                )

                if not Confirm.ask(
                    "Update the fabric-spark-testnb configuration?", default=True
                ):
                    console.print("[red]Profile update cancelled.[/red]")
                    return False

            # Update the configuration
            existing_config["fabric-spark-testnb"] = new_fabric
            self.write_profile(existing_config)
            console.print(
                f"[green]✓ Updated dbt profile at {self.profile_path}[/green]"
            )
            return True

        else:
            # fabric-spark-testnb doesn't exist, add it
            console.print(
                Panel.fit(
                    f"[yellow]Adding fabric-spark-testnb configuration to existing dbt profile[/yellow]\n"
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
                f"[green]✓ Added fabric-spark-testnb to dbt profile at {self.profile_path}[/green]"
            )
            return True


def ensure_dbt_profile(ctx: typer.Context, ask_confirmation: bool = True) -> bool:
    """Ensure dbt profile exists and is configured correctly.

    This function should be called before any dbt command execution.

    Args:
        ctx: Typer context containing workspace configuration
        ask_confirmation: Whether to ask for user confirmation

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
        manager = DBTProfileManager(workspace_dir, environment)
        return manager.check_and_update_profile(ask_confirmation)
    except FileNotFoundError as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]Error managing dbt profile: {e}[/red]")
        return False

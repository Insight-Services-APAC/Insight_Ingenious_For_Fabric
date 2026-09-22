from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml
from azure.core.credentials import TokenCredential
from fabric_cicd import (
    FabricWorkspace,
    append_feature_flag,
    constants,
    publish_all_items,
    unpublish_all_orphan_items,
)
from rich.console import Console

from ingen_fab.az_cli.credentials import get_token_credential
from ingen_fab.az_cli.onelake_utils import OneLakeUtils
from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
from ingen_fab.fabric_api.utils import FabricApiUtils

# Feature flags are global to the fabric-cicd process. Shortcut publishing has always been
# on; response collection is what turns the manifest from "attempted" into "deployed or
# failed" per item (fabric-cicd >= 1.0.0 returns the collected API responses).
append_feature_flag("enable_shortcut_publish")
append_feature_flag("enable_response_collection")


@dataclass
class PublishResult:
    """Outcome of publishing one item, derived from fabric-cicd's collected responses."""

    name: str
    item_type: str
    success: bool
    error: Optional[str] = None
    status_code: Optional[int] = None

    @property
    def key(self) -> str:
        """Manifest key: ``<name>.<ItemType>``, lower-cased."""
        return f"{self.name}.{self.item_type}".lower()


def _status_code(response: Any) -> Optional[int]:
    """Pull the HTTP status code out of one collected response.

    fabric-cicd stores either ``{"header", "body", "status_code"}`` or, when an item was
    also moved into a folder, ``{"publish_response": {...}, "move_response": {...}}``.
    """
    if not isinstance(response, dict):
        return None
    inner = response.get("publish_response", response)
    code = inner.get("status_code") if isinstance(inner, dict) else None
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


def publish_results_from_responses(
    responses: Optional[dict],
    attempted: Optional[Iterable[str]] = None,
    error: Optional[BaseException] = None,
) -> list[PublishResult]:
    """Turn fabric-cicd's response dictionary into one result per item.

    Args:
        responses: ``{item_type: {item_name: response}}`` as returned by
            ``publish_all_items`` with ``enable_response_collection``; ``None`` when nothing
            was collected.
        attempted: The ``items_to_include`` keys (``name.Type``) that were sent. When
            ``error`` is given, every attempted item without a collected response is
            reported as failed with that error.
        error: The exception that interrupted publishing, if any.
    """
    results: list[PublishResult] = []
    seen: set[str] = set()
    for item_type, by_name in (responses or {}).items():
        if not isinstance(by_name, dict):
            continue
        for name, response in by_name.items():
            code = _status_code(response)
            success = code is None or 200 <= code < 300
            result = PublishResult(
                name=name,
                item_type=item_type,
                success=success,
                error=None if success else f"HTTP {code}",
                status_code=code,
            )
            results.append(result)
            seen.add(result.key)

    if error is not None and attempted:
        for key in attempted:
            if key.lower() in seen or "." not in key:
                continue
            name, _, item_type = key.rpartition(".")
            results.append(
                PublishResult(
                    name=name, item_type=item_type, success=False, error=str(error)
                )
            )
    return results


def publish_items(
    workspace: FabricWorkspace, items_to_include: Optional[list[str]] = None
) -> list[PublishResult]:
    """Publish the repository items of ``workspace`` and return one result per item.

    ``items_to_include`` uses fabric-cicd's ``name.Type`` keys. An empty list means "publish
    everything" here; it is passed as ``None`` because, since fabric-cicd 1.1.0, an empty
    list is honoured literally and publishes nothing.
    """
    include = list(items_to_include) if items_to_include else None
    append_feature_flag("enable_experimental_features")
    append_feature_flag("enable_items_to_include")
    append_feature_flag("enable_response_collection")
    responses = publish_all_items(
        fabric_workspace_obj=workspace, items_to_include=include
    )
    return publish_results_from_responses(responses, include)


@dataclass
class WorkspaceSettings:
    """What is needed to open a Fabric workspace for publishing."""

    workspace_id: str
    repository_directory: str | Path
    environment: str = "N/A"
    item_type_in_scope: Optional[list[str]] = None
    credential: Optional[TokenCredential] = field(default=None, repr=False)


class promotion_utils:
    """Utility class for promoting Fabric items between workspaces."""

    def __init__(
        self,
        workspace: WorkspaceSettings | Any,
        console: Optional[Console] = None,
        *,
        credential: Optional[TokenCredential] = None,
    ) -> None:
        """``workspace`` is a ``WorkspaceSettings`` (or any object with the same attributes)."""
        self.console = console or Console()
        self.workspace_id = workspace.workspace_id
        self.repository_directory = workspace.repository_directory
        self.environment = workspace.environment or "N/A"
        self.credential = get_token_credential(
            credential or getattr(workspace, "credential", None)
        )

        if workspace.item_type_in_scope is None:
            self.item_type_in_scope = list(constants.ACCEPTED_ITEM_TYPES)
        else:
            self.item_type_in_scope = list(workspace.item_type_in_scope)

    def _workspace(self) -> FabricWorkspace:
        """Create a FabricWorkspace instance for the configured workspace."""
        return FabricWorkspace(
            workspace_id=self.workspace_id,
            repository_directory=str(self.repository_directory),
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
            token_credential=self.credential,
        )

    def publish_all(
        self, items_to_include: Optional[list[str]] = None
    ) -> list[PublishResult]:
        """Publish all items (or only ``items_to_include``) from the repository."""
        return publish_items(self._workspace(), items_to_include)

    def unpublish_orphans(self) -> None:
        """Remove items from the workspace that are not present in the repository."""
        ws = self._workspace()
        unpublish_all_orphan_items(fabric_workspace_obj=ws)

    def promote(self, *, delete_orphans: bool = False) -> None:
        """Publish repository items and optionally unpublish orphans."""
        self.publish_all()
        if delete_orphans:
            self.unpublish_orphans()


class SyncToFabricEnvironment:
    """Class to synchronize environment variables and platform folders with Fabric."""

    def __init__(
        self,
        project_path: str,
        environment: str = "development",
        console: Console = None,
    ):
        self.project_path = Path(project_path)
        self.environment = environment
        self.target_workspace_id = None
        self.console = console or Console()
        self.workspace_manifest_location = os.getenv("WORKSPACE_MANIFEST_LOCATION")
        self.credential: Optional[TokenCredential] = None  # resolved on first use

    @dataclass
    class manifest_item:
        """Data class to represent a platform folder item in the manifest."""

        name: str
        path: str
        hash: str
        status: str
        environment: str

    @dataclass
    class manifest:
        """Data class to represent the platform folders manifest."""

        platform_folders: list[SyncToFabricEnvironment.manifest_item]
        generated_at: str
        version: str

    def calculate_folder_hash(self, folder_path: Path) -> str:
        """Calculate SHA256 hash of all files in a folder."""
        hasher = hashlib.sha256()

        # Sort files for consistent hashing
        for file_path in sorted(folder_path.rglob("*")):
            if file_path.is_file():
                # Include relative path in hash for structure changes
                relative_path = file_path.relative_to(folder_path)
                hasher.update(str(relative_path).encode())

                # Include file content
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(65536), b""):
                        hasher.update(chunk)

        return hasher.hexdigest()

    def find_platform_folders(
        self, base_path: Path, adjust_paths: bool = False
    ) -> list[SyncToFabricEnvironment.manifest_item]:
        """Find all folders containing platform files and calculate their hashes."""
        platform_folders: list[SyncToFabricEnvironment.manifest_item] = []

        # Recursively search for .platform files
        for platform_file in base_path.rglob(".platform"):
            if platform_file.is_file():
                # Get the folder containing this .platform file
                folder = platform_file.parent
                folder_hash = self.calculate_folder_hash(folder)
                # read the platform file to get the name
                try:
                    with open(platform_file, "r", encoding="utf-8") as f:
                        platform_json = json.loads(f.read())
                        # Get the name from the platform file
                        display_name = platform_json.get("metadata").get("displayName")
                        item_type = platform_json.get("metadata").get("type")
                        # Normalize path to be relative to fabric_workspace_items for consistent comparison
                        # This handles both original and output directory paths
                        if (
                            adjust_paths
                        ):  # Paths are adjusted for manifest files during publishing
                            if "output" in str(folder):
                                # For output directory, convert back to fabric_workspace_items relative path
                                relative_path = folder.relative_to(Path("./output"))
                                normalized_path = (
                                    f"fabric_workspace_items/{relative_path}"
                                )
                            else:
                                # For original directory, make it relative to project path
                                relative_path = folder.relative_to(self.project_path)
                                normalized_path = str(relative_path)
                        else:
                            # For original directory, make it relative to project path
                            normalized_path = folder

                        platform_folders.append(
                            SyncToFabricEnvironment.manifest_item(
                                name=f"{display_name}.{item_type}",
                                path=normalized_path,
                                hash=folder_hash,
                                status="new",  # Default status for new folders
                                environment=self.environment,
                            )
                        )
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    ConsoleStyles.print_error(
                        self.console,
                        f"Error reading platform file {platform_file}: {e}",
                    )
                    raise e

        return platform_folders

    def _read_local_manifest_file(
        self, manifest_path: Path
    ) -> Optional[SyncToFabricEnvironment.manifest]:
        """Read manifest from local file only, without remote download."""
        if not manifest_path.exists():
            return SyncToFabricEnvironment.manifest(
                platform_folders=[],
                generated_at=str(Path.cwd()),
                version="1.0",
            )

        with open(manifest_path, "r", encoding="utf-8") as f:
            try:
                data = f.read()
                if data.strip() == "":
                    return SyncToFabricEnvironment.manifest(
                        platform_folders=[],
                        generated_at=str(Path.cwd()),
                        version="1.0",
                    )
                else:
                    manifest_data = yaml.safe_load(data)
                    return SyncToFabricEnvironment.manifest(
                        platform_folders=[
                            SyncToFabricEnvironment.manifest_item(**item)
                            for item in manifest_data.get("platform_folders", [])
                        ],
                        generated_at=manifest_data.get("generated_at"),
                        version=manifest_data.get("version"),
                    )
            except yaml.YAMLError as e:
                ConsoleStyles.print_error(self.console, f"Error reading manifest: {e}")
                return None

    def read_platform_manifest(
        self, manifest_path: Path
    ) -> Optional[SyncToFabricEnvironment.manifest]:
        """Read the platform folders manifest, downloading from config lakehouse if configured."""

        # If using config lakehouse, always attempt to download from lakehouse first
        if self.workspace_manifest_location == "config_lakehouse":
            ConsoleStyles.print_info(
                self.console, "Downloading manifest file from config lakehouse"
            )
            onelake_utils = OneLakeUtils(
                environment=self.environment,
                project_path=Path(self.project_path),
                console=self.console,
            )
            try:
                config_lakehouse_id = onelake_utils.get_config_lakehouse_id()
                onelake_utils._get_lakehouse_name(config_lakehouse_id)
                results = onelake_utils.download_manifest_file_from_config_lakehouse(
                    manifest_path
                )
                if not results.get("success"):
                    ConsoleStyles.print_info(
                        self.console,
                        "Manifest not found in config lakehouse - will create new one.",
                    )
            except Exception as e:
                print(e)
                ConsoleStyles.print_info(
                    self.console,
                    "Config lakehouse does not yet exist - will create new manifest.",
                )

        # Now read the manifest file (either downloaded or local)
        ConsoleStyles.print_info(self.console, str(Path.cwd()))
        ConsoleStyles.print_info(
            self.console, f"Reading manifest from: {manifest_path}"
        )

        if manifest_path.exists():
            return self._read_local_manifest_file(manifest_path)
        else:
            # File doesn't exist locally
            if self.workspace_manifest_location == "config_lakehouse":
                # In config_lakehouse mode, return empty manifest for first deployment
                ConsoleStyles.print_info(
                    self.console, "Creating new empty manifest for first deployment."
                )
                return SyncToFabricEnvironment.manifest(
                    platform_folders=[],
                    generated_at=str(Path.cwd()),
                    version="1.0",
                )
            else:
                # In local mode, file must exist
                ConsoleStyles.print_error(
                    self.console, f"Manifest file not found: {manifest_path}"
                )
                return None

    def save_platform_manifest(
        self,
        in_memory_manifest_items: list[manifest_item],
        output_path: Path,
        perform_hash_check: bool = True,
    ) -> None:
        """Save the platform folders manifest to a YAML file and upload to remote if configured."""
        # Load existing manifest from local file only (don't download from remote)
        on_disk_manifest_items: list[SyncToFabricEnvironment.manifest_item] = []
        existing_manifest = self._read_local_manifest_file(manifest_path=output_path)
        if existing_manifest and existing_manifest.platform_folders:
            on_disk_manifest_items = existing_manifest.platform_folders

        # Update statuses based on comparison
        merged_manifest_items: list[SyncToFabricEnvironment.manifest_item] = []
        for in_mem_item in in_memory_manifest_items:
            # Check if this folder exists in the existing manifest
            existing_item = next(
                (
                    f
                    for f in on_disk_manifest_items
                    if f.path.lower() == in_mem_item.path.lower()
                ),
                None,
            )

            if existing_item:
                # If it exists, compare hashes
                if perform_hash_check and existing_item.hash != in_mem_item.hash:
                    # If hashes differ, mark as updated
                    in_mem_item.status = "updated"
                else:
                    if perform_hash_check:
                        # If hashes are the same, keep existing status
                        if existing_item.status == "deleted":
                            in_mem_item.status = "deployed"
                        else:
                            in_mem_item.status = existing_item.status
                    else:
                        pass  # If hash check is not performed, keep the status as is in memory item

            # If it doesn't exist, mark as new
            else:
                in_mem_item.status = "new"

            # Find items that are in the exiting manifest but not in the new one
            merged_manifest_items.append(in_mem_item)

        # Add deleted items
        for on_disk_item in on_disk_manifest_items:
            _match = False
            for in_mem_item in in_memory_manifest_items:
                if on_disk_item.path == in_mem_item.path:
                    _match = True
                    break
            if not _match:
                # If the item is not in the new manifest, mark it as deleted
                existing_folder = SyncToFabricEnvironment.manifest_item(
                    name=on_disk_item.name,
                    path=on_disk_item.path,
                    hash=on_disk_item.hash,
                    status="deleted",
                    environment=self.environment,
                )
                merged_manifest_items.append(existing_folder)

        manifest = SyncToFabricEnvironment.manifest(
            platform_folders=[f.__dict__ for f in merged_manifest_items],
            generated_at=str(Path.cwd()),
            version=1.0,
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                manifest.__dict__, f, default_flow_style=False, sort_keys=False
            )

    def _upload_manifest_to_remote(self, manifest_path: Path) -> None:
        """Upload manifest file to config lakehouse if configured."""
        if self.workspace_manifest_location == "config_lakehouse":
            ConsoleStyles.print_info(
                self.console, "Uploading manifest file to config lakehouse"
            )
            onelake_utils = OneLakeUtils(
                environment=self.environment,
                project_path=Path(self.project_path),
                console=self.console,
            )
            onelake_utils.upload_manifest_file_to_config_lakehouse(manifest_path)

    def _get_credential(self) -> TokenCredential:
        """The credential for every Fabric call of this sync (see az_cli.credentials)."""
        if self.credential is None:
            self.credential = get_token_credential()
        return self.credential

    def _update_manifest_with_results(
        self,
        manifest_items: list[manifest_item],
        status_entries: list[PublishResult],
        manifest_path: Path,
        attempted_item_names: set[str],
        exception_occurred: bool = False,
    ) -> dict:
        """
        Update manifest with deployment results.

        Returns:
            Dict with 'deployed' and 'failed' item lists
        """
        deployed_items = []
        failed_items = []

        if not status_entries:
            if exception_occurred:
                # Deployment threw an exception — mark all attempted items as failed
                for item in manifest_items:
                    if item.name in attempted_item_names:
                        item.status = "failed"
                        failed_items.append(
                            {
                                "name": item.name,
                                "error": "Publishing exception — see error above",
                            }
                        )
            else:
                ConsoleStyles.print_warning(
                    self.console,
                    "Warning: no status entries found. Falling back to attempted item list.",
                )

                # Compatibility fallback for older fabric-cicd versions that can publish
                # successfully but return an empty status list.
                for item in manifest_items:
                    if item.name in attempted_item_names:
                        item.status = "deployed"
                        deployed_items.append({"name": item.name})
        else:
            # Create lookup dict for O(n) instead of O(n*m)
            status_lookup = {entry.key: entry for entry in status_entries}

            # Update manifest items silently
            for item in manifest_items:
                entry = status_lookup.get(item.name.lower())
                if entry:
                    if entry.success:
                        item.status = "deployed"
                        deployed_items.append({"name": item.name})
                    else:
                        item.status = "failed"
                        error_msg = (
                            getattr(entry, "error", None)
                            or getattr(entry, "message", None)
                            or getattr(entry, "error_message", None)
                            or "Unknown error"
                        )
                        failed_items.append({"name": item.name, "error": error_msg})

        # Save updated manifest
        self.save_platform_manifest(
            manifest_items, manifest_path, perform_hash_check=False
        )

        return {"deployed": deployed_items, "failed": failed_items}

    def _print_deployment_summary(self, results: dict, unchanged: int) -> None:
        """Print deployment summary."""
        deployed = results["deployed"]
        failed = results["failed"]

        if deployed:
            self.console.print()
            for item in deployed:
                ConsoleStyles.print_success(self.console, f"{item['name']}: Deployed")

        if failed:
            self.console.print()
            for item in failed:
                ConsoleStyles.print_error(self.console, f"{item['name']}: Failed")
                ConsoleStyles.print_dim(self.console, f"  {item['error']}")

        self.console.print()
        if failed:
            ConsoleStyles.print_error(
                self.console,
                f"Deploy failed! Items: {len(deployed)} deployed, {len(failed)} failed, {unchanged} unchanged.",
            )
        else:
            ConsoleStyles.print_success(
                self.console,
                f"Deploy complete! Items: {len(deployed)} deployed, {len(failed)} failed, {unchanged} unchanged.",
            )
        self.console.print()

    def _update_variables_with_item_ids_after_deployment(
        self,
        status_entries: list[PublishResult],
        workspace_id: str,
        environment: str,
    ) -> None:
        """
        Update variable library with Item IDs from successfully deployed artifacts.
        Only updates variables that already exist in the valueSet.
        Uses convention: {artifact_name}_{artifact_type_lower}_id
        """
        # Step 1: Load valueSet
        valueset_path = (
            self.project_path
            / "fabric_workspace_items"
            / "config"
            / "var_lib.VariableLibrary"
            / "valueSets"
            / f"{environment}.json"
        )

        if not valueset_path.exists():
            ConsoleStyles.print_warning(
                self.console, f"⚠️  ValueSet file not found: {valueset_path}"
            )
            return

        try:
            with open(valueset_path, "r", encoding="utf-8") as f:
                valueset_data = json.load(f)

            # Build lookup of existing variables for fast checking
            existing_vars = {
                var["name"]: var for var in valueset_data.get("variableOverrides", [])
            }

            # Step 2: Query workspace once for all artifacts
            fabric_api = FabricApiUtils(
                environment=environment,
                project_path=self.project_path,
                workspace_id=workspace_id,
                credential=self._get_credential(),
            )

            # Get all workspace items once (more efficient than multiple type-specific calls)
            all_items = fabric_api.list_workspace_items(workspace_id)

            # Build artifact lookups by type and name
            artifact_lookups = {}
            for item_type in [
                "Lakehouse",
                "Warehouse",
                "Notebook",
                "SemanticModel",
                "SQLDatabase",
                "Eventhouse",
            ]:
                artifact_lookups[item_type] = {
                    item["displayName"]: item["id"]
                    for item in all_items
                    if item.get("type") == item_type
                }

            # Step 3: Process successfully deployed artifacts
            updated_vars = []

            for entry in status_entries:
                if not entry.success:
                    continue

                # Extract artifact name (remove .Extension if present)
                artifact_name = entry.name.split(".")[0]
                artifact_type = entry.item_type

                # Convention: {name}_{type_lower}_id
                var_name = f"{artifact_name}_{artifact_type.lower()}_id"

                # Only proceed if variable exists in valueSet
                if var_name not in existing_vars:
                    continue

                # Look up Item ID from workspace
                lookup = artifact_lookups.get(artifact_type, {})
                item_id = lookup.get(artifact_name)

                if item_id:
                    old_value = existing_vars[var_name]["value"]
                    existing_vars[var_name]["value"] = item_id
                    updated_vars.append((var_name, old_value, item_id))

            # Step 4: Save if any updates made
            if updated_vars:
                with open(valueset_path, "w", encoding="utf-8") as f:
                    json.dump(valueset_data, f, indent=2, ensure_ascii=False)

                ConsoleStyles.print_success(
                    self.console, f"\n✓ Updated {len(updated_vars)} Item ID variables"
                )
                for var_name, old_val, new_val in updated_vars:
                    if old_val != new_val:
                        ConsoleStyles.print_info(
                            self.console,
                            f"  {var_name}: {old_val or '(empty)'} → {new_val}",
                        )
            else:
                ConsoleStyles.print_dim(
                    self.console,
                    "  No Item ID variables to update (none exist in valueSet)",
                )

        except Exception as e:
            ConsoleStyles.print_warning(
                self.console, f"⚠️  Failed to auto-update Item IDs: {str(e)}"
            )
            ConsoleStyles.print_info(
                self.console, "💡 Run 'ingen_fab init workspace' to update manually"
            )

    def sync_environment(self):
        """Synchronize environment variables and platform folders. Upload to Fabric."""
        # 1) Inject variables into template
        vlu = VariableLibraryUtils(
            project_path=self.project_path,
            environment=self.environment,
        )

        # Get the target workspace ID from the variables
        self.target_workspace_id = vlu.get_workspace_id()

        # Copy files from fabric_workspace_items to output directory
        source_dir = self.project_path / "fabric_workspace_items"
        output_dir = Path("./output")

        if source_dir.exists():
            # Remove existing output directory if it exists
            if output_dir.exists():
                shutil.rmtree(output_dir)

            # Copy entire directory tree
            shutil.copytree(source_dir, output_dir)
            ConsoleStyles.print_success(
                self.console, f"Copied workspace items to {output_dir}"
            )

            # Filter VariableLibrary valueSets to only include current environment
            # This ensures only the target environment's values are deployed, preventing
            # accidental exposure of other environments' configuration (e.g., dev values to prod)
            var_lib_path = output_dir / "config" / "var_lib.VariableLibrary"
            if var_lib_path.exists():
                value_sets_path = var_lib_path / "valueSets"
                settings_path = var_lib_path / "settings.json"

                if value_sets_path.exists():
                    # Remove all valueSet JSON files except the current environment
                    removed_count = 0
                    for value_set_file in value_sets_path.glob("*.json"):
                        if value_set_file.stem != self.environment:
                            value_set_file.unlink()
                            removed_count += 1
                            ConsoleStyles.print_dim(
                                self.console, f"Removed valueSet: {value_set_file.name}"
                            )

                    if removed_count > 0:
                        ConsoleStyles.print_success(
                            self.console,
                            f"Filtered valueSets to only include '{self.environment}' environment",
                        )

                # Update settings.json to only reference current environment
                if settings_path.exists():
                    with open(settings_path, "r", encoding="utf-8") as f:
                        settings = json.load(f)

                    settings["valueSetsOrder"] = [self.environment]

                    with open(settings_path, "w", encoding="utf-8") as f:
                        json.dump(settings, f, indent=2)

                    ConsoleStyles.print_success(
                        self.console,
                        f"Updated settings.json to reference only '{self.environment}' environment",
                    )
        else:
            ConsoleStyles.print_warning(
                self.console, f"Source directory {source_dir} does not exist"
            )
            return

        # Inject variables into template files in the OUTPUT directory
        ConsoleStyles.print_info(self.console, "Injecting variables into template...")

        # Create a new VariableLibraryUtils instance that will process files in the output directory
        output_vlu = VariableLibraryUtils(
            project_path=self.project_path,  # Still use original project path for variable library lookup
            environment=self.environment,
        )

        # Process all notebook-content.py files in the output directory
        updated_count = 0
        for notebook_file in output_dir.rglob("notebook-content.py"):
            with open(notebook_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Perform variable substitution (replace placeholders) and code injection
            updated_content = output_vlu.perform_code_replacements(
                content,
                replace_placeholders=True,  # Replace {{varlib:...}} placeholders during deployment
                inject_code=True,  # Also inject code between markers
            )

            if updated_content != content:
                with open(notebook_file, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                updated_count += 1

        if updated_count > 0:
            ConsoleStyles.print_success(
                self.console,
                f"Updated {updated_count} notebook files with variable substitution",
            )
        else:
            ConsoleStyles.print_info(
                self.console, "No notebook files needed variable substitution"
            )

        # Process all .tmdl files in the output directory
        tmdl_updated_count = 0
        for tmdl_file in output_dir.rglob("*.tmdl"):
            with open(tmdl_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Perform variable substitution (replace placeholders) and code injection
            updated_content = output_vlu.perform_code_replacements(
                content,
                replace_placeholders=True,  # Replace {{varlib:...}} placeholders during deployment
                inject_code=True,  # Also inject code between markers
            )

            if updated_content != content:
                with open(tmdl_file, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                tmdl_updated_count += 1

        if tmdl_updated_count > 0:
            ConsoleStyles.print_success(
                self.console,
                f"Updated {tmdl_updated_count} semantic model files with variable substitution",
            )
        else:
            ConsoleStyles.print_info(
                self.console, "No semantic model files needed variable substitution"
            )

        # Process all graphql-definition.json files in the output directory
        graphql_updated_count = 0
        for graphql_file in output_dir.rglob("graphql-definition.json"):
            with open(graphql_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Perform variable substitution (replace placeholders) and code injection
            updated_content = output_vlu.perform_code_replacements(
                content,
                replace_placeholders=True,  # Replace {{varlib:...}} placeholders during deployment
                inject_code=True,  # Also inject code between markers
            )

            if updated_content != content:
                with open(graphql_file, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                graphql_updated_count += 1

        if graphql_updated_count > 0:
            ConsoleStyles.print_success(
                self.console,
                f"Updated {graphql_updated_count} GraphQL API files with variable substitution",
            )
        else:
            ConsoleStyles.print_info(
                self.console, "No GraphQL API files needed variable substitution"
            )

        # Process all pipeline-content.json files in the output directory
        pipeline_updated_count = 0
        for pipeline_file in output_dir.rglob("pipeline-content.json"):
            with open(pipeline_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Perform variable substitution (replace placeholders) and code injection
            updated_content = output_vlu.perform_code_replacements(
                content,
                replace_placeholders=True,  # Replace {{varlib:...}} placeholders during deployment
                inject_code=True,  # Also inject code between markers
            )

            if updated_content != content:
                with open(pipeline_file, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                pipeline_updated_count += 1

        if pipeline_updated_count > 0:
            ConsoleStyles.print_success(
                self.console,
                f"Updated {pipeline_updated_count} data pipeline files with variable substitution",
            )
        else:
            ConsoleStyles.print_info(
                self.console, "No data pipeline files needed variable substitution"
            )

        # Process all definition.pbir files in the output directory
        pbir_updated_count = 0
        for pbir_file in output_dir.rglob("definition.pbir"):
            with open(pbir_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Perform variable substitution (replace placeholders) and code injection
            updated_content = output_vlu.perform_code_replacements(
                content,
                replace_placeholders=True,  # Replace {{varlib:...}} placeholders during deployment
                inject_code=True,  # Also inject code between markers
            )

            if updated_content != content:
                with open(pbir_file, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                pbir_updated_count += 1

        if pbir_updated_count > 0:
            ConsoleStyles.print_success(
                self.console,
                f"Updated {pbir_updated_count} Power BI report files with variable substitution",
            )
        else:
            ConsoleStyles.print_info(
                self.console, "No Power BI report files needed variable substitution"
            )

        # 2) Download manifest from remote if configured (PULL remote state)
        manifest_path = Path(
            f"{self.project_path}/platform_manifest_{self.environment}.yml"
        )
        ConsoleStyles.print_info(self.console, "\nLoading platform manifest...")
        manifest = self.read_platform_manifest(manifest_path)

        # 3) Find folders with platform files and generate hashes - SCAN THE OUTPUT DIRECTORY
        fabric_items_path = (
            output_dir  # Changed: scan the output directory, not the original
        )
        # Before publishing remove all __pycache__ folders from OUTPUT directory
        for pycache in fabric_items_path.rglob("__pycache__"):
            if pycache.is_dir():
                ConsoleStyles.print_dim(
                    self.console, f"Removing __pycache__ folder: {pycache}"
                )
                shutil.rmtree(pycache)
        ConsoleStyles.print_info(
            self.console, f"\nScanning for platform folders in: {fabric_items_path}"
        )

        platform_folders = self.find_platform_folders(
            fabric_items_path, adjust_paths=True
        )

        if platform_folders:
            ConsoleStyles.print_success(
                self.console,
                f"Found {len(platform_folders)} folders with platform files:",
            )
            for folder in platform_folders:
                ConsoleStyles.print_info(
                    self.console, f"  - {folder.name}: {folder.hash[:16]}..."
                )

            # Save manifest locally (merge with downloaded manifest)
            self.save_platform_manifest(
                platform_folders, manifest_path, perform_hash_check=True
            )
            ConsoleStyles.print_success(
                self.console, f"\nSaved platform manifest to: {manifest_path}"
            )

            # Re-read manifest after saving to get updated status
            manifest = self._read_local_manifest_file(manifest_path)
        else:
            ConsoleStyles.print_warning(
                self.console, "No folders with platform files found."
            )

        manifest_items: list[SyncToFabricEnvironment.manifest_item] = []

        if manifest:
            manifest_items = manifest.platform_folders

            manifest_items_new_updated: list[SyncToFabricEnvironment.manifest_item] = [
                f for f in manifest_items if f.status in ["new", "updated", "failed"]
            ]

            # Initialize deployment results
            results = {"deployed": [], "failed": []}

            if manifest_items_new_updated:
                ConsoleStyles.print_success(
                    self.console,
                    f"Found {len(manifest_items_new_updated)} folders to publish",
                )

                items_to_publish = [
                    f"{item.name}" for item in manifest_items_new_updated
                ]
                ConsoleStyles.print_info(
                    self.console, f"Items to publish: {items_to_publish}"
                )

                _item_type_in_scope = os.getenv("ITEM_TYPES_TO_DEPLOY", "")
                if _item_type_in_scope == "":
                    item_type_in_scope = [
                        "VariableLibrary",
                        "DataPipeline",
                        "Environment",
                        "Notebook",
                        "Report",
                        "SemanticModel",
                        "Lakehouse",
                        "MirroredDatabase",
                        "CopyJob",
                        "Eventhouse",
                        "Reflex",
                        "Eventstream",
                        "Warehouse",
                        "SQLDatabase",
                        "GraphQLApi",
                    ]
                    ConsoleStyles.print_info(
                        self.console, "Items to be published filter: None"
                    )
                else:
                    ConsoleStyles.print_info(
                        self.console,
                        "Items to be published filter: " + _item_type_in_scope,
                    )
                    item_type_in_scope = [
                        item.strip() for item in _item_type_in_scope.split(",")
                    ]

                ConsoleStyles.print_info(self.console, "\nPublishing items...")
                status_entries: list[PublishResult] = []
                publish_exception = False

                fw = None
                try:
                    fw = FabricWorkspace(
                        workspace_id=self.target_workspace_id,
                        repository_directory=str(output_dir),
                        item_type_in_scope=item_type_in_scope,
                        environment=self.environment,
                        token_credential=self._get_credential(),
                    )
                    status_entries = publish_items(fw, items_to_publish)
                except Exception as e:
                    ConsoleStyles.print_error(
                        self.console, f"\nPublishing failed with error: {e}"
                    )
                    publish_exception = True
                    # Items published before the failure are still reported as deployed;
                    # the attempted ones without a response as failed.
                    status_entries = publish_results_from_responses(
                        getattr(fw, "responses", None), items_to_publish, error=e
                    )

                # Auto-update Item IDs if enabled
                auto_update_enabled = os.getenv("AUTO_UPDATE_ITEM_IDS", "").lower() in [
                    "true",
                    "1",
                    "yes",
                    "y",
                ]

                if auto_update_enabled:
                    if status_entries:
                        ConsoleStyles.print_info(
                            self.console,
                            "\n[cyan]Auto-updating Item IDs[/cyan] (AUTO_UPDATE_ITEM_IDS=true)",
                        )
                        self._update_variables_with_item_ids_after_deployment(
                            status_entries=status_entries,
                            workspace_id=self.target_workspace_id,
                            environment=self.environment,
                        )
                    else:
                        ConsoleStyles.print_info(
                            self.console,
                            "\n💡 AUTO_UPDATE_ITEM_IDS enabled but no items were deployed",
                        )
                else:
                    ConsoleStyles.print_dim(
                        self.console,
                        "\n💡 Tip: Set AUTO_UPDATE_ITEM_IDS=true to automatically update Item ID variables",
                    )

                results = self._update_manifest_with_results(
                    manifest_items,
                    status_entries,
                    manifest_path,
                    attempted_item_names={
                        item.name for item in manifest_items_new_updated
                    },
                    exception_occurred=publish_exception,
                )

            # Calculate unchanged count
            attempted_item_names = {item.name for item in manifest_items_new_updated}
            unchanged_count = sum(
                1
                for item in manifest_items
                if item.name not in attempted_item_names and item.status != "deleted"
            )

            self._print_deployment_summary(results, unchanged_count)

            # Upload manifest to remote at the very end (PUSH remote state)
            self._upload_manifest_to_remote(manifest_path)

            if results["failed"]:
                raise SystemExit(1)
        else:
            ConsoleStyles.print_warning(self.console, "Platform manifest not found")

    def clear_environment(self):
        # make an empty temp dir
        temp_publish_path = Path("./temp-publish")
        if temp_publish_path.exists():
            shutil.rmtree(temp_publish_path)
        temp_publish_path.mkdir(parents=True, exist_ok=True)
        settings = WorkspaceSettings(
            workspace_id=self.target_workspace_id,
            repository_directory=temp_publish_path,
            item_type_in_scope=[
                "VariableLibrary",
                "DataPipeline",
                "Environment",
                "Notebook",
                "Report",
                "SemanticModel",
                "MirroredDatabase",
                "CopyJob",
                "Eventhouse",
                "Reflex",
                "Eventstream",
                "SQLDatabase",
            ],
            environment=self.environment,
            credential=self._get_credential(),
        )
        pu = promotion_utils(settings, self.console)

        ConsoleStyles.print_info(self.console, "Unpublishing all orphan items...")
        pu.unpublish_orphans()

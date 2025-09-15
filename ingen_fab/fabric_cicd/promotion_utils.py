from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml
from fabric_cicd import (
    FabricWorkspace,
    append_feature_flag,
    constants,
    publish_all_items,
    unpublish_all_orphan_items,
)
from fabric_cicd._common._publish_log_entry import PublishLogEntry
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles

# from fabric_cicd.fabric_workspace import PublishLogEntry
from ingen_fab.config_utils.variable_lib import VariableLibraryUtils

from ingen_fab.az_cli.onelake_utils import OneLakeUtils

from fabric_cicd import (
    FabricWorkspace,
    constants,
    publish_all_items,
    unpublish_all_orphan_items,
    append_feature_flag
)

import os

append_feature_flag("enable_shortcut_publish")

class promotion_utils:
    """Utility class for promoting Fabric items between workspaces."""

    def __init__(self, FabricWorkspace: FabricWorkspace, console: Console) -> None:
        self.console = console
        self.workspace_id = FabricWorkspace.workspace_id
        self.repository_directory = FabricWorkspace.repository_directory
        self.environment = FabricWorkspace.environment

        if FabricWorkspace.item_type_in_scope is None:
            self.item_type_in_scope = list(constants.ACCEPTED_ITEM_TYPES_UPN)
        else:
            self.item_type_in_scope = list(FabricWorkspace.item_type_in_scope)

    def _workspace(self) -> FabricWorkspace:
        """Create a FabricWorkspace instance for the configured workspace."""
        return FabricWorkspace(
            workspace_id=self.workspace_id,
            repository_directory=str(self.repository_directory),
            item_type_in_scope=self.item_type_in_scope,
            items_to_include=self.items_to_include,
            environment=self.environment,
        )

    def publish_all(self, items_to_include: list[str]= []) -> list[PublishLogEntry]:
        """Publish all items from the repository to the workspace."""
        ws = self._workspace()
        ConsoleStyles.print_info(self.console, f"before: {constants.FEATURE_FLAG}")
        append_feature_flag("enable_experimental_features")
        append_feature_flag("enable_items_to_include")
        ConsoleStyles.print_info(self.console, f"after: {constants.FEATURE_FLAG}")
        return publish_all_items(fabric_workspace_obj=ws, items_to_include=items_to_include)

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
        self.workspace_manifest_location = os.getenv('WORKSPACE_MANIFEST_LOCATION')

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

    def read_platform_manifest(
        self, manifest_path: Path
    ) -> Optional[SyncToFabricEnvironment.manifest]:
        
        if self.workspace_manifest_location == "config_lakehouse":
            ConsoleStyles.print_info(
                self.console, f"Downloading manifest file from config lakehouse"
            )
            onelake_utils = OneLakeUtils(
                environment=self.environment, project_path=Path(self.project_path), console=self.console
            )
            try:
                config_lakehouse_id = onelake_utils.get_config_lakehouse_id()
                onelake_utils._get_lakehouse_name(config_lakehouse_id)
                results = onelake_utils.download_manifest_file_from_config_lakehouse(manifest_path)
            except Exception as e:
                ConsoleStyles.print_info(
                    self.console, f"Config lakehouse does not yet exist."
                )

        """Read the platform folders manifest from a YAML file."""
        ConsoleStyles.print_info(self.console, str(Path.cwd()))
        ConsoleStyles.print_info(
            self.console, f"Reading manifest from: {manifest_path}"
        )
        if manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as f:
                try:
                    data = f.read()
                    if data.strip() == "":
                        ConsoleStyles.print_warning(
                            self.console, "Manifest file is empty."
                        )
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
                    ConsoleStyles.print_error(
                        self.console, f"Error reading manifest: {e}"
                    )
                    return None
        else:
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
        """Save the platform folders manifest to a YAML file."""
        # Load existing manifest if it exists
        on_disk_manifest_items: list[SyncToFabricEnvironment.manifest_item] = []
        existing_manifest = self.read_platform_manifest(manifest_path=output_path)
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
        
        if self.workspace_manifest_location == "config_lakehouse":
            ConsoleStyles.print_info(
                self.console, f"Uploading manifest file to config lakehouse"
            )
            onelake_utils = OneLakeUtils(
                environment=self.environment, project_path=Path(self.project_path), console=self.console
            )
            results = onelake_utils.upload_manifest_file_to_config_lakehouse(output_path)

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

        # 2) Find folders with platform files and generate hashes - SCAN THE OUTPUT DIRECTORY
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
        manifest_path = Path(
            f"{self.project_path}/platform_manifest_{self.environment}.yml"
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

            # Save manifest
            self.save_platform_manifest(
                platform_folders, manifest_path, perform_hash_check=True
            )
            ConsoleStyles.print_success(
                self.console, f"\nSaved platform manifest to: {manifest_path}"
            )
        else:
            ConsoleStyles.print_warning(
                self.console, "No folders with platform files found."
            )

        manifest_items: list[SyncToFabricEnvironment.manifest_item] = []
        if manifest_path.exists():
            ConsoleStyles.print_info(self.console, "\nLoading platform manifest...")
            manifest: SyncToFabricEnvironment.manifest = self.read_platform_manifest(
                manifest_path
            )
            manifest_items: list[SyncToFabricEnvironment.manifest_item] = (
                manifest.platform_folders
            )

            manifest_items_new_updated: list[SyncToFabricEnvironment.manifest_item] = [
                f for f in manifest_items if f.status in ["new", "updated"]
            ]

            if manifest_items_new_updated:
                ConsoleStyles.print_success(
                    self.console,
                    f"Found {len(manifest_items_new_updated)} folders to publish",
                )

                # add the name of items to publish to list[str]
                items_to_publish = [
                    f"{item.name}" for item in manifest_items_new_updated
                ]

                ConsoleStyles.print_info(
                    self.console, f"Items to publish: {items_to_publish}"
                )

                status_entries: list[PublishLogEntry]
                status_entries = None
                # After copying all folders, attempt to publish
                ConsoleStyles.print_info(self.console, "\nPublishing items...")
                try:
                    fw = FabricWorkspace(
                        workspace_id=self.target_workspace_id,
                        repository_directory=str(
                            output_dir
                        ),  # Changed: publish from output directory
                        item_type_in_scope=[
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
                        ],
                        environment="development",
                    )

                    append_feature_flag("enable_experimental_features")
                    append_feature_flag("enable_items_to_include")
                    status_entries = publish_all_items(
                        fabric_workspace_obj=fw, items_to_include=items_to_publish
                    )

                    ConsoleStyles.print_success(
                        self.console, "\nPublishing succeeded. Updating manifest..."
                    )

                except Exception as e:
                    # If failed, update status to "failed"
                    ConsoleStyles.print_error(
                        self.console, f"\nPublishing failed with error: {e}"
                    )
                finally:
                    # Update status in manifest
                    for item in manifest_items:
                        if status_entries is None:
                            ConsoleStyles.print_warning(
                                self.console, "Warning: no status entries found."
                            )
                            break
                        else:
                            for entry in status_entries:
                                # ConsoleStyles.print_info(self.console, f"Checking status for {entry.name}...")
                                if (
                                    f"{entry.name}.{entry.item_type}".lower()
                                    == item.name.lower()
                                ):
                                    if entry.success:
                                        item.status = "deployed"
                                        ConsoleStyles.print_success(
                                            self.console,
                                            f"Item {item.name} deployed successfully",
                                        )
                                    break
                    # Save updated manifest
                    self.save_platform_manifest(
                        manifest_items, manifest_path, perform_hash_check=False
                    )
                    ConsoleStyles.print_success(
                        self.console, "Manifest updated with deployed status"
                    )

            else:
                ConsoleStyles.print_info(
                    self.console, "No folders with new/updated status to publish"
                )
        else:
            ConsoleStyles.print_warning(self.console, "Platform manifest not found")

        ConsoleStyles.print_success(
            self.console, "Promotion utilities initialized successfully."
        )

    def clear_environment(self):
        # make an empty temp dir
        temp_publish_path = Path("./temp-publish")
        if temp_publish_path.exists():
            shutil.rmtree(temp_publish_path)
        temp_publish_path.mkdir(parents=True, exist_ok=True)
        pu = promotion_utils(
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
        )

        ConsoleStyles.print_info(self.console, "Unpublishing all orphan items...")
        pu.unpublish_orphans()

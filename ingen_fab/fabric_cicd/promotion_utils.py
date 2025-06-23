import hashlib
from pathlib import Path
import shutil
from typing import Iterable, Optional, List
import yaml

from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
    constants,
)

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils


class promotion_utils:
    """Utility class for promoting Fabric items between workspaces."""

    def __init__(
        self,
        workspace_id: str,
        repository_directory: str | str = "fabric_workspace_items",
        *,
        item_type_in_scope: Optional[Iterable[str]] = None,
        environment: str = "N/A",
    ) -> None:
        self.workspace_id = workspace_id
        self.repository_directory = repository_directory
        self.environment = environment
        if item_type_in_scope is None:
            self.item_type_in_scope = list(constants.ACCEPTED_ITEM_TYPES_UPN)
        else:
            self.item_type_in_scope = list(item_type_in_scope)

    def _workspace(self) -> FabricWorkspace:
        """Create a FabricWorkspace instance for the configured workspace."""
        return FabricWorkspace(
            workspace_id=self.workspace_id,
            repository_directory=str(self.repository_directory),
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

    def publish_all(self) -> None:
        """Publish all items from the repository to the workspace."""
        ws = self._workspace()
        publish_all_items(fabric_workspace_obj=ws)

    def unpublish_orphans(self) -> None:
        """Remove items from the workspace that are not present in the repository."""
        ws = self._workspace()
        unpublish_all_orphan_items(fabric_workspace_obj=ws)

    def promote(self, *, delete_orphans: bool = False) -> None:
        """Publish repository items and optionally unpublish orphans."""
        self.publish_all()
        if delete_orphans:
            self.unpublish_orphans()


class SyncToFabricEnvionment:
    """Class to synchronize environment variables and platform folders with Fabric."""

    def __init__(self, project_path: str, environment: str = "development"):
        self.project_path = Path(project_path)
        self.environment = environment
        self.target_workspace_id = None

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

    def find_platform_folders(self, base_path: Path) -> list[dict[str, str]]:
        """Find all folders containing platform files and calculate their hashes."""
        platform_folders = []
        
        # Recursively search for .platform files
        for platform_file in base_path.rglob(".platform"):
            if platform_file.is_file():
                # Get the folder containing this .platform file
                folder = platform_file.parent
                folder_hash = self.calculate_folder_hash(folder)
                platform_folders.append({
                    "name": folder.name,
                    "path": str(folder.relative_to(base_path.parent)),
                    "hash": folder_hash,
                    "status": "new",
                    "environment": self.environment
                })
        
        return platform_folders

    def save_platform_manifest(self, folders: list[dict[str, str]], output_path: Path) -> None:
        """Save the platform folders manifest to a YAML file."""
        # Load existing manifest if it exists
        existing_folders = {}
        if output_path.exists():
            with open(output_path, "r") as f:
                existing_manifest = yaml.safe_load(f)
                if existing_manifest and "platform_folders" in existing_manifest:
                    # Create a lookup dict by path
                    existing_folders = {folder["path"]: folder for folder in existing_manifest["platform_folders"]}
        
        # Update statuses based on comparison
        current_paths = set()
        for folder in folders:
            path = folder["path"]
            current_paths.add(path)
            
            if path in existing_folders:
                existing_folder = existing_folders[path]
                if folder["hash"] == existing_folder["hash"]:
                    # Hashes match - preserve all existing attributes
                    folder.update(existing_folder)
                else:
                    # Hash changed - mark as updated
                    folder["status"] = "updated"
            else:
                # New folder - already marked as "new"
                pass
        
        # Add deleted folders
        for path, existing_folder in existing_folders.items():
            if path not in current_paths:
                deleted_folder = existing_folder.copy()
                deleted_folder["status"] = "deleted"
                folders.append(deleted_folder)
        
        manifest = {
            "platform_folders": folders,
            "generated_at": str(Path.cwd()),
            "version": "1.0"
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)

    def sync_environment(self):
        """Synchronize environment variables and platform folders. Upload to Fabric."""
        # 1) Inject variables into template
        vlu = VariableLibraryUtils(
            project_path=self.project_path,
            environment=self.environment,
            template_path="ingen_fab/ddl_scripts/_templates/warehouse/config.py.jinja",
            output=None,
            in_place=False
        )

        varlib_data = vlu.load_variable_library(self.project_path, self.environment)
        # Extract variables
        variables = vlu.extract_variables(varlib_data)
        # Get the target workspace ID from the variables
        self.target_workspace_id = variables.get("fabric_deployment_workspace_id", self.target_workspace_id)

        print("Injecting variables into template...")
        vlu.inject_variables_into_template()
        
        # 2) Find folders with platform files and generate hashes
        fabric_items_path = Path("./sample_project/fabric_workspace_items")
        print(f"\nScanning for platform folders in: {fabric_items_path}")
        
        platform_folders = self.find_platform_folders(fabric_items_path)
        
        if platform_folders:
            print(f"Found {len(platform_folders)} folders with platform files:")
            for folder in platform_folders:
                print(f"  - {folder['name']}: {folder['hash'][:16]}...")
            
            # Save manifest
            manifest_path = Path("./sample_project/platform_manifest.yml")
            self.save_platform_manifest(platform_folders, manifest_path)
            print(f"\nSaved platform manifest to: {manifest_path}")
        else:
            print("No folders with platform files found.")
        
        print("\nInitializing promotion utilities...")
        temp_publish_path = Path("./temp-publish")
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
                "Lakehouse",
                "MirroredDatabase", 
                "CopyJob",
                "Eventhouse",
                "Reflex",
                "Eventstream",
                "Warehouse",
                "SQLDatabase"
            ],
            environment="development",
        )


        # 3) Load platform manifest and copy updated/new folders
        manifest_path = Path(f"{self.project_path}/platform_manifest.yml")

        if manifest_path.exists():
            print("\nLoading platform manifest...")
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = yaml.safe_load(f)
            
            platform_folders = manifest.get("platform_folders", [])
            folders_to_publish = [f for f in platform_folders if f["status"] in ["new", "updated"]]
            
            if folders_to_publish:
                print(f"Found {len(folders_to_publish)} folders to publish")
                
                # Clear temp-publish directory if it exists
                if temp_publish_path.exists():
                    shutil.rmtree(temp_publish_path)
                temp_publish_path.mkdir(parents=True, exist_ok=True)
                
                # Copy folders with new/updated status
                for folder in platform_folders:
                    source_path = Path(self.project_path) / Path(folder["path"])
                    dest_path = temp_publish_path / source_path.name
                    
                    print(f"  - Copying {folder['name']} ({folder['status']}) to temp-publish/")
                    shutil.copytree(source_path, dest_path)
                    
                    # After copying all folders, attempt to publish
                    print("\nPublishing items...")
                    try:
                        pu.publish_all()
                        
                        # If successful, update status to "deployed"
                        print("\nPublishing succeeded. Updating manifest...")
                        folder["status"] = "deployed"
                        
                    except Exception as e:
                        # If failed, update status to "failed"
                        print(f"\nPublishing failed with error: {e}")
                        folder["status"] = "failed"
                        print("Manifest updated with failed status")
                    finally:
                        # remove temp-publish directory
                        if temp_publish_path.exists():
                            shutil.rmtree(temp_publish_path)

                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(manifest_path, "w") as f:
                    yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)
                    
                print("Manifest updated with deployed status")
            else:
                print("No folders with new/updated status to publish")
        else:
            print("Platform manifest not found")
        
        
        
        print("Promotion utilities initialized successfully.")
            

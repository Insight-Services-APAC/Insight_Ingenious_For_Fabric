from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import yaml

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
from ingen_fab.fabric_cicd.promotion_utils import promotion_utils

project_path = "./sample_project"
environment = "development"


def calculate_folder_hash(folder_path: Path) -> str:
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


def find_platform_folders(base_path: Path) -> list[dict[str, str]]:
    """Find all folders containing platform files and calculate their hashes."""
    platform_folders = []

    # Recursively search for .platform files
    for platform_file in base_path.rglob(".platform"):
        if platform_file.is_file():
            # Get the folder containing this .platform file
            folder = platform_file.parent
            folder_hash = calculate_folder_hash(folder)
            platform_folders.append(
                {
                    "name": folder.name,
                    "path": str(folder.relative_to(base_path.parent)),
                    "hash": folder_hash,
                    "status": "new",
                    "environment": environment,
                }
            )

    return platform_folders


def save_platform_manifest(folders: list[dict[str, str]], output_path: Path) -> None:
    """Save the platform folders manifest to a YAML file."""
    # Load existing manifest if it exists
    existing_folders = {}
    if output_path.exists():
        with open(output_path, "r") as f:
            existing_manifest = yaml.safe_load(f)
            if existing_manifest and "platform_folders" in existing_manifest:
                # Create a lookup dict by path
                existing_folders = {
                    folder["path"]: folder
                    for folder in existing_manifest["platform_folders"]
                }

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
        "version": "1.0",
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)


def main():
    # 1) Inject variables into template
    vlu = VariableLibraryUtils(
        project_path=project_path,
        environment=environment,
        template_path="ingen_fab/ddl_scripts/_templates/warehouse/config.py.jinja",
        output=None,
        in_place=False,
    )

    print("Injecting variables into template...")
    vlu.inject_variables_into_template()

    # 2) Find folders with platform files and generate hashes
    fabric_items_path = Path("./sample_project/fabric_workspace_items")
    print(f"\nScanning for platform folders in: {fabric_items_path}")

    platform_folders = find_platform_folders(fabric_items_path)

    if platform_folders:
        print(f"Found {len(platform_folders)} folders with platform files:")
        for folder in platform_folders:
            print(f"  - {folder['name']}: {folder['hash'][:16]}...")

        # Save manifest
        manifest_path = Path("./sample_project/platform_manifest.yml")
        save_platform_manifest(platform_folders, manifest_path)
        print(f"\nSaved platform manifest to: {manifest_path}")
    else:
        print("No folders with platform files found.")

    # 4) Instantiate promotion_utils
    workspace_id = "3a4fc13c-f7c5-463e-a9de-57c4754699ff"
    lakehouse_id = "352eb28d-d085-4767-a985-28b03d0829ae"

    print("\nInitializing promotion utilities...")
    temp_publish_path = Path("./temp-publish")
    pu = promotion_utils(
        workspace_id=workspace_id,
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
            "SQLDatabase",
        ],
        environment="development",
    )

    # 3) Load platform manifest and copy updated/new folders
    manifest_path = Path(f"{project_path}/platform_manifest.yml")

    if manifest_path.exists():
        print("\nLoading platform manifest...")
        with open(manifest_path, "r") as f:
            manifest = yaml.safe_load(f)

        platform_folders = manifest.get("platform_folders", [])
        folders_to_publish = [
            f for f in platform_folders if f["status"] in ["new", "updated"]
        ]

        if folders_to_publish:
            print(f"Found {len(folders_to_publish)} folders to publish")

            # Clear temp-publish directory if it exists
            if temp_publish_path.exists():
                shutil.rmtree(temp_publish_path)
            temp_publish_path.mkdir(parents=True, exist_ok=True)

            # Copy folders with new/updated status
            for folder in platform_folders:
                source_path = Path(project_path) / Path(folder["path"])
                dest_path = temp_publish_path / source_path.name

                print(
                    f"  - Copying {folder['name']} ({folder['status']}) to temp-publish/"
                )
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

    # Uncomment to run promotion operations
    # pu.unpublish_orphans()
    # pu.publish_all()

    return pu


if __name__ == "__main__":
    pu = main()

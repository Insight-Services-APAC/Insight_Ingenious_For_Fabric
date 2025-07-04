#!/usr/bin/env python3
"""
Clean a Fabric workspace by deleting all objects except lakehouses and warehouses.
"""

import argparse
import json
import subprocess
import sys
from typing import Any, Dict, List


def run_fabric_command(cmd: List[str]) -> tuple[bool, str]:
    """Run a fabric CLI command and return success status and output."""
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True, result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return False, e.stderr.strip()


def list_workspace_items(workspace_name: str) -> List[Dict[str, Any]]:
    """List all items in a workspace."""
    cmd = ["fab", "list", f"{workspace_name}.Workspace", "--output", "json"]
    success, output = run_fabric_command(cmd)

    if not success:
        print(f"Error listing workspace items: {output}")
        return []

    try:
        items = json.loads(output)
        return items if isinstance(items, list) else []
    except json.JSONDecodeError:
        print(f"Error parsing JSON output: {output}")
        return []


def delete_item(workspace_name: str, item_name: str, item_type: str) -> bool:
    """Delete a single item from the workspace."""
    # Construct the full item path
    item_path = f"{workspace_name}.Workspace/{item_name}.{item_type}"
    cmd = ["fab", "delete", item_path, "-f"]  # -f for force (no confirmation)

    success, output = run_fabric_command(cmd)

    if success:
        print(f"✓ Deleted {item_type}: {item_name}")
    else:
        print(f"✗ Failed to delete {item_type}: {item_name} - {output}")

    return success


def clean_workspace(workspace_name: str, dry_run: bool = False) -> None:
    """Clean all objects from workspace except lakehouses and warehouses."""
    # Types to preserve
    preserve_types = {"Lakehouse", "Warehouse"}

    print(f"Cleaning workspace: {workspace_name}")
    print(f"Preserving: {', '.join(preserve_types)}")
    print("-" * 50)

    # Get all items in workspace
    items = list_workspace_items(workspace_name)

    if not items:
        print("No items found in workspace or error occurred.")
        return

    # Separate items to delete and preserve
    items_to_delete = []
    items_to_preserve = []

    for item in items:
        item_name = item.get("name", "Unknown")
        item_type = item.get("type", "Unknown")

        if item_type in preserve_types:
            items_to_preserve.append((item_name, item_type))
        else:
            items_to_delete.append((item_name, item_type))

    # Show summary
    print(f"\nFound {len(items)} items total:")
    print(f"- {len(items_to_preserve)} to preserve")
    print(f"- {len(items_to_delete)} to delete")

    if items_to_preserve:
        print("\nPreserving:")
        for name, item_type in items_to_preserve:
            print(f"  - {item_type}: {name}")

    if not items_to_delete:
        print("\nNothing to delete!")
        return

    print("\nTo be deleted:")
    for name, item_type in items_to_delete:
        print(f"  - {item_type}: {name}")

    if dry_run:
        print("\n[DRY RUN] No items were deleted.")
        return

    # Confirm deletion
    print(f"\nThis will delete {len(items_to_delete)} items from the workspace.")
    response = input("Are you sure you want to continue? (yes/no): ")

    if response.lower() != "yes":
        print("Operation cancelled.")
        return

    # Delete items
    print("\nDeleting items...")
    success_count = 0

    for name, item_type in items_to_delete:
        if delete_item(workspace_name, name, item_type):
            success_count += 1

    # Summary
    print("\n" + "=" * 50)
    print(
        f"Deletion complete: {success_count}/{len(items_to_delete)} items deleted successfully"
    )

    if success_count < len(items_to_delete):
        print(f"Failed to delete {len(items_to_delete) - success_count} items")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Clean a Fabric workspace by deleting all objects except lakehouses and warehouses."
    )
    parser.add_argument("workspace", help="Name of the workspace to clean")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    args = parser.parse_args()

    try:
        clean_workspace(args.workspace, args.dry_run)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

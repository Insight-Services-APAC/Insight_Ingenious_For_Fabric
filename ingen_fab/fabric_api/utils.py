import logging
from pathlib import Path
from typing import Optional

import requests
from azure.identity import DefaultAzureCredential

from ingen_fab.config_utils.variable_lib_factory import (
    get_workspace_id_from_environment,
)

# Suppress verbose Azure SDK logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("azure.identity").setLevel(logging.WARNING)


class FabricApiUtils:
    """
    Utility class for interacting with the Fabric API.
    """

    def __init__(
        self,
        environment: str,
        project_path: Path,
        *,
        credential: Optional[DefaultAzureCredential] = None,
        workspace_id: Optional[str] = None,
    ) -> None:
        self.environment = environment
        self.project_path = project_path
        self.credential = credential or DefaultAzureCredential()
        self.base_url = "https://api.fabric.microsoft.com/v1/workspaces"
        # Only get workspace_id from variable library if not provided
        if workspace_id:
            self.workspace_id = workspace_id
        else:
            self.workspace_id = self._get_workspace_id()

    def _get_token(self) -> str:
        scope = "https://api.fabric.microsoft.com/.default"
        token = self.credential.get_token(scope)
        return token.token

    def _get_workspace_id(self) -> str:
        """
        Set the workspace ID for API requests.
        """
        return get_workspace_id_from_environment(self.environment, self.project_path)

    def get_workspace_id_from_name(self, workspace_name: str) -> Optional[str]:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = "https://api.fabric.microsoft.com/v1/workspaces"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            workspaces = response.json()
            for ws in workspaces.get("value", []):
                if ws["displayName"] == workspace_name:
                    return ws["id"]
        return None

    def get_lakehouse_id_from_name(
        self, workspace_id: str, lakehouse_name: str
    ) -> Optional[str]:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            items = response.json()
            for item in items.get("value", []):
                if (
                    item["displayName"] == lakehouse_name
                    and item["type"] == "Lakehouse"
                ):
                    return item["id"]
        return None

    def get_lakehouse_name_from_id(
        self, workspace_id: str, lakehouse_id: str
    ) -> Optional[str]:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{lakehouse_id}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            item = response.json()
            if item.get("type") == "Lakehouse":
                return item.get("displayName")
        return None

    def get_workspace_name_from_id(self, workspace_id: str) -> Optional[str]:
        """
        Get the workspace name from its ID.
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            workspace = response.json()
            return workspace.get("displayName")
        return None

    def delete_all_items_except_lakehouses_and_warehouses(self) -> dict[str, int]:
        """
        Delete all items in the workspace except lakehouses and warehouses.

        Returns:
            Dictionary with counts of deleted items by type and any errors
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        # Item types to preserve
        preserved_types = {"Lakehouse", "Warehouse"}

        deleted_counts = {}
        errors = []

        # Get all items in the workspace with pagination
        continuation_token = None

        while True:
            # Build URL with continuation token if present
            url = f"{self.base_url}/{self.workspace_id}/items"
            if continuation_token:
                url += f"?continuationToken={continuation_token}"

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(
                    f"Failed to list workspace items: {response.status_code} - {response.text}"
                )

            data = response.json()
            items = data.get("value", [])

            # Process items
            for item in items:
                item_type = item.get("type")
                item_id = item.get("id")
                item_name = item.get("displayName", "Unknown")

                # Skip if type should be preserved
                if item_type in preserved_types:
                    continue

                # Delete the item
                delete_url = f"{self.base_url}/{self.workspace_id}/items/{item_id}"
                delete_response = requests.delete(delete_url, headers=headers)

                if delete_response.status_code == 200:
                    # Track successful deletions by type
                    deleted_counts[item_type] = deleted_counts.get(item_type, 0) + 1
                else:
                    # Track errors
                    errors.append(
                        {
                            "item_name": item_name,
                            "item_type": item_type,
                            "item_id": item_id,
                            "status_code": delete_response.status_code,
                            "error": delete_response.text,
                        }
                    )

            # Check for continuation token
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break

        return {
            "deleted_counts": deleted_counts,
            "errors": errors,
            "total_deleted": sum(deleted_counts.values()),
        }

    def get_warehouse_sql_endpoint(self, workspace_id: str, warehouse_id: str) -> str:
        """
        Get the SQL endpoint for a given warehouse.

        Args:
            warehouse_id: The ID of the warehouse.

        Returns:
            The SQL endpoint URL for the warehouse.
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        # List all warehouse items in the workspace
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=warehouse"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        warehouses = response.json().get("value", [])

        # Find the warehouse and get the endpoint
        warehouse = next((w for w in warehouses if w["id"] == warehouse_id), None)
        if not warehouse:
            raise Exception("Warehouse not found in workspace.")

        # (Optionally, call GET on warehouse details endpoint for more info)
        detail_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{warehouse_id}"
        detail_response = requests.get(detail_url, headers=headers)
        detail_response.raise_for_status()
        warehouse_details = detail_response.json()

        # Extract SQL endpoint
        # This can vary depending on the API. Look for properties like 'sqlEndpoint', 'connectivityEndpoints', or similar.
        sql_endpoint = (
            warehouse_details.get("connectivityEndpoints", {}).get("sql")
            or warehouse_details.get("properties", {}).get("sqlEndpoint")
            or warehouse_details.get(
                "webUrl"
            )  # fallback; webUrl may contain it, parse if needed
        )

        print(f"SQL Endpoint: {sql_endpoint}")

        return sql_endpoint

    def create_workspace(
        self, workspace_name: str, description: Optional[str] = None
    ) -> str:
        """
        Create a new Fabric workspace and return its ID.

        Args:
            workspace_name: Name of the workspace to create
            description: Optional description for the workspace

        Returns:
            The ID of the created workspace

        Raises:
            Exception: If workspace creation fails
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        # Prepare the workspace creation payload
        create_payload = {
            "displayName": workspace_name,
            "description": description
            or "Workspace created by Ingenious Fabric Accelerator",
        }

        create_url = "https://api.fabric.microsoft.com/v1/workspaces"

        response = requests.post(create_url, json=create_payload, headers=headers)

        if response.status_code == 201:
            workspace_data = response.json()
            workspace_id = workspace_data.get("id")
            if workspace_id:
                return workspace_id
            else:
                raise Exception("Workspace created but ID not found in response")
        elif response.status_code == 409:
            raise Exception(f"Workspace '{workspace_name}' already exists")
        else:
            error_msg = f"Failed to create workspace. Status: {response.status_code}"
            try:
                error_detail = response.json()
                if "error" in error_detail:
                    error_msg += f", Error: {error_detail['error']}"
            except (ValueError, KeyError):
                error_msg += f", Response: {response.text}"

            raise Exception(f"Workspace creation failed: {error_msg}")

    def list_lakehouses(self, workspace_id: str) -> list[dict]:
        """
        List all lakehouses in a workspace.

        Args:
            workspace_id: The ID of the workspace

        Returns:
            List of lakehouse dictionaries with 'id', 'displayName', and other properties
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}/{workspace_id}/items?type=Lakehouse"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json().get("value", [])
        else:
            raise Exception(
                f"Failed to list lakehouses: {response.status_code} - {response.text}"
            )

    def list_warehouses(self, workspace_id: str) -> list[dict]:
        """
        List all warehouses in a workspace.

        Args:
            workspace_id: The ID of the workspace

        Returns:
            List of warehouse dictionaries with 'id', 'displayName', and other properties
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}/{workspace_id}/items?type=Warehouse"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json().get("value", [])
        else:
            raise Exception(
                f"Failed to list warehouses: {response.status_code} - {response.text}"
            )

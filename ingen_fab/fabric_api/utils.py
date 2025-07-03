from typing import Optional

import requests
from azure.identity import DefaultAzureCredential

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils


class FabricApiUtils:
    """
    Utility class for interacting with the Fabric API.
    """

    def __init__(
        self, environment: str, *, credential: Optional[DefaultAzureCredential] = None
    ) -> None:
        self.environment = environment
        self.credential = credential or DefaultAzureCredential()
        self.base_url = "https://api.fabric.microsoft.com/v1/workspaces"
        self.workspace_id = self._get_workspace_id()

    def _get_token(self) -> str:
        scope = "https://api.fabric.microsoft.com/.default"
        token = self.credential.get_token(scope)
        return token.token

    def _get_workspace_id(self) -> str:
        """
        Set the workspace ID for API requests.
        """
        vlu = VariableLibraryUtils(environment=self.environment)
        return vlu.get_workspace_id()

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

import logging
import re
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

    def get_warehouse_id_from_name(
        self, workspace_id: str, warehouse_name: str
    ) -> Optional[str]:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Warehouse"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            items = response.json()
            for item in items.get("value", []):
                if item.get("displayName") == warehouse_name:
                    return item.get("id")
        return None

    def get_warehouse_name_from_id(
        self, workspace_id: str, warehouse_id: str
    ) -> Optional[str]:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{warehouse_id}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            item = response.json()
            if item.get("type") == "Warehouse":
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

    # --- Metadata extraction helpers (Lakehouse SQL endpoint) ---
    def list_workspace_items(self, workspace_id: Optional[str] = None) -> list[dict]:
        """List all items in a workspace with pagination.

        Args:
            workspace_id: Workspace ID; defaults to instance workspace_id

        Returns:
            List of item dicts as returned by Fabric API
        """
        wsid = workspace_id or self.workspace_id
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        items: list[dict] = []
        continuation_token = None
        while True:
            url = f"{self.base_url}/{wsid}/items"
            if continuation_token:
                url += f"?continuationToken={continuation_token}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to list workspace items: {response.status_code} - {response.text}"
                )
            data = response.json()
            items.extend(data.get("value", []))
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break
        return items

    def get_sql_endpoint_id_for_lakehouse(
        self, workspace_id: str, lakehouse_id: str
    ) -> Optional[str]:
        """Resolve SQL endpoint ID for a given Lakehouse.

        Uses the dedicated SQL Endpoints listing endpoint and matches by relationship
        when available, falling back to displayName matching.

        Returns:
            SQL endpoint ID if found, otherwise None
        """
        endpoints = self.list_sql_endpoints(workspace_id)

        # Try to match by explicit relationship first (property name varies by API version)
        for ep in endpoints:
            # print(ep)
            # Common potential keys seen across previews
            related_id = (
                ep.get("lakehouseId")
                or ep.get("lakehouseItemId")
                or ep.get("lakehouseArtifactId")
            )
            if related_id and related_id == lakehouse_id:
                return ep.get("id")

        # Fallback: match by display name equality with the lakehouse (frequently same)
        lakehouse_name = self.get_lakehouse_name_from_id(workspace_id, lakehouse_id)
        if lakehouse_name:
            for ep in endpoints:
                if ep.get("displayName") == lakehouse_name:
                    return ep.get("id")

        return None

    def get_sql_endpoint_id_for_warehouse(
        self, workspace_id: str, warehouse_id: str
    ) -> Optional[str]:
        """Resolve SQL endpoint ID for a given Warehouse.

        Attempts to match by explicit relationship fields or by display name equality.
        """
        endpoints = self.list_sql_endpoints(workspace_id)

        for ep in endpoints:
            related_id = (
                ep.get("warehouseId")
                or ep.get("warehouseItemId")
                or ep.get("warehouseArtifactId")
            )
            if related_id and related_id == warehouse_id:
                return ep.get("id")

        # Fallback: match by display name
        warehouse_name = self.get_warehouse_name_from_id(workspace_id, warehouse_id)
        if warehouse_name:
            for ep in endpoints:
                if ep.get("displayName") == warehouse_name:
                    return ep.get("id")
        return None

    def execute_sql_on_sql_endpoint(
        self,
        *,
        workspace_id: str,
        sql_endpoint_id: str,
        query: str,
        parameters: Optional[dict[str, object]] = None,
    ) -> dict:
        """Execute a SQL query against a Lakehouse SQL endpoint.

        POST /v1/workspaces/{workspaceId}/sqlEndpoints/{sqlEndpointId}/query
        Body: { "query": "...", "parameters": {"name": value, ...} }

        Returns the parsed JSON response.
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/query"
        body: dict[str, object] = {"query": query}
        if parameters:
            # Filter out None values to avoid API errors
            body["parameters"] = {k: v for k, v in parameters.items() if v is not None}

        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            raise Exception(
                f"SQL endpoint query failed: {response.status_code} - {response.text}"
            )
        return response.json()

    def list_sql_endpoints(self, workspace_id: Optional[str] = None) -> list[dict]:
        """List SQL endpoints in a workspace.

        GET /v1/workspaces/{workspaceId}/sqlEndpoints
        """
        wsid = workspace_id or self.workspace_id
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{wsid}/sqlEndpoints"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(
                f"Failed to list SQL endpoints: {response.status_code} - {response.text}"
            )
        data = response.json()
        # Some APIs return {"value": [...]}; normalize to list
        if (
            isinstance(data, dict)
            and "value" in data
            and isinstance(data["value"], list)
        ):
            return data["value"]
        # Or return as-is if already a list
        if isinstance(data, list):
            return data
        return []

    # --- Connectivity helpers ---
    def _parse_server_prefix_from_sql_endpoint(
        self, value: Optional[str]
    ) -> Optional[str]:
        """Extract server prefix (before .datawarehouse.fabric.microsoft.com) from a connection string or URL.

        Examples of inputs observed in Fabric responses:
          - "Server=tcp:myws-abc123.datawarehouse.fabric.microsoft.com,1433;Database=fMyLakehouse;..."
          - "tcp:myws-abc123.datawarehouse.fabric.microsoft.com,1433"
          - "myws-abc123.datawarehouse.fabric.microsoft.com"

        Returns just the prefix: "myws-abc123".
        """
        if not value:
            return None
        try:
            # Normalize
            s = str(value)
            # If it's a full connection string, isolate after 'Server='
            m = re.search(r"Server\s*=\s*([^;]+)", s, flags=re.IGNORECASE)
            if m:
                s = m.group(1)
            # Strip leading tcp:
            s = re.sub(r"^tcp:\s*", "", s, flags=re.IGNORECASE)
            # Now extract host
            # Remove port if present (",1433")
            s = s.split(",")[0]
            # s might now be like "myws-abc123.datawarehouse.fabric.microsoft.com"
            host = s.strip()
            if ".datawarehouse.fabric.microsoft.com" in host:
                return host.split(".datawarehouse.fabric.microsoft.com")[0]
            # If it's already a bare prefix, return as-is
            if re.match(r"^[a-zA-Z0-9\-]+$", host):
                return host
        except Exception:
            return None
        return None

    def get_lakehouse_details(
        self, workspace_id: str, lakehouse_id: str
    ) -> Optional[dict]:
        """Fetch Lakehouse details via the Lakehouse-specific endpoint.

        GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        return None

    def get_sql_server_for_lakehouse(
        self, workspace_id: str, lakehouse_id: str
    ) -> Optional[str]:
        """Return the SQL endpoint connection string for a lakehouse using the Lakehouse API."""
        details = self.get_lakehouse_details(workspace_id, lakehouse_id)
        if not details:
            return None

        # Prefer connectionString from sqlEndpointProperties when available
        sql_endpoint_properties = details.get("properties", {}).get(
            "sqlEndpointProperties", {}
        )
        connection_string = sql_endpoint_properties.get("connectionString")
        if connection_string:
            return connection_string

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

    def get_item_details(self, workspace_id: str, item_id: str) -> Optional[dict]:
        """Get raw item details for any workspace item (Lakehouse/Warehouse/etc)."""
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        return None

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

    def list_lakehouses_api(self, workspace_id: str) -> list[dict]:
        """List all lakehouses via the Lakehouse-specific API.

        Uses: GET /v1/workspaces/{workspaceId}/lakehouses with pagination support.

        Returns a list of lakehouse dicts (id, displayName, connectivityEndpoints, etc.).
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        lakehouses: list[dict] = []
        continuation_token: Optional[str] = None

        while True:
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
            if continuation_token:
                url += f"?continuationToken={continuation_token}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to list lakehouses: {response.status_code} - {response.text}"
                )
            data = response.json() or {}
            vals = data.get("value", []) if isinstance(data, dict) else []
            if not isinstance(vals, list):
                vals = []
            lakehouses.extend(vals)
            continuation_token = (
                data.get("continuationToken") if isinstance(data, dict) else None
            )
            if not continuation_token:
                break

        return lakehouses

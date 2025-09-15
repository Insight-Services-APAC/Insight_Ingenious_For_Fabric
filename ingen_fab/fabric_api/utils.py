import base64
import io
import json
import logging
import re
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

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

    # --- Artifact Download Methods ---

    def download_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        output_path: Optional[Path] = None,
        format: str = "DEFAULT",
    ) -> dict[str, Any]:
        """Download the definition of any Fabric item.

        Args:
            workspace_id: The ID of the workspace
            item_id: The ID of the item to download
            output_path: Optional path to save the downloaded content
            format: The format to download (DEFAULT, TMSL, TMDL, etc.)

        Returns:
            Dictionary containing:
            - 'definition': The item definition content
            - 'format': The format of the definition
            - 'item_type': The type of the item
            - 'display_name': The display name of the item
            - 'files': List of files if the definition contains multiple files
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        # First, get item details to understand what we're downloading
        item_details = self.get_item_details(workspace_id, item_id)
        if not item_details:
            raise Exception(f"Item {item_id} not found in workspace {workspace_id}")

        item_type = item_details.get("type")
        display_name = item_details.get("displayName")
        
        # Use specialized method for semantic models
        if item_type == "SemanticModel":
            return self.download_semantic_model(workspace_id, item_id, output_path, format)

        # Download the item definition
        url = f"{self.base_url}/{workspace_id}/items/{item_id}/getDefinition"
        params = {"format": format}
        
        response = requests.post(url, headers=headers, params=params)
        
        if response.status_code != 200:
            raise Exception(
                f"Failed to download item definition: {response.status_code} - {response.text}"
            )

        definition_data = response.json()
        
        result = {
            "definition": definition_data,
            "format": format,
            "item_type": item_type,
            "display_name": display_name,
            "files": [],
        }

        # Process the definition based on its structure
        if "definition" in definition_data:
            # Extract files if the definition contains multiple parts
            definition = definition_data["definition"]
            if "parts" in definition:
                result["files"] = definition["parts"]
            
            # Save to file if output path is provided
            if output_path:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Determine file extension based on item type and format
                extension = self._get_file_extension(item_type, format)
                file_path = output_path.with_suffix(extension)
                
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(definition_data, f, indent=2)
                
                result["saved_to"] = str(file_path)

        return result

    def download_semantic_model(
        self,
        workspace_id: str,
        semantic_model_id: str,
        output_path: Optional[Path] = None,
        format: str = "TMDL",
    ) -> dict[str, Any]:
        """Download a semantic model (dataset) from Fabric using async operation.

        Args:
            workspace_id: The ID of the workspace
            semantic_model_id: The ID of the semantic model (dataset)
            output_path: Optional path to save the downloaded model
            format: The format to download (TMSL, TMDL, etc.)

        Returns:
            Dictionary containing the downloaded semantic model data and saved files
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

        # Step 1: Initiate the download operation
        url = f"{self.base_url}/{workspace_id}/semanticModels/{semantic_model_id}/getDefinition"
        params = {"format": format}
        
        response = requests.post(url, headers=headers, params=params)
        
        if response.status_code not in [200, 202]:
            raise Exception(
                f"Failed to initiate semantic model download: {response.status_code} - {response.text}"
            )

        # Get the operation location from headers
        operation_location = response.headers.get("Location") or response.headers.get("location")
        if not operation_location:
            # If no location header, it might be a synchronous response
            return self._process_semantic_model_response(response.json(), output_path, semantic_model_id, format, workspace_id)
        
        # Extract operation ID from the location URL
        operation_id = operation_location.split("/operations/")[-1]
        
        # Step 2: Poll for operation completion
        result = self._poll_operation(operation_id)
        
        # Step 3: Process and save the result
        return self._process_semantic_model_response(result, output_path, semantic_model_id, format, workspace_id)

    def _poll_operation(self, operation_id: str, max_wait: int = 300) -> Dict[str, Any]:
        """Poll an async operation until it completes.
        
        Args:
            operation_id: The operation ID to poll
            max_wait: Maximum seconds to wait for completion
            
        Returns:
            The operation result
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
        }
        
        url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
        result_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}/result"
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            # Check operation status
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                operation = response.json()
                status = operation.get("status", "Unknown")
                
                if status == "Succeeded":
                    # Get the result
                    result_response = requests.get(result_url, headers=headers)
                    if result_response.status_code == 200:
                        return result_response.json()
                    else:
                        raise Exception(
                            f"Failed to get operation result: {result_response.status_code} - {result_response.text}"
                        )
                elif status == "Failed":
                    error = operation.get("error", {})
                    raise Exception(f"Operation failed: {error}")
                elif status in ["Running", "NotStarted", "InProgress"]:
                    # Wait before polling again
                    time.sleep(2)
                else:
                    raise Exception(f"Unknown operation status: {status}")
            else:
                raise Exception(
                    f"Failed to check operation status: {response.status_code} - {response.text}"
                )
        
        raise Exception(f"Operation timed out after {max_wait} seconds")

    def _process_semantic_model_response(
        self,
        response_data: Dict[str, Any],
        output_path: Optional[Path],
        semantic_model_id: str,
        format: str,
        workspace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Process the semantic model response and save files.
        
        Args:
            response_data: The response data from the API
            output_path: Optional path to save files
            semantic_model_id: The semantic model ID
            format: The format (TMDL, TMSL, etc.)
            workspace_id: The workspace ID (for fetching display name)
            
        Returns:
            Dictionary with processed results and saved file paths
        """
        result = {
            "semantic_model_id": semantic_model_id,
            "format": format,
            "files": [],
        }
        
        # Get the display name for the semantic model
        display_name = None
        if workspace_id:
            try:
                item_details = self.get_item_details(workspace_id, semantic_model_id)
                if item_details:
                    display_name = item_details.get("displayName")
            except Exception:
                pass  # Fall back to using ID if we can't get display name
        
        # Use display name if available, otherwise fall back to ID
        folder_name = display_name if display_name else semantic_model_id
        # Sanitize folder name for filesystem
        folder_name = re.sub(r'[<>:"|?*\\]', '_', folder_name)
        
        # Determine output directory
        if output_path is None:
            # Use default path: project_dir/fabric_workspace_items/semantic_models/{display_name or model_id}
            output_path = (
                self.project_path
                / "fabric_workspace_items"
                / "semantic_models"
                / folder_name
            )
        else:
            output_path = Path(output_path)
        
        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)
        
        result["display_name"] = display_name
        
        # Process the definition
        if "definition" in response_data:
            definition = response_data["definition"]
            
            if "parts" in definition:
                # Multiple files (typical for TMDL)
                for part in definition["parts"]:
                    file_path = part.get("path", "")
                    payload = part.get("payload", "")
                    
                    if file_path and payload:
                        # Decode base64 payload
                        try:
                            content = base64.b64decode(payload).decode("utf-8")
                        except Exception:
                            # If decoding fails, use raw payload
                            content = payload
                        
                        # Save file
                        full_path = output_path / file_path
                        full_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(full_path, "w", encoding="utf-8") as f:
                            f.write(content)
                        
                        result["files"].append({
                            "path": str(full_path),
                            "relative_path": file_path,
                            "size": len(content),
                        })
            else:
                # Single file (typical for TMSL)
                content = json.dumps(definition, indent=2)
                file_name = f"model.{format.lower()}.json" if format == "TMSL" else "model.json"
                full_path = output_path / file_name
                
                with open(full_path, "w", encoding="utf-8") as f:
                    f.write(content)
                
                result["files"].append({
                    "path": str(full_path),
                    "relative_path": file_name,
                    "size": len(content),
                })
        
        result["output_directory"] = str(output_path)
        result["total_files"] = len(result["files"])
        
        return result

    def download_report(
        self,
        workspace_id: str,
        report_id: str,
        output_path: Optional[Path] = None,
    ) -> bytes:
        """Download a Power BI report (.pbix file) from Fabric.

        Args:
            workspace_id: The ID of the workspace
            report_id: The ID of the report
            output_path: Optional path to save the .pbix file

        Returns:
            The binary content of the .pbix file
        """
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
        }

        url = f"{self.base_url}/{workspace_id}/reports/{report_id}/export"
        response = requests.get(url, headers=headers, stream=True)

        if response.status_code != 200:
            raise Exception(
                f"Failed to download report: {response.status_code} - {response.text}"
            )

        content = response.content

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if not output_path.suffix:
                output_path = output_path.with_suffix(".pbix")
            
            with open(output_path, "wb") as f:
                f.write(content)

        return content

    def download_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        output_path: Optional[Path] = None,
        format: str = "ipynb",
    ) -> dict[str, Any]:
        """Download a notebook from Fabric.

        Args:
            workspace_id: The ID of the workspace
            notebook_id: The ID of the notebook
            output_path: Optional path to save the notebook
            format: The format to download (ipynb, py, etc.)

        Returns:
            Dictionary containing the notebook content and metadata
        """
        definition = self.download_item_definition(
            workspace_id, notebook_id, output_path, "DEFAULT"
        )

        # Extract notebook content from the definition
        if "definition" in definition:
            def_data = definition["definition"]
            if "parts" in def_data:
                for part in def_data["parts"]:
                    if part.get("path", "").endswith(".ipynb") or part.get("path", "").endswith(".py"):
                        # Decode the payload if it's base64 encoded
                        if "payload" in part:
                            import base64
                            content = base64.b64decode(part["payload"]).decode("utf-8")
                            definition["content"] = content
                            
                            if output_path and format == "ipynb":
                                output_path = Path(output_path)
                                output_path.parent.mkdir(parents=True, exist_ok=True)
                                if not output_path.suffix:
                                    output_path = output_path.with_suffix(".ipynb")
                                
                                with open(output_path, "w", encoding="utf-8") as f:
                                    f.write(content)
                                
                                definition["saved_to"] = str(output_path)
                            break

        return definition

    def download_dataflow(
        self,
        workspace_id: str,
        dataflow_id: str,
        output_path: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Download a dataflow definition from Fabric.

        Args:
            workspace_id: The ID of the workspace
            dataflow_id: The ID of the dataflow
            output_path: Optional path to save the dataflow definition

        Returns:
            Dictionary containing the dataflow definition
        """
        return self.download_item_definition(
            workspace_id, dataflow_id, output_path, "DEFAULT"
        )

    def download_kql_database(
        self,
        workspace_id: str,
        kql_database_id: str,
        output_path: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Download a KQL database definition from Fabric.

        Args:
            workspace_id: The ID of the workspace
            kql_database_id: The ID of the KQL database
            output_path: Optional path to save the definition

        Returns:
            Dictionary containing the KQL database definition
        """
        return self.download_item_definition(
            workspace_id, kql_database_id, output_path, "DEFAULT"
        )

    def download_sql_endpoint_definition(
        self,
        workspace_id: str,
        sql_endpoint_id: str,
        output_path: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Download a SQL endpoint definition from Fabric.

        Args:
            workspace_id: The ID of the workspace
            sql_endpoint_id: The ID of the SQL endpoint
            output_path: Optional path to save the definition

        Returns:
            Dictionary containing the SQL endpoint definition
        """
        return self.download_item_definition(
            workspace_id, sql_endpoint_id, output_path, "DEFAULT"
        )

    def download_spark_job_definition(
        self,
        workspace_id: str,
        spark_job_id: str,
        output_path: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Download a Spark job definition from Fabric.

        Args:
            workspace_id: The ID of the workspace
            spark_job_id: The ID of the Spark job definition
            output_path: Optional path to save the definition

        Returns:
            Dictionary containing the Spark job definition
        """
        return self.download_item_definition(
            workspace_id, spark_job_id, output_path, "DEFAULT"
        )

    def download_all_items(
        self,
        workspace_id: str,
        output_dir: Path,
        item_types: Optional[list[str]] = None,
    ) -> dict[str, list[dict]]:
        """Download all items of specified types from a workspace.

        Args:
            workspace_id: The ID of the workspace
            output_dir: Directory to save all downloaded items
            item_types: Optional list of item types to download.
                       If None, downloads all supported types.

        Returns:
            Dictionary with item types as keys and lists of download results as values
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Default supported item types for download
        if item_types is None:
            item_types = [
                "SemanticModel",
                "Report",
                "Notebook",
                "Dataflow",
                "KQLDatabase",
                "SparkJobDefinition",
                "DataPipeline",
            ]

        results = {}
        items = self.list_workspace_items(workspace_id)

        for item in items:
            item_type = item.get("type")
            if item_type not in item_types:
                continue

            item_id = item.get("id")
            display_name = item.get("displayName")
            
            # Create directory for this item type
            type_dir = output_dir / item_type
            type_dir.mkdir(exist_ok=True)
            
            # Create safe filename from display name
            safe_name = re.sub(r'[<>:"|?*]', "_", display_name)
            item_path = type_dir / safe_name

            try:
                if item_type == "Report":
                    # Special handling for reports (.pbix files)
                    self.download_report(workspace_id, item_id, item_path)
                    result = {
                        "id": item_id,
                        "display_name": display_name,
                        "saved_to": str(item_path.with_suffix(".pbix")),
                        "status": "success",
                    }
                elif item_type == "Notebook":
                    # Special handling for notebooks
                    result = self.download_notebook(workspace_id, item_id, item_path)
                    result["status"] = "success"
                else:
                    # Use generic download for other types
                    result = self.download_item_definition(
                        workspace_id, item_id, item_path
                    )
                    result["status"] = "success"

                if item_type not in results:
                    results[item_type] = []
                results[item_type].append(result)
                
            except Exception as e:
                error_result = {
                    "id": item_id,
                    "display_name": display_name,
                    "status": "failed",
                    "error": str(e),
                }
                if item_type not in results:
                    results[item_type] = []
                results[item_type].append(error_result)

        return results

    def _get_file_extension(self, item_type: str, format: str) -> str:
        """Get appropriate file extension based on item type and format.

        Args:
            item_type: The type of Fabric item
            format: The download format

        Returns:
            Appropriate file extension including the dot
        """
        extension_map = {
            "SemanticModel": {
                "TMSL": ".json",
                "TMDL": ".tmdl",
                "DEFAULT": ".json",
            },
            "Report": ".pbix",
            "Notebook": ".ipynb",
            "Dataflow": ".json",
            "KQLDatabase": ".json",
            "SparkJobDefinition": ".json",
            "DataPipeline": ".json",
            "SQLEndpoint": ".json",
        }

        if isinstance(extension_map.get(item_type), dict):
            return extension_map[item_type].get(format, ".json")
        else:
            return extension_map.get(item_type, ".json")

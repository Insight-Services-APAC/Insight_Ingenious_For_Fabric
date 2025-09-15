"""
Dynamic Model Loader for dbt projects.

This module provides runtime loading of dbt manifest and SQL files
without requiring code generation.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DynamicModelLoader:
    """
    Dynamically loads dbt manifest and SQL files at runtime.

    This class provides an alternative to static code generation by reading
    dbt artifacts directly when needed, enabling hot-reload capabilities
    and reducing the need for regeneration.
    """

    def __init__(self, dbt_project_path: Path, cache_sql: bool = True):
        """
        Initialize the dynamic model loader.

        Args:
            dbt_project_path: Path to the dbt project root directory
            cache_sql: Whether to cache loaded SQL statements in memory
        """
        self.project_path = Path(dbt_project_path)
        self.target_dir = self.project_path / "target"
        self.sql_dir = self.target_dir / "sql"
        self.cache_sql = cache_sql

        # Caches
        self._manifest: Optional[Dict[str, Any]] = None
        self._sql_cache: Dict[str, List[str]] = {}
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}

        # Validate paths
        if not self.project_path.exists():
            raise FileNotFoundError(f"DBT project path not found: {self.project_path}")
        if not self.target_dir.exists():
            raise FileNotFoundError(f"DBT target directory not found: {self.target_dir}")
        if not self.sql_dir.exists():
            raise FileNotFoundError(f"DBT SQL directory not found: {self.sql_dir}")

    @property
    def manifest(self) -> Dict[str, Any]:
        """
        Load and return the dbt manifest.

        Returns:
            Dictionary containing the dbt manifest data
        """
        if self._manifest is None:
            self._load_manifest()
        return self._manifest or {}

    def _load_manifest(self) -> None:
        """Load the manifest.json file."""
        manifest_path = self.target_dir / "manifest.json"

        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

        try:
            with manifest_path.open("r", encoding="utf-8") as f:
                self._manifest = json.load(f)
            logger.info(f"Loaded manifest with {len(self._manifest.get('nodes', {}))} nodes")
        except Exception as e:
            raise RuntimeError(f"Failed to load manifest: {e}")

    def reload_manifest(self) -> None:
        """Force reload of the manifest file."""
        self._manifest = None
        self._load_manifest()

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Get node information from the manifest.

        Args:
            node_id: The unique identifier of the node

        Returns:
            Node dictionary if found, None otherwise
        """
        # Check nodes first
        nodes = self.manifest.get("nodes", {})
        if node_id in nodes:
            return nodes[node_id]

        # Check sources
        sources = self.manifest.get("sources", {})
        if node_id in sources:
            return sources[node_id]

        return None

    def get_sql_statements(self, node_id: str, use_cache: bool = True) -> List[str]:
        """
        Load SQL statements for a given node.

        Args:
            node_id: The unique identifier of the node
            use_cache: Whether to use cached SQL if available

        Returns:
            List of SQL statements for the node
        """
        # Check cache first
        if use_cache and self.cache_sql and node_id in self._sql_cache:
            return self._sql_cache[node_id]

        # Find corresponding JSON file
        json_files = list(self.sql_dir.glob("*.json"))

        for json_path in json_files:
            try:
                with json_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                if data.get("node_id") == node_id:
                    sql_statements = data.get("sql", [])

                    # Cache if enabled
                    if self.cache_sql:
                        self._sql_cache[node_id] = sql_statements

                    return sql_statements
            except Exception as e:
                logger.warning(f"Failed to read {json_path}: {e}")
                continue

        raise FileNotFoundError(f"No SQL file found for node_id: {node_id}")

    def get_node_metadata(self, node_id: str) -> Dict[str, Any]:
        """
        Get metadata for a node including SQL file info.

        Args:
            node_id: The unique identifier of the node

        Returns:
            Dictionary containing node metadata
        """
        if node_id in self._metadata_cache:
            return self._metadata_cache[node_id]

        # Find corresponding JSON file
        for json_path in self.sql_dir.glob("*.json"):
            try:
                with json_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                if data.get("node_id") == node_id:
                    metadata = {
                        "node_id": node_id,
                        "session_id": data.get("session_id", ""),
                        "sql_count": len(data.get("sql", [])),
                        "json_file": json_path.name,
                        "json_path": str(json_path),
                    }

                    # Add manifest data if available
                    node_info = self.get_node(node_id)
                    if node_info:
                        metadata.update({
                            "resource_type": node_info.get("resource_type"),
                            "path": node_info.get("path"),
                            "database": node_info.get("database"),
                            "schema": node_info.get("schema"),
                            "name": node_info.get("name"),
                            "alias": node_info.get("alias"),
                            "dependencies": node_info.get("depends_on", {}).get("nodes", []),
                        })

                    self._metadata_cache[node_id] = metadata
                    return metadata

            except Exception as e:
                logger.warning(f"Failed to read {json_path}: {e}")
                continue

        raise FileNotFoundError(f"No metadata found for node_id: {node_id}")

    def get_all_nodes(self) -> List[str]:
        """
        Get list of all node IDs from the manifest.

        Returns:
            List of node IDs (includes both nodes and sources)
        """
        nodes = list(self.manifest.get("nodes", {}).keys())
        sources = list(self.manifest.get("sources", {}).keys())
        return nodes + sources

    def get_nodes_by_type(self, resource_type: str) -> List[str]:
        """
        Get nodes filtered by resource type.

        Args:
            resource_type: Type of resource (model, test, seed, source, etc.)

        Returns:
            List of node IDs matching the resource type
        """
        result = []

        # Check nodes
        nodes = self.manifest.get("nodes", {})
        result.extend([
            node_id for node_id, node_data in nodes.items()
            if node_data.get("resource_type") == resource_type
        ])

        # Check sources if looking for source type
        if resource_type == "source":
            sources = self.manifest.get("sources", {})
            result.extend([
                source_id for source_id, source_data in sources.items()
                if source_data.get("resource_type", "source") == "source"
            ])

        return result

    def get_dependencies(self, node_id: str) -> List[str]:
        """
        Get direct dependencies for a node.

        Args:
            node_id: The unique identifier of the node

        Returns:
            List of node IDs that this node depends on
        """
        node = self.get_node(node_id)
        if node:
            return node.get("depends_on", {}).get("nodes", [])
        return []

    def get_dependents(self, node_id: str) -> List[str]:
        """
        Get nodes that depend on the given node.

        Args:
            node_id: The unique identifier of the node

        Returns:
            List of node IDs that depend on this node
        """
        dependents = []

        # Check all nodes (not sources, as sources don't depend on anything)
        nodes = self.manifest.get("nodes", {})
        for other_id, other_node in nodes.items():
            deps = other_node.get("depends_on", {}).get("nodes", [])
            if node_id in deps:
                dependents.append(other_id)

        return dependents

    def build_dependency_graph(self) -> Dict[str, List[str]]:
        """
        Build complete dependency graph from manifest.

        Returns:
            Dictionary mapping node_id to list of dependencies
        """
        graph = {}

        # Add executable nodes with their dependencies
        nodes = self.manifest.get("nodes", {})
        for node_id, node_data in nodes.items():
            graph[node_id] = node_data.get("depends_on", {}).get("nodes", [])

        # Add sources with no dependencies (they are assumed to exist)
        sources = self.manifest.get("sources", {})
        for source_id in sources:
            graph[source_id] = []

        return graph

    def get_execution_order(self) -> List[List[str]]:
        """
        Calculate execution order based on dependencies.

        Returns:
            List of node ID groups that can be executed in parallel
        """
        from collections import defaultdict, deque

        # Build dependency graph
        graph = self.build_dependency_graph()

        # Filter out sources from execution (they are assumed to exist)
        sources = set(self.manifest.get("sources", {}).keys())
        executable_nodes = {node_id for node_id in graph if node_id not in sources}

        # Calculate in-degree for executable nodes only
        in_degree = defaultdict(int)
        for node_id in executable_nodes:
            deps = graph[node_id]
            # Count only dependencies that are executable (not sources)
            executable_deps = [dep for dep in deps if dep not in sources]
            for dep in executable_deps:
                in_degree[dep] += 1

        # Find executable nodes with no executable dependencies
        # (nodes that only depend on sources or have no dependencies)
        queue = deque([
            node for node in executable_nodes
            if in_degree[node] == 0
        ])
        execution_order = []

        while queue:
            # All nodes in current queue can be executed in parallel
            current_batch = list(queue)
            execution_order.append(current_batch)

            # Process next level
            queue.clear()
            for node in current_batch:
                # Reduce in-degree for dependent executable nodes
                for dependent in self.get_dependents(node):
                    if dependent in executable_nodes:
                        in_degree[dependent] -= 1
                        if in_degree[dependent] == 0:
                            queue.append(dependent)

        return execution_order

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._manifest = None
        self._sql_cache.clear()
        self._metadata_cache.clear()
        logger.info("Cleared all caches")

    def get_python_model(self, node_id: str) -> Optional[str]:
        """
        Check if there's a Python model file for this node.

        Args:
            node_id: The unique identifier of the node

        Returns:
            Python code string if found, None otherwise
        """
        models_python_dir = self.project_path / "models_python"

        if not models_python_dir.exists():
            return None

        # Look for matching Python file
        potential_files = [
            models_python_dir / f"{node_id}.py",
            models_python_dir / f"{node_id.replace('.', '_')}.py",
        ]

        for py_file in potential_files:
            if py_file.exists():
                try:
                    with py_file.open("r", encoding="utf-8") as f:
                        return f.read()
                except Exception as e:
                    logger.warning(f"Failed to read Python model {py_file}: {e}")

        return None
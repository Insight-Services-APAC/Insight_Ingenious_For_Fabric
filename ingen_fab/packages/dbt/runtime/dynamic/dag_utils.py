"""
DAG Utilities for dbt Dynamic Runtime.

This module provides utility functions for exploring and analyzing dbt DAGs,
including node information retrieval, dependency analysis, and visualization helpers.
"""

import json
import logging
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .model_loader import DynamicModelLoader

logger = logging.getLogger(__name__)


class DAGAnalyzer:
    """
    Utility class for analyzing dbt DAGs and extracting information about nodes.
    """

    def __init__(self, dbt_project_path: Path):
        """
        Initialize the DAG analyzer.

        Args:
            dbt_project_path: Path to the dbt project root
        """
        self.project_path = Path(dbt_project_path)
        self.loader = DynamicModelLoader(self.project_path)
        self._dependency_cache: Optional[Dict[str, List[str]]] = None
        self._reverse_dependency_cache: Optional[Dict[str, List[str]]] = None

    def get_dag_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of the DAG.

        Returns:
            Dictionary containing DAG statistics and information
        """
        nodes = self.loader.get_all_nodes()
        dependencies = self.loader.build_dependency_graph()

        # Count nodes by type
        type_counts = defaultdict(int)
        for node_id in nodes:
            node = self.loader.get_node(node_id)
            if node:
                type_counts[node.get("resource_type", "unknown")] += 1

        # Calculate dependency statistics
        dep_counts = [len(deps) for deps in dependencies.values()]
        max_deps = max(dep_counts) if dep_counts else 0
        avg_deps = sum(dep_counts) / len(dep_counts) if dep_counts else 0

        # Find root and leaf nodes
        root_nodes = self.get_root_nodes()
        leaf_nodes = self.get_leaf_nodes()

        # Calculate DAG depth
        depth = self.get_dag_depth()

        # Check for cycles
        has_cycles, cycles = self.detect_cycles()

        return {
            "total_nodes": len(nodes),
            "node_types": dict(type_counts),
            "total_edges": sum(dep_counts),
            "max_dependencies": max_deps,
            "avg_dependencies": round(avg_deps, 2),
            "root_nodes": len(root_nodes),
            "leaf_nodes": len(leaf_nodes),
            "dag_depth": depth,
            "has_cycles": has_cycles,
            "cycle_count": len(cycles),
            "sample_root_nodes": root_nodes[:5],
            "sample_leaf_nodes": leaf_nodes[:5],
        }

    def get_node_info(self, node_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific node.

        Args:
            node_id: The node ID to get information for

        Returns:
            Dictionary containing node details
        """
        node = self.loader.get_node(node_id)
        if not node:
            return {"error": f"Node {node_id} not found"}

        metadata = self.loader.get_node_metadata(node_id)
        dependencies = self.loader.get_dependencies(node_id)
        dependents = self.loader.get_dependents(node_id)

        # Calculate node's position in DAG
        upstream_count = len(self.get_all_upstream_nodes(node_id))
        downstream_count = len(self.get_all_downstream_nodes(node_id))

        return {
            "node_id": node_id,
            "resource_type": node.get("resource_type"),
            "name": node.get("name"),
            "database": node.get("database"),
            "schema": node.get("schema"),
            "alias": node.get("alias"),
            "path": node.get("path"),
            "original_file_path": node.get("original_file_path"),
            "description": node.get("description", ""),
            "tags": node.get("tags", []),
            "sql_count": metadata.get("sql_count", 0),
            "direct_dependencies": dependencies,
            "direct_dependents": dependents,
            "total_upstream": upstream_count,
            "total_downstream": downstream_count,
            "is_root": len(dependencies) == 0,
            "is_leaf": len(dependents) == 0,
        }

    def get_root_nodes(self) -> List[str]:
        """
        Get all root nodes (nodes with no dependencies).

        Returns:
            List of root node IDs
        """
        dependencies = self.loader.build_dependency_graph()
        return [node_id for node_id, deps in dependencies.items() if not deps]

    def get_leaf_nodes(self) -> List[str]:
        """
        Get all leaf nodes (nodes with no dependents).

        Returns:
            List of leaf node IDs
        """
        dependencies = self.loader.build_dependency_graph()
        has_dependents = set()

        for deps in dependencies.values():
            has_dependents.update(deps)

        return [node_id for node_id in dependencies if node_id not in has_dependents]

    def get_all_upstream_nodes(self, node_id: str) -> Set[str]:
        """
        Get all upstream nodes (transitive dependencies) for a node.

        Args:
            node_id: The node to get upstream nodes for

        Returns:
            Set of all upstream node IDs
        """
        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue

            visited.add(current)
            dependencies = self.loader.get_dependencies(current)
            queue.extend(dependencies)

        visited.discard(node_id)  # Remove the node itself
        return visited

    def get_all_downstream_nodes(self, node_id: str) -> Set[str]:
        """
        Get all downstream nodes (transitive dependents) for a node.

        Args:
            node_id: The node to get downstream nodes for

        Returns:
            Set of all downstream node IDs
        """
        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue

            visited.add(current)
            dependents = self.loader.get_dependents(current)
            queue.extend(dependents)

        visited.discard(node_id)  # Remove the node itself
        return visited

    def get_node_lineage(self, node_id: str) -> Dict[str, Any]:
        """
        Get complete lineage information for a node.

        Args:
            node_id: The node to get lineage for

        Returns:
            Dictionary containing upstream and downstream lineage
        """
        upstream = self.get_all_upstream_nodes(node_id)
        downstream = self.get_all_downstream_nodes(node_id)

        # Categorize by resource type
        upstream_by_type = self._categorize_nodes_by_type(upstream)
        downstream_by_type = self._categorize_nodes_by_type(downstream)

        return {
            "node_id": node_id,
            "upstream": {
                "total": len(upstream),
                "nodes": list(upstream),
                "by_type": upstream_by_type,
            },
            "downstream": {
                "total": len(downstream),
                "nodes": list(downstream),
                "by_type": downstream_by_type,
            },
        }

    def get_execution_order(self, target_nodes: Optional[List[str]] = None) -> List[List[str]]:
        """
        Get the execution order for the DAG or specific nodes.

        Args:
            target_nodes: Optional list of target nodes

        Returns:
            List of node batches that can be executed in parallel
        """
        if target_nodes:
            # Get all required nodes including dependencies
            required_nodes = set()
            for target in target_nodes:
                required_nodes.add(target)
                required_nodes.update(self.get_all_upstream_nodes(target))

            # Build sub-graph and filter out sources
            sources = set(self.loader.manifest.get("sources", {}).keys())
            executable_nodes = required_nodes - sources

            dependencies = {
                node: [dep for dep in self.loader.get_dependencies(node)
                      if dep in executable_nodes]
                for node in executable_nodes
            }
        else:
            # Use the updated execution order from loader which handles sources
            return self.loader.get_execution_order()

        # Topological sort with level information
        in_degree = defaultdict(int)
        for node in dependencies:
            for dep in dependencies[node]:
                in_degree[dep] += 1

        # Find nodes with no dependencies
        queue = deque([node for node in dependencies if in_degree[node] == 0])
        execution_order = []

        while queue:
            # All nodes in current queue can run in parallel
            current_batch = list(queue)
            execution_order.append(current_batch)

            queue.clear()
            for node in current_batch:
                # Find nodes that depend on current node
                for other_node, deps in dependencies.items():
                    if node in deps:
                        in_degree[other_node] -= 1
                        if in_degree[other_node] == 0:
                            queue.append(other_node)

        return execution_order

    def get_dag_depth(self) -> int:
        """
        Calculate the maximum depth of the DAG.

        Returns:
            Maximum depth of the DAG
        """
        execution_order = self.get_execution_order()
        return len(execution_order)

    def detect_cycles(self) -> Tuple[bool, List[List[str]]]:
        """
        Detect cycles in the DAG.

        Returns:
            Tuple of (has_cycles, list_of_cycles)
        """
        dependencies = self.loader.build_dependency_graph()
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node_id: str, path: List[str]) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)

            for dep in dependencies.get(node_id, []):
                if dep not in visited:
                    if dfs(dep, path.copy()):
                        return True
                elif dep in rec_stack:
                    # Found cycle
                    cycle_start = path.index(dep) if dep in path else -1
                    if cycle_start >= 0:
                        cycle = path[cycle_start:] + [dep]
                        cycles.append(cycle)
                    return True

            rec_stack.remove(node_id)
            return False

        # Check all nodes
        for node in dependencies:
            if node not in visited:
                dfs(node, [])

        return len(cycles) > 0, cycles

    def get_critical_path(self) -> List[str]:
        """
        Find the critical path (longest path) through the DAG.

        Returns:
            List of node IDs representing the critical path
        """
        # Build reverse dependency graph for easier traversal
        reverse_deps = defaultdict(list)
        dependencies = self.loader.build_dependency_graph()

        for node, deps in dependencies.items():
            for dep in deps:
                reverse_deps[dep].append(node)

        # Find all paths from roots to leaves
        root_nodes = self.get_root_nodes()
        leaf_nodes = set(self.get_leaf_nodes())

        longest_path = []
        max_length = 0

        def find_paths(node: str, current_path: List[str]):
            nonlocal longest_path, max_length

            current_path.append(node)

            if node in leaf_nodes:
                if len(current_path) > max_length:
                    max_length = len(current_path)
                    longest_path = current_path.copy()
            else:
                for next_node in reverse_deps[node]:
                    find_paths(next_node, current_path.copy())

        for root in root_nodes:
            find_paths(root, [])

        return longest_path

    def get_nodes_by_pattern(self, pattern: str) -> List[str]:
        """
        Find nodes matching a pattern in their ID or name.

        Args:
            pattern: Pattern to match (case-insensitive)

        Returns:
            List of matching node IDs
        """
        pattern_lower = pattern.lower()
        matching_nodes = []

        for node_id in self.loader.get_all_nodes():
            if pattern_lower in node_id.lower():
                matching_nodes.append(node_id)
                continue

            node = self.loader.get_node(node_id)
            if node:
                name = node.get("name", "").lower()
                if pattern_lower in name:
                    matching_nodes.append(node_id)

        return matching_nodes

    def get_impact_analysis(self, node_id: str) -> Dict[str, Any]:
        """
        Analyze the impact of changes to a specific node.

        Args:
            node_id: The node to analyze impact for

        Returns:
            Dictionary containing impact analysis
        """
        downstream = self.get_all_downstream_nodes(node_id)
        downstream_by_type = self._categorize_nodes_by_type(downstream)

        # Find critical downstream nodes (nodes with many further dependencies)
        critical_nodes = []
        for downstream_node in downstream:
            further_downstream = len(self.get_all_downstream_nodes(downstream_node))
            if further_downstream > 5:  # Threshold for "critical"
                critical_nodes.append({
                    "node_id": downstream_node,
                    "further_impact": further_downstream
                })

        critical_nodes.sort(key=lambda x: x["further_impact"], reverse=True)

        return {
            "node_id": node_id,
            "total_impact": len(downstream),
            "impacted_by_type": downstream_by_type,
            "critical_downstream": critical_nodes[:10],  # Top 10 critical nodes
            "immediate_impact": len(self.loader.get_dependents(node_id)),
        }

    def _categorize_nodes_by_type(self, node_ids: Set[str]) -> Dict[str, List[str]]:
        """
        Categorize a set of nodes by their resource type.

        Args:
            node_ids: Set of node IDs to categorize

        Returns:
            Dictionary mapping resource type to list of node IDs
        """
        by_type = defaultdict(list)

        for node_id in node_ids:
            node = self.loader.get_node(node_id)
            if node:
                resource_type = node.get("resource_type", "unknown")
                by_type[resource_type].append(node_id)

        return dict(by_type)

    def export_dag_to_json(self, output_path: Optional[Path] = None) -> str:
        """
        Export the DAG structure to a JSON file.

        Args:
            output_path: Optional path to save the JSON file

        Returns:
            JSON string representation of the DAG
        """
        dag_data = {
            "summary": self.get_dag_summary(),
            "nodes": {},
            "edges": [],
        }

        # Add node information
        for node_id in self.loader.get_all_nodes():
            node_info = self.get_node_info(node_id)
            dag_data["nodes"][node_id] = node_info

            # Add edges
            for dep in node_info.get("direct_dependencies", []):
                dag_data["edges"].append({"from": dep, "to": node_id})

        json_str = json.dumps(dag_data, indent=2)

        if output_path:
            output_path = Path(output_path)
            with output_path.open("w", encoding="utf-8") as f:
                f.write(json_str)
            logger.info(f"DAG exported to {output_path}")

        return json_str


def get_dag_info(dbt_project_path: Path) -> Dict[str, Any]:
    """
    Quick function to get DAG information.

    Args:
        dbt_project_path: Path to dbt project

    Returns:
        DAG summary information
    """
    analyzer = DAGAnalyzer(dbt_project_path)
    return analyzer.get_dag_summary()


def get_node_details(dbt_project_path: Path, node_id: str) -> Dict[str, Any]:
    """
    Quick function to get node details.

    Args:
        dbt_project_path: Path to dbt project
        node_id: Node ID to get details for

    Returns:
        Node information dictionary
    """
    analyzer = DAGAnalyzer(dbt_project_path)
    return analyzer.get_node_info(node_id)


def find_nodes(dbt_project_path: Path, pattern: str) -> List[str]:
    """
    Quick function to find nodes by pattern.

    Args:
        dbt_project_path: Path to dbt project
        pattern: Pattern to search for

    Returns:
        List of matching node IDs
    """
    analyzer = DAGAnalyzer(dbt_project_path)
    return analyzer.get_nodes_by_pattern(pattern)


def analyze_impact(dbt_project_path: Path, node_id: str) -> Dict[str, Any]:
    """
    Quick function to analyze impact of a node.

    Args:
        dbt_project_path: Path to dbt project
        node_id: Node to analyze

    Returns:
        Impact analysis dictionary
    """
    analyzer = DAGAnalyzer(dbt_project_path)
    return analyzer.get_impact_analysis(node_id)
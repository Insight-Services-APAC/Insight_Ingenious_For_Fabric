"""
DAG Visualization utilities for dbt Dynamic Runtime.

This module provides functions to visualize DAG structure and relationships.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .dag_utils import DAGAnalyzer

logger = logging.getLogger(__name__)


class DAGVisualizer:
    """
    Utility class for visualizing dbt DAGs.
    """

    def __init__(self, dbt_project_path: Path):
        """
        Initialize the DAG visualizer.

        Args:
            dbt_project_path: Path to the dbt project root
        """
        self.analyzer = DAGAnalyzer(dbt_project_path)

    def generate_mermaid_diagram(
        self,
        target_nodes: Optional[List[str]] = None,
        max_nodes: int = 50,
        include_tests: bool = False
    ) -> str:
        """
        Generate a Mermaid diagram representation of the DAG.

        Args:
            target_nodes: Optional list of target nodes to focus on
            max_nodes: Maximum number of nodes to include
            include_tests: Whether to include test nodes

        Returns:
            Mermaid diagram string
        """
        if target_nodes:
            # Get subgraph for target nodes
            nodes = set()
            for target in target_nodes:
                nodes.add(target)
                nodes.update(self.analyzer.get_all_upstream_nodes(target))
                nodes.update(self.analyzer.get_all_downstream_nodes(target))
        else:
            nodes = set(self.analyzer.loader.get_all_nodes()[:max_nodes])

        # Filter out tests if requested
        if not include_tests:
            nodes = {n for n in nodes if not n.startswith("test.")}

        mermaid = ["graph TD"]

        # Add nodes with styling based on type
        for node_id in nodes:
            node = self.analyzer.loader.get_node(node_id)
            if node:
                resource_type = node.get("resource_type", "unknown")
                name = node.get("name", node_id.split(".")[-1])

                # Style based on resource type
                if resource_type == "model":
                    mermaid.append(f'    {self._sanitize_id(node_id)}["{name}"]')
                elif resource_type == "seed":
                    mermaid.append(f'    {self._sanitize_id(node_id)}(["{name}"])')
                elif resource_type == "test":
                    mermaid.append(f'    {self._sanitize_id(node_id)}{{"{name}"}};')
                else:
                    mermaid.append(f'    {self._sanitize_id(node_id)}["{name}"]')

        # Add edges
        for node_id in nodes:
            dependencies = self.analyzer.loader.get_dependencies(node_id)
            for dep in dependencies:
                if dep in nodes:
                    mermaid.append(f'    {self._sanitize_id(dep)} --> {self._sanitize_id(node_id)}')

        # Add styling
        mermaid.extend([
            "",
            "    %% Styling",
            "    classDef model fill:#e1f5fe,stroke:#01579b,stroke-width:2px;",
            "    classDef seed fill:#f3e5f5,stroke:#4a148c,stroke-width:2px;",
            "    classDef test fill:#fff3e0,stroke:#e65100,stroke-width:2px;",
        ])

        # Apply styles
        for node_id in nodes:
            node = self.analyzer.loader.get_node(node_id)
            if node:
                resource_type = node.get("resource_type", "unknown")
                if resource_type in ["model", "seed", "test"]:
                    mermaid.append(f'    class {self._sanitize_id(node_id)} {resource_type};')

        return "\n".join(mermaid)

    def generate_ascii_tree(
        self,
        node_id: str,
        direction: str = "downstream",
        max_depth: int = 3
    ) -> str:
        """
        Generate an ASCII tree representation for a node and its relationships.

        Args:
            node_id: The root node for the tree
            direction: 'upstream' or 'downstream'
            max_depth: Maximum depth to traverse

        Returns:
            ASCII tree string
        """
        lines = []
        visited = set()

        def build_tree(current_node: str, prefix: str = "", depth: int = 0, is_last: bool = True):
            if depth > max_depth or current_node in visited:
                return

            visited.add(current_node)

            # Get node info
            node = self.analyzer.loader.get_node(current_node)
            if node:
                name = node.get("name", current_node.split(".")[-1])
                resource_type = node.get("resource_type", "unknown")
                node_str = f"{name} ({resource_type})"
            else:
                node_str = current_node

            # Add tree symbols
            if depth == 0:
                lines.append(node_str)
            else:
                connector = "└── " if is_last else "├── "
                lines.append(f"{prefix}{connector}{node_str}")

            # Get related nodes
            if direction == "downstream":
                related = self.analyzer.loader.get_dependents(current_node)
            else:
                related = self.analyzer.loader.get_dependencies(current_node)

            # Add children
            if depth < max_depth:
                extension = "    " if is_last else "│   "
                for i, related_node in enumerate(related):
                    is_last_child = (i == len(related) - 1)
                    new_prefix = prefix + extension if depth > 0 else ""
                    build_tree(related_node, new_prefix, depth + 1, is_last_child)

        build_tree(node_id)
        return "\n".join(lines)

    def generate_execution_timeline(self) -> str:
        """
        Generate a text representation of the execution timeline.

        Returns:
            Timeline string showing parallel execution stages
        """
        execution_order = self.analyzer.get_execution_order()
        lines = ["Execution Timeline (nodes that can run in parallel are grouped):", ""]

        for i, stage in enumerate(execution_order, 1):
            lines.append(f"Stage {i}: ({len(stage)} nodes)")
            for node_id in stage[:10]:  # Limit to 10 nodes per stage for readability
                node = self.analyzer.loader.get_node(node_id)
                if node:
                    name = node.get("name", node_id.split(".")[-1])
                    resource_type = node.get("resource_type", "unknown")
                    lines.append(f"  - {name} ({resource_type})")
                else:
                    lines.append(f"  - {node_id}")

            if len(stage) > 10:
                lines.append(f"  ... and {len(stage) - 10} more")
            lines.append("")

        return "\n".join(lines)

    def generate_dependency_matrix(self, nodes: Optional[List[str]] = None) -> str:
        """
        Generate a dependency matrix for specified nodes.

        Args:
            nodes: List of nodes to include (default: first 10 nodes)

        Returns:
            Matrix string showing dependencies
        """
        if nodes is None:
            all_nodes = self.analyzer.loader.get_all_nodes()
            nodes = all_nodes[:10]  # Default to first 10 nodes

        # Create matrix
        lines = ["Dependency Matrix (→ means row depends on column):", ""]

        # Header
        header = "From \\ To".ljust(20)
        for node in nodes:
            name = node.split(".")[-1][:8]  # Truncate for display
            header += f" {name:^8}"
        lines.append(header)
        lines.append("-" * len(header))

        # Rows
        for from_node in nodes:
            from_name = from_node.split(".")[-1][:18]
            row = f"{from_name:18}"

            dependencies = self.analyzer.loader.get_dependencies(from_node)
            for to_node in nodes:
                if to_node in dependencies:
                    row += "    →    "
                elif from_node == to_node:
                    row += "    •    "
                else:
                    row += "         "
            lines.append(row)

        return "\n".join(lines)

    def _sanitize_id(self, node_id: str) -> str:
        """
        Sanitize node ID for use in diagrams.

        Args:
            node_id: Raw node ID

        Returns:
            Sanitized ID safe for diagram use
        """
        return node_id.replace(".", "_").replace("-", "_")

    def print_dag_stats(self) -> None:
        """Print formatted DAG statistics to console."""
        summary = self.analyzer.get_dag_summary()

        print("\n" + "=" * 60)
        print("DAG STATISTICS")
        print("=" * 60)

        print(f"\nTotal Nodes: {summary['total_nodes']}")
        print(f"Total Edges: {summary['total_edges']}")
        print(f"DAG Depth: {summary['dag_depth']}")
        print(f"Has Cycles: {'Yes' if summary['has_cycles'] else 'No'}")

        print("\nNode Types:")
        for node_type, count in summary['node_types'].items():
            print(f"  - {node_type}: {count}")

        print(f"\nDependency Statistics:")
        print(f"  - Average dependencies per node: {summary['avg_dependencies']}")
        print(f"  - Maximum dependencies: {summary['max_dependencies']}")
        print(f"  - Root nodes (no dependencies): {summary['root_nodes']}")
        print(f"  - Leaf nodes (no dependents): {summary['leaf_nodes']}")

        if summary['sample_root_nodes']:
            print("\nSample Root Nodes:")
            for node in summary['sample_root_nodes']:
                print(f"  - {node}")

        if summary['sample_leaf_nodes']:
            print("\nSample Leaf Nodes:")
            for node in summary['sample_leaf_nodes']:
                print(f"  - {node}")

        print("=" * 60)


def create_dag_report(dbt_project_path: Path, output_path: Optional[Path] = None) -> str:
    """
    Create a comprehensive DAG report.

    Args:
        dbt_project_path: Path to dbt project
        output_path: Optional path to save the report

    Returns:
        Report string
    """
    visualizer = DAGVisualizer(dbt_project_path)
    analyzer = visualizer.analyzer

    report_lines = [
        "# DBT DAG Analysis Report",
        "=" * 60,
        ""
    ]

    # Summary
    summary = analyzer.get_dag_summary()
    report_lines.extend([
        "## Summary",
        f"- Total Nodes: {summary['total_nodes']}",
        f"- Total Edges: {summary['total_edges']}",
        f"- DAG Depth: {summary['dag_depth']}",
        f"- Has Cycles: {'Yes' if summary['has_cycles'] else 'No'}",
        ""
    ])

    # Node Types
    report_lines.append("## Node Types")
    for node_type, count in summary['node_types'].items():
        report_lines.append(f"- {node_type}: {count}")
    report_lines.append("")

    # Critical Path
    critical_path = analyzer.get_critical_path()
    if critical_path:
        report_lines.extend([
            "## Critical Path",
            f"Length: {len(critical_path)} nodes",
            "Path:"
        ])
        for i, node in enumerate(critical_path[:10], 1):
            report_lines.append(f"{i}. {node}")
        if len(critical_path) > 10:
            report_lines.append(f"... and {len(critical_path) - 10} more")
        report_lines.append("")

    # Execution Timeline
    report_lines.extend([
        "## Execution Timeline",
        visualizer.generate_execution_timeline(),
        ""
    ])

    # High Impact Nodes
    report_lines.append("## High Impact Nodes (nodes with many downstream dependencies)")
    high_impact_nodes = []
    for node_id in analyzer.loader.get_all_nodes()[:50]:  # Check first 50 nodes
        downstream_count = len(analyzer.get_all_downstream_nodes(node_id))
        if downstream_count > 5:
            high_impact_nodes.append((node_id, downstream_count))

    high_impact_nodes.sort(key=lambda x: x[1], reverse=True)
    for node_id, impact in high_impact_nodes[:10]:
        report_lines.append(f"- {node_id}: impacts {impact} downstream nodes")
    report_lines.append("")

    report = "\n".join(report_lines)

    if output_path:
        output_path = Path(output_path)
        with output_path.open("w", encoding="utf-8") as f:
            f.write(report)
        logger.info(f"Report saved to {output_path}")

    return report
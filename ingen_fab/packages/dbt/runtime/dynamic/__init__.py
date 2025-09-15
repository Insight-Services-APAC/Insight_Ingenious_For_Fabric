"""
Dynamic Runtime for dbt Projects.

This package provides dynamic loading and execution of dbt models
without requiring static code generation.
"""

from .dag_executor import DynamicDAGExecutor
from .dag_utils import (
    DAGAnalyzer,
    analyze_impact,
    find_nodes,
    get_dag_info,
    get_node_details,
)
from .model_loader import DynamicModelLoader
from .sql_executor import DynamicSQLExecutor
from .visualization import DAGVisualizer, create_dag_report

__all__ = [
    # Core components
    "DynamicModelLoader",
    "DynamicSQLExecutor",
    "DynamicDAGExecutor",
    # Analysis utilities
    "DAGAnalyzer",
    "get_dag_info",
    "get_node_details",
    "find_nodes",
    "analyze_impact",
    # Visualization
    "DAGVisualizer",
    "create_dag_report",
]

__version__ = "1.0.0"
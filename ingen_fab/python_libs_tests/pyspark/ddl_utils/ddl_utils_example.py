"""
Example usage of the DDL Utils interface.

This example demonstrates how to use the DDLUtilsInterface to ensure type safety
and consistent implementation across different DDL utilities.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ingen_fab.python_libs.interfaces.ddl_utils_interface import DDLUtilsInterface


def example_usage(ddl_utility: DDLUtilsInterface) -> None:
    """
    Example function showing how to use DDL utilities with type hints.
    
    Args:
        ddl_utility: Any implementation of DDLUtilsInterface
    """
    # Print the current execution log
    ddl_utility.print_log()
    
    # Check if a script has already been executed
    script_id = "example-script-001"
    if ddl_utility.check_if_script_has_run(script_id):
        print(f"Script {script_id} has already been executed")
    else:
        print(f"Script {script_id} has not been executed yet")
    
    # Example of using run_once
    def create_example_table() -> None:
        print("Creating example table...")
        # Table creation logic would go here
        pass
    
    # Run the function once with automatic GUID generation
    ddl_utility.run_once(
        work_fn=create_example_table,
        object_name="example_table",
        guid=None  # Will auto-generate from function source
    )
    
    # Run another function with explicit GUID
    ddl_utility.run_once(
        work_fn=lambda: print("Another operation"),
        object_name="another_operation", 
        guid="custom-guid-123"
    )

    # Print the execution log again to see updates
    ddl_utility.print_log()


def get_ddl_utility(workspace_id: str, lakehouse_id: str) -> DDLUtilsInterface:
    """
    Factory function to create a DDL utility instance.
    
    Args:
        workspace_id: The workspace identifier
        lakehouse_id: The lakehouse identifier
        
    Returns:
        DDLUtilsInterface: An instance implementing the interface
    """
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    
    return ddl_utils(workspace_id, lakehouse_id)


if __name__ == "__main__":
    # Example usage
    workspace_id = "example-workspace-123"
    lakehouse_id = "example-lakehouse-456"
    
    ddl_util = get_ddl_utility(workspace_id, lakehouse_id)
    example_usage(ddl_util)

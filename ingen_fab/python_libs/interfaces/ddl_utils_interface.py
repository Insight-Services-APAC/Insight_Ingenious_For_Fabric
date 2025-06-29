from __future__ import annotations

from typing import Callable, Protocol

from pyspark.sql.types import StructType


class DDLUtilsInterface(Protocol):
    """
    Interface for DDL utilities providing execution tracking and script management.
    
    This interface defines the contract for utilities that handle DDL script execution,
    logging, and preventing duplicate script runs in a lakehouse environment.
    """

    def __init__(self, target_workspace_id: str, target_lakehouse_id: str) -> None:
        """
        Initialize the DDL utilities with workspace and lakehouse identifiers.
        
        Args:
            target_workspace_id: The workspace identifier
            target_lakehouse_id: The lakehouse identifier
        """
        ...

    @staticmethod
    def execution_log_schema() -> StructType:
        """
        Return the schema for the execution log table.
        
        Returns:
            StructType: Schema definition for the execution log table
        """
        ...

    def print_log(self) -> None:
        """
        Display the execution log table showing all script executions.
        """
        ...

    def check_if_script_has_run(self, script_id: str) -> bool:
        """
        Check if a script has already been successfully executed.
        
        Args:
            script_id: Unique identifier for the script
            
        Returns:
            bool: True if script has been successfully executed, False otherwise
        """
        ...

    def print_skipped_script_execution(self, guid: str, object_name: str) -> None:
        """
        Print a message indicating that script execution was skipped.
        
        Args:
            guid: The script's unique identifier
            object_name: The name of the database object
        """
        ...

    def write_to_execution_log(self, object_guid: str, object_name: str, script_status: str) -> None:
        """
        Write an execution entry to the log table.
        
        Args:
            object_guid: Unique identifier for the script
            object_name: Name of the database object
            script_status: Status of the execution (Success/Failure)
        """
        ...

    def run_once(self, work_fn: Callable[[], None], object_name: str, guid: str | None = None) -> None:
        """
        Execute a function exactly once, tracked by GUID.
        
        If the script has already been successfully executed, it will be skipped.
        If no GUID is provided, one will be generated from the function's source code.
        
        Args:
            work_fn: The function to execute
            object_name: Name of the database object being created/modified
            guid: Optional unique identifier for the script. If None, will be auto-generated
        """
        ...

    def initialise_ddl_script_executions_table(self) -> None:
        """
        Initialize the execution log table if it doesn't exist.
        
        Creates the table structure for tracking script executions.
        """
        ...

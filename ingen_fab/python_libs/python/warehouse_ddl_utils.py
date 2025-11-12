"""
Warehouse DDL utilities for Microsoft Fabric
Provides DDL execution tracking and run-once functionality for warehouses
"""

import hashlib
import inspect
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any
import pyodbc


class WarehouseDDLUtils:
    """
    Warehouse DDL utilities with run-once tracking functionality.
    Similar to ddl_utils for lakehouses but adapted for warehouse environments.
    """
    
    def __init__(self, warehouse_connection, target_warehouse_id: str, target_workspace_id: str):
        """
        Initialize warehouse DDL utilities.
        
        Args:
            warehouse_connection: FabricWarehouseConnection instance
            target_warehouse_id: Target warehouse ID
            target_workspace_id: Target workspace ID
        """
        self.warehouse_connection = warehouse_connection
        self.target_warehouse_id = target_warehouse_id
        self.target_workspace_id = target_workspace_id
        self.execution_log_table_name = "ddl_script_executions"
        self.execution_log_schema = "dbo"
        self.logger = logging.getLogger(__name__)
        
        # Initialize execution tracking table
        self.initialize_execution_log_table()
    
    def get_execution_log_table_ddl(self) -> str:
        """
        Get DDL for creating the execution log table.
        Fabric warehouse compatible - no DEFAULT constraints.
        
        Returns:
            str: DDL statement for creating the execution log table
        """
        return f"""
        IF NOT EXISTS (SELECT * FROM sys.tables t 
                      JOIN sys.schemas s ON t.schema_id = s.schema_id 
                      WHERE t.name = '{self.execution_log_table_name}' AND s.name = '{self.execution_log_schema}')
        BEGIN
            CREATE TABLE [{self.execution_log_schema}].[{self.execution_log_table_name}] (
                object_guid VARCHAR(128) NOT NULL,
                object_name VARCHAR(255) NOT NULL,
                script_status VARCHAR(50) NOT NULL,
                execution_timestamp DATETIME2(3) NOT NULL,
                target_warehouse_id VARCHAR(128) NOT NULL,
                target_workspace_id VARCHAR(128) NOT NULL
            );
        END
        """
    
    def initialize_execution_log_table(self) -> None:
        """
        Initialize the execution log table if it doesn't exist.
        """
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        
        if not self.check_if_script_has_run(script_id=guid):
            try:
                ddl_sql = self.get_execution_log_table_ddl()
                success = self.warehouse_connection.execute_ddl(
                    ddl_sql, 
                    "Initialize execution log table"
                )
                
                if success:
                    self.write_to_execution_log(
                        object_guid=guid,
                        object_name=object_name,
                        script_status="Success"
                    )
                    self.logger.info("Execution log table initialized successfully")
                else:
                    self.logger.error("Failed to initialize execution log table")
            except Exception as e:
                self.logger.error(f"Error initializing execution log table: {str(e)}")
        else:
            self.logger.debug("Execution log table already initialized")
    
    def check_if_script_has_run(self, script_id: str) -> bool:
        """
        Check if a script has already been executed.
        
        Args:
            script_id: Unique identifier for the script
            
        Returns:
            bool: True if script has been executed, False otherwise
        """
        if not self.warehouse_connection.can_execute:
            return False
        
        # First check if the execution log table exists
        table_exists = self.warehouse_connection.check_if_table_exists(
            self.execution_log_table_name, 
            self.execution_log_schema
        )
        
        if not table_exists:
            # If table doesn't exist, script hasn't run
            return False
            
        query = f"""
        SELECT COUNT(*) as count
        FROM [{self.execution_log_schema}].[{self.execution_log_table_name}]
        WHERE object_guid = ? 
        AND target_warehouse_id = ? 
        AND target_workspace_id = ?
        AND script_status = 'Success'
        """
        
        try:
            result = self.warehouse_connection.execute_query(
                query, 
                params=[script_id, self.target_warehouse_id, self.target_workspace_id]
            )
            
            if result and len(result) > 0:
                count = result[0][0]
                has_run = count > 0
                self.logger.debug(f"Script {script_id} has_run: {has_run}")
                return has_run
            else:
                return False
                
        except Exception as e:
            self.logger.warning(f"Error checking script execution status: {str(e)}")
            return False
    
    def write_to_execution_log(self, object_guid: str, object_name: str, script_status: str) -> None:
        """
        Write execution record to the log table.
        
        Args:
            object_guid: Unique identifier for the executed object
            object_name: Name of the executed object
            script_status: Status of execution (Success/Failure)
        """
        if not self.warehouse_connection.can_execute:
            self.logger.warning("Cannot write to execution log - warehouse connection not available")
            return
            
        insert_sql = f"""
        INSERT INTO [{self.execution_log_schema}].[{self.execution_log_table_name}]
        (object_guid, object_name, script_status, execution_timestamp, target_warehouse_id, target_workspace_id)
        VALUES (?, ?, ?, GETDATE(), ?, ?)
        """
        
        try:
            connection = self.warehouse_connection.get_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute(insert_sql, [
                    object_guid,
                    object_name,
                    script_status,
                    self.target_warehouse_id,
                    self.target_workspace_id
                ])
                connection.commit()
                cursor.close()
                connection.close()
                self.logger.debug(f"Execution log written: {object_guid} - {script_status}")
            else:
                self.logger.error("Could not get warehouse connection for logging")
                
        except Exception as e:
            self.logger.error(f"Error writing to execution log: {str(e)}")
    
    def run_once(self, work_fn: callable, object_name: str, guid: Optional[str] = None):
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
        
        Args:
            work_fn: Function to execute
            object_name: Name of the object being created/modified
            guid: Optional unique identifier (auto-generated if None)
        """
        
        # 1. Auto-derive GUID if not provided
        if guid is None:
            try:
                src = inspect.getsource(work_fn)
            except (OSError, TypeError):
                raise ValueError(
                    "work_fn must be a named function defined at top-level"
                )
            # compute SHA256 and take first 12 hex chars
            digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
            guid = digest
            self.logger.info(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(script_id=guid):
            try:
                work_fn()
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Success"
                )
                self.logger.info(f"Successfully executed work_fn for guid={guid}")
            except Exception as e:
                error_message = (
                    f"Error in work_fn for {guid}: {e}\n{traceback.format_exc()}"
                )
                self.logger.error(error_message)

                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Failure"
                )
                # Print the error message to stderr and raise a RuntimeError
                import sys
                print(error_message, file=sys.stderr)
                raise RuntimeError(error_message) from e
        else:
            self.logger.info(
                f"Skipping {guid}:{object_name} as the script has already run on workspace_id:"
                f"{self.target_workspace_id} | warehouse_id {self.target_warehouse_id}"
            )
    
    def print_log(self) -> None:
        """
        Print the execution log for this warehouse.
        """
        if not self.warehouse_connection.can_execute:
            print("⚠️ Cannot display execution log - warehouse connection not available")
            return
            
        query = f"""
        SELECT object_guid, object_name, script_status, execution_timestamp
        FROM [{self.execution_log_schema}].[{self.execution_log_table_name}]
        WHERE target_warehouse_id = ? AND target_workspace_id = ?
        ORDER BY execution_timestamp DESC
        """
        
        try:
            result = self.warehouse_connection.execute_query(
                query, 
                params=[self.target_warehouse_id, self.target_workspace_id]
            )
            
            if result and len(result) > 0:
                print("\n📇 DDL EXECUTION LOG")
                print("=" * 80)
                print(f"{'GUID':<20} {'Object Name':<30} {'Status':<10} {'Timestamp':<20}")
                print("-" * 80)
                
                for row in result:
                    guid = row[0][:16] + "..." if len(row[0]) > 16 else row[0]
                    name = row[1][:28] + "..." if len(row[1]) > 28 else row[1]
                    status = row[2]
                    timestamp = row[3].strftime("%Y-%m-%d %H:%M:%S") if row[3] else "Unknown"
                    print(f"{guid:<20} {name:<30} {status:<10} {timestamp:<20}")
                print("=" * 80)
            else:
                print("📇 No DDL execution records found for this warehouse")
                
        except Exception as e:
            print(f"⚠️ Error retrieving execution log: {str(e)}")
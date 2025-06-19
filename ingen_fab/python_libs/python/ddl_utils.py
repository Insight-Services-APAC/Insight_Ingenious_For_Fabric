# { "depends_on": "warehouse_utils" }

from datetime import datetime
from typing import List, Callable, Optional
import inspect, hashlib
from dataclasses import dataclass, asdict
import notebookutils  # type: ignore # noqa: F401


class ddl_utils:
    def __init__(self, target_workspace_id, target_warehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id
        self.execution_log_table_schema = "log"
        self.execution_log_table_name = "ddl_script_executions"
        self.warehouse_utils = warehouse_utils(target_workspace_id, target_warehouse_id)
        self.initialise_ddl_script_executions_table()
        

    def execution_log_schema():
        pass

    def print_log(self):
        conn = self.warehouse_utils.get_connection()
        query = f"SELECT * FROM [{self.execution_log_table_schema}].[{self.execution_log_table_name}]"
        df = self.warehouse_utils.execute_query(conn, query)
        display(df)

    def check_if_script_has_run(self, script_id) -> bool:
        conn = self.warehouse_utils.get_connection()
        query = f"""
        SELECT *
        FROM [{self.execution_log_table_schema}].[{self.execution_log_table_name}]
        WHERE script_id = '{script_id}'
        AND execution_status = 'success'
        """
        df = self.warehouse_utils.execute_query(conn, query)
        
        if df:
            if len(df) == 0:
                # print("matched:", matched)
                return False
            else:
                return True
        else: 
            return False

    def print_skipped_script_execution(self, guid, object_name):
        print(
            f"skipping {guid}:{object_name} as the script has already run on workspace_id:" \
            f"{self.target_workspace_id} | warehouse_id {self.target_warehouse_id}"
        )

    def write_to_execution_log(self, object_guid, object_name, script_status):
        conn = self.warehouse_utils.get_connection()
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_query = f"""
        INSERT INTO [{self.execution_log_table_schema}].[{self.execution_log_table_name}]
        (script_id, script_name, execution_status, update_date)
        VALUES ('{object_guid}', '{object_name}', '{script_status}', '{current_timestamp}')
        """
        self.warehouse_utils.execute_query(conn, insert_query)

    def run_once(
        self,
        work_fn: callable,
        object_name: str,
        guid: str 
    ):
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
        """
        # 1. Auto-derive GUID if not provided
        if guid is None:
            try:
                src = inspect.getsource(work_fn)
            except (OSError, TypeError):
                raise ValueError("work_fn must be a named function defined at top-level")
            # compute SHA256 and take first 12 hex chars
            digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
            guid = digest
            print(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(guid):
            try:
                work_fn()
                self.write_to_execution_log(object_guid=guid,
                                            object_name=object_name,
                                            script_status="Success"
                                            )
            except Exception as e:
                print(f"Error in work_fn for {guid}: {e}")
                self.write_to_execution_log(object_guid=guid,
                                            object_name=object_name,
                                            script_status="Failure"
                                            )
                raise
        else:
            self.print_skipped_script_execution(guid, object_name)

    def initialise_ddl_script_executions_table(self):
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        
        conn = self.warehouse_utils.get_connection()
        
        # Check if table exists
        check_query = f"""
        SELECT COUNT(*) as count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{self.execution_log_table_schema}'
        AND TABLE_NAME = '{self.execution_log_table_name}'
        """
        
        result = self.warehouse_utils.execute_query(conn, check_query)
        table_exists = result.iloc[0]['count'] > 0
        
        if not table_exists:
            # Create the table
            create_table_query = f"""
            CREATE TABLE [{self.execution_log_table_schema}].[{self.execution_log_table_name}] (
            script_id VARCHAR(255) NOT NULL,
            script_name VARCHAR(255) NOT NULL,
            execution_status VARCHAR(50) NOT NULL,
            update_date DATETIME2(0) NOT NULL
            )
            """
            self.warehouse_utils.execute_query(conn, create_table_query)
            self.write_to_execution_log(object_guid=guid, object_name=object_name, script_status="Success")
        else:
            print(f"Skipping {object_name} as it already exists")

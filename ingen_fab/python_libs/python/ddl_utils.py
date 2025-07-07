# { "depends_on": "warehouse_utils" }

import hashlib
import inspect
from datetime import datetime

from .warehouse_utils import warehouse_utils


class ddl_utils:
    """Run DDL scripts once and track execution in a warehouse table."""

    def __init__(self, target_workspace_id: str, target_warehouse_id: str) -> None:
        super().__init__()
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id
        self.execution_log_table_schema = "log"
        self.execution_log_table_name = "ddl_script_executions"
        self.warehouse_utils = warehouse_utils(
            target_workspace_id=target_workspace_id,
            target_warehouse_id=target_warehouse_id,
            dialect="sqlserver",
            connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Encrypt=no;UID=sa;PWD=tester321!"
        )
        self.initialise_ddl_script_executions_table()

    def execution_log_schema():
        pass

    def print_log(self):
        conn = self.warehouse_utils.get_connection()
        query = f"SELECT * FROM [{self.execution_log_table_schema}].[{self.execution_log_table_name}]"
        df = self.warehouse_utils.execute_query(conn=conn, query=query)
        print(df)

    def check_if_script_has_run(self, script_id) -> bool:
        conn = self.warehouse_utils.get_connection()
        query = f"""
        SELECT *
        FROM [{self.execution_log_table_schema}].[{self.execution_log_table_name}]
        WHERE script_id = '{script_id}'
        AND execution_status = 'success'
        """
        df = self.warehouse_utils.execute_query(conn=conn, query=query)

        if df is not None:
            if len(df) == 0:
                # print("matched:", matched)
                return False
            else:
                return True
        else:
            return False

    def print_skipped_script_execution(self, guid, object_name):
        print(
            f"skipping {guid}:{object_name} as the script has already run on workspace_id:"
            f"{self.target_workspace_id} | warehouse_id {self.target_warehouse_id}"
        )

    def write_to_execution_log(self, object_guid, object_name, script_status):
        conn = self.warehouse_utils.get_connection()
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_query = f"""
        INSERT INTO [{self.execution_log_table_schema}].[{self.execution_log_table_name}]
        (script_id, script_name, execution_status, update_date)
        VALUES ('{object_guid}', '{object_name}', '{script_status}', '{current_timestamp}')
        """
        self.warehouse_utils.execute_query(conn=conn, query=insert_query)

    def run_once(self, work_fn: callable, object_name: str, guid: str):
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
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
            print(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(script_id=guid):
            try:
                work_fn()
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Success"
                )
            except Exception as e:
                print(f"Error in work_fn for {guid}: {e}")
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Failure"
                )
                raise
        else:
            self.print_skipped_script_execution(guid=guid, object_name=object_name)

    def initialise_ddl_script_executions_table(self):
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        conn = self.warehouse_utils.get_connection()
        table_exists = self.warehouse_utils.check_if_table_exists(
            table_name=self.execution_log_table_name,
            schema_name=self.execution_log_table_schema,
        )

        if not table_exists:
            self.warehouse_utils.create_schema_if_not_exists(
                schema_name=self.execution_log_table_schema
            )
            # Create the table
            create_table_query = f"""
            CREATE TABLE [{self.execution_log_table_schema}].[{self.execution_log_table_name}] (
            script_id VARCHAR(255) NOT NULL,
            script_name VARCHAR(255) NOT NULL,
            execution_status VARCHAR(50) NOT NULL,
            update_date DATETIME2(0) NOT NULL
            )
            """
            self.warehouse_utils.execute_query(conn=conn, query=create_table_query)
            self.write_to_execution_log(
                object_guid=guid, object_name=object_name, script_status="Success"
            )
        else:
            print(f"Skipping {object_name} as it already exists")

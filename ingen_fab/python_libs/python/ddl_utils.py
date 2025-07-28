# { "depends_on": "warehouse_utils" }

import hashlib
import inspect
import logging
import traceback
from datetime import datetime
from typing import Any, Optional

from ingen_fab.python_libs.python.sql_templates import SQLTemplates
from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


class ddl_utils:
    """Run DDL scripts once and track execution in a warehouse table."""

    def __init__(self, target_workspace_id: str, target_warehouse_id: str, notebookutils: Optional[Any] = None) -> None:
        super().__init__(
            target_datastore_id=target_warehouse_id,
            target_workspace_id=target_workspace_id,
        )
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id
        self.execution_log_table_schema = "log"
        self.execution_log_table_name = "ddl_script_executions"
        self.warehouse_utils = warehouse_utils(
            target_workspace_id=target_workspace_id,
            target_warehouse_id=target_warehouse_id,
            notebookutils=notebookutils
        )
        # Use the same notebook utils instance as warehouse_utils
        self.notebook_utils = self.warehouse_utils.notebook_utils
        # Initialize SQL templates
        self.sql = SQLTemplates(dialect="fabric")
        self.initialise_ddl_script_executions_table()

    def execution_log_schema():
        pass

    def print_log(self):
        conn = self.warehouse_utils.get_connection()
        query = self.sql.render("read_table", 
                               schema_name=self.execution_log_table_schema,
                               table_name=self.execution_log_table_name)
        df = self.warehouse_utils.execute_query(conn=conn, query=query)
        self.notebook_utils.display(df)

    def check_if_script_has_run(self, script_id) -> bool:
        conn = self.warehouse_utils.get_connection()
        query = self.sql.render("check_script_executed",
                               schema_name=self.execution_log_table_schema,
                               table_name=self.execution_log_table_name,
                               script_id=script_id)
        df = self.warehouse_utils.execute_query(conn=conn, query=query)
        if df is not None and not df.empty:
            if int(df.iloc[0, 0]) > 0:
                return True
            else:
                return False
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
        insert_query = self.sql.render("insert_ddl_log",
                                      schema_name=self.execution_log_table_schema,
                                      table_name=self.execution_log_table_name,
                                      script_id=object_guid,
                                      script_name=object_name,
                                      execution_status=script_status,
                                      update_date=current_timestamp)
        self.warehouse_utils.execute_query(conn=conn, query=insert_query)

    def run_once(self, work_fn: callable, object_name: str, guid: str):
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
        """
        logger = logging.getLogger(__name__)

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
            logger.info(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(script_id=guid):
            try:
                work_fn()
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Success"
                )
                logger.info(f"Successfully executed work_fn for guid={guid}")
            except Exception as e:
                error_message = f"Error in work_fn for {guid}: {e}\n{traceback.format_exc()}"
                logger.error(error_message)

                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Failure"
                )
                # Print the error message to stderr and raise a RuntimeError
                import sys
                print(error_message, file=sys.stderr)
                raise RuntimeError(error_message) from e
        else:
            logger.info(
                f"Skipping {guid}:{object_name} as the script has already run on workspace_id:"
                f"{self.target_workspace_id} | warehouse_id {self.target_warehouse_id}"
            )

    def initialise_ddl_script_executions_table(self):
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        conn = self.warehouse_utils.get_connection()
        table_exists = self.warehouse_utils.check_if_table_exists(
            table_name=self.execution_log_table_name,
            schema_name=self.execution_log_table_schema,
        )

        if not table_exists:
            try:
                self.warehouse_utils.create_schema_if_not_exists(
                    schema_name=self.execution_log_table_schema
                )
                # Create the table using SQL template
                create_table_query = self.sql.render("create_ddl_log_table",
                                                    schema_name=self.execution_log_table_schema,
                                                    table_name=self.execution_log_table_name)
                self.warehouse_utils.execute_query(conn=conn, query=create_table_query)
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Success"
                )
            except Exception as e:
                # Check again if table exists - it might have been created by another process
                table_exists_now = self.warehouse_utils.check_if_table_exists(
                    table_name=self.execution_log_table_name,
                    schema_name=self.execution_log_table_schema,
                )
                if table_exists_now:
                    print(f"Table {object_name} was created by another process")
                else:
                    raise e
        else:
            print(f"Skipping {object_name} as it already exists")

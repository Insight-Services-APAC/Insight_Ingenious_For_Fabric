from __future__ import annotations

import hashlib
import inspect
import logging
import traceback
from datetime import datetime

from pyspark.sql import SparkSession  # type: ignore # noqa: F401
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingen_fab.python_libs.interfaces.ddl_utils_interface import DDLUtilsInterface
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


class ddl_utils(DDLUtilsInterface):
    def __init__(self, target_workspace_id: str, target_lakehouse_id: str, spark: SparkSession = None) -> None:
        """
        Initializes the DDLUtils class with the target workspace and lakehouse IDs.
        """
        super().__init__(
            target_datastore_id=target_lakehouse_id,
            target_workspace_id=target_workspace_id,
        )
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id
        self.lakehouse_utils: lakehouse_utils = lakehouse_utils(target_workspace_id, target_lakehouse_id, spark=spark)
        self.execution_log_table_name = "ddl_script_executions"
        self.initialise_ddl_script_executions_table()

    @staticmethod
    def execution_log_schema() -> StructType:
        return StructType(
            [
                StructField("script_id", StringType(), nullable=False),
                StructField("script_name", StringType(), nullable=False),
                StructField("execution_status", StringType(), nullable=False),
                StructField("update_date", TimestampType(), nullable=False),
            ]
        )

    def print_log(self) -> None:
        df = self.lakehouse_utils.spark.read.format("delta").load(
            f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
        )
        if self.lakehouse_utils.spark_version == "local":
            df.show()
        else:
            display(df)  # type: ignore # noqa: F821

    def check_if_script_has_run(self, script_id: str) -> bool:
        from pyspark.sql.functions import col

        df = self.lakehouse_utils.spark.read.format("delta").load(
            f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
        )
        # display(df)
        # Build filter condition
        cond = col("script_id") == script_id
        status = "Success"
        if status is not None:
            cond = cond & (col("execution_status") == status)
        matched = df.filter(cond).limit(1).take(1)
        if (len(matched)) == 0:
            # print("matched:", matched)
            return False
        else:
            return True

    def print_skipped_script_execution(self, guid: str, object_name: str) -> None:
        print(
            f"skipping {guid}:{object_name} as the script has already run on workspace_id:"
            f"{self.target_workspace_id} | lakehouse_id {self.target_lakehouse_id}"
        )

    def write_to_execution_log(
        self, object_guid: str, object_name: str, script_status: str
    ) -> None:
        data = [(object_guid, object_name, script_status, datetime.now())]
        new_df = self.lakehouse_utils.spark.createDataFrame(
            data=data, schema=ddl_utils.execution_log_schema()
        )
        new_df.write.format("delta").mode("append").save(
            f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
        )

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
                f"{self.target_workspace_id} | warehouse_id {self.target_lakehouse_id}"
            )

    def initialise_ddl_script_executions_table(self) -> None:
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        # Check if the execution log table exists
        table_exists = self.lakehouse_utils.check_if_table_exists(
            self.execution_log_table_name
        )
        if not table_exists:
            print(
                f"Creating execution log table at {self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
            )
            empty_df = self.lakehouse_utils.spark.createDataFrame(
                data=[], schema=ddl_utils.execution_log_schema()
            )

            self.lakehouse_utils.write_to_table(
                empty_df, self.execution_log_table_name,
                mode="errorIfExists",  # will error if table exists; change to "overwrite" to replace.
                options={
                    "parquet.vorder.default": "true"
                }
            )
      
            self.write_to_execution_log(
                object_guid=guid, object_name=object_name, script_status="Success"
            )
        else:
            print(f"Skipping {object_name} as it already exists")

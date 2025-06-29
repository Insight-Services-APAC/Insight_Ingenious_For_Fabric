import hashlib
import inspect
from datetime import datetime

from notebookutils import mssparkutils  # type: ignore # noqa: F401
from pyspark.sql import SparkSession  # type: ignore # noqa: F401
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class ddl_utils:
    def __init__(self, target_workspace_id, target_lakehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id
        self.execution_log_table_path = (
            f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{target_lakehouse_id}/Tables/ddl_script_executions"
        )
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

    def print_log(self):
        df = spark.read.format("delta").load(self.execution_log_table_path)
        display(df)

    def check_if_script_has_run(self, script_id) -> bool:
        from pyspark.sql.functions import col

        df = spark.read.format("delta").load(self.execution_log_table_path)
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

    def print_skipped_script_execution(self, guid, object_name):
        print(
            f"skipping {guid}:{object_name} as the script has already run on workspace_id:"
            f"{self.target_workspace_id} | lakehouse_id {self.target_lakehouse_id}"
        )

    def write_to_execution_log(self, object_guid, object_name, script_status):
        data = [(object_guid, object_name, script_status, datetime.now())]
        new_df = spark.createDataFrame(
            data=data, schema=ddl_utils.execution_log_schema()
        )
        new_df.write.format("delta").mode("append").save(self.execution_log_table_path)

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
                mssparkutils.notebook.exit(
                    f"Script {object_name} with guid {guid} failed with error: {e}"
                )
        else:
            self.print_skipped_script_execution(guid=guid, object_name=object_name)

    def initialise_ddl_script_executions_table(self):
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        if (
            lakehouse_utils.check_if_table_exists(self.execution_log_table_path)
            == False
        ):
            empty_df = spark.createDataFrame(
                data=[], schema=ddl_utils.execution_log_schema()
            )
            (
                empty_df.write.format("delta")
                .option("parquet.vorder.default", "true")
                .mode(
                    "errorIfExists"
                )  # will error if table exists; change to "overwrite" to replace.
                .save(self.execution_log_table_path)
            )
            self.write_to_execution_log(
                object_guid=guid, object_name=object_name, script_status="Success"
            )
        else:
            print(f"Skipping {object_name} as it already exists")

# Log table for config execution tracking - Lakehouse version
# Import centralized schema to ensure consistency
from ingen_fab.python_libs.common.config_execution_logging_schema import (
    get_config_execution_log_schema,
)

schema = get_config_execution_log_schema()

target_lakehouse.create_table(  # noqa: F821
    table_name="log_config_execution",
    schema=schema,
    mode="overwrite",
    partition_by=["config_id"],
    options={"parquet.vorder.default": "true"},
)

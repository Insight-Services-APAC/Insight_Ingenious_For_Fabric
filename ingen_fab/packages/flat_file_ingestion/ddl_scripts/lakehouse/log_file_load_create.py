# Log table for file load tracking - Lakehouse version
# Import centralized schema to ensure consistency
from ingen_fab.python_libs.common.file_load_logging_schema import (
    get_file_load_log_schema,
)

schema = get_file_load_log_schema()

target_lakehouse.create_table(  # noqa: F821
    table_name="log_file_load",
    schema=schema,
    mode="overwrite",
    partition_by=["config_id"],
    options={"parquet.vorder.default": "true"},
)

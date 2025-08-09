# Log table for flat file ingestion execution tracking - Lakehouse version
# Import centralized schema to ensure consistency
from ingen_fab.python_libs.common.flat_file_logging_schema import (
    get_flat_file_ingestion_log_schema,
)

schema = get_flat_file_ingestion_log_schema()

target_lakehouse.create_table(
    table_name="log_flat_file_ingestion",
    schema=schema,
    mode="overwrite",
    options={"parquet.vorder.default": "true"},
)

# Config table for ingestion resource configuration - Lakehouse version
# Import centralized schema to ensure consistency
from ingen_fab.python_libs.common.ingestion_resource_config_schema import (
    get_ingestion_resource_config_schema,
)

schema = get_ingestion_resource_config_schema()

target_lakehouse.create_table(  # noqa: F821
    table_name="config_ingestion_resource",
    schema=schema,
    mode="overwrite",
    partition_by=["source_type", "execution_group"],
    options={"parquet.vorder.default": "true"},
)

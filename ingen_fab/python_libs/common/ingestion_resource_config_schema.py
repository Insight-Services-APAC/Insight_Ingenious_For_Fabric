# Centralized schema definition for ingestion resource configuration
# This ensures consistency between DDL creation and ConfigIngestionManager operations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import json
from typing import Any, Dict

from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    ResourceConfig,
    SourceConfig,
    FileFormatParams,
    FileSystemExtractionParams,
    APIExtractionParams,
    DatabaseExtractionParams,
)


def get_ingestion_resource_config_schema() -> StructType:
    """
    Returns the standardized schema for the config_ingestion_resource table.
    This schema stores ResourceConfig objects with polymorphic source parameters.

    Uses MapType for flexible params to support multiple source types:
    - filesystem (FileSystemExtractionParams)
    - api (APIExtractionParams)
    - database (DatabaseExtractionParams)

    Primary Key: resource_name
    """
    return StructType(
        [
            # ====================================================================
            # IDENTITY
            # ====================================================================
            StructField("resource_name", StringType(), False),  # PK
            StructField("source_name", StringType(), False),
            # ====================================================================
            # SOURCE CONFIGURATION
            # ====================================================================
            StructField("source_type", StringType(), False),  # 'filesystem', 'api', 'database'
            StructField("source_connection_params", MapType(StringType(), StringType()), True),
            StructField("source_extraction_params", MapType(StringType(), StringType()), True),
            # ====================================================================
            # EXTRACT LAYER (Used by BOTH frameworks)
            # ====================================================================
            StructField("extract_path", StringType(), False),
            StructField("extract_file_format_params", MapType(StringType(), StringType()), False),
            StructField("extract_storage_workspace", StringType(), False),
            StructField("extract_storage_lakehouse", StringType(), False),
            StructField("extract_error_path", StringType(), False),  # Path for rejected files
            StructField("extract_partition_columns", ArrayType(StringType()), True),  # Hive partition structure
            # ====================================================================
            # STAGING TABLE (Step 1: FILES → STAGING TABLE)
            # ====================================================================
            StructField("stg_table_workspace", StringType(), True),
            StructField("stg_table_lakehouse", StringType(), True),
            StructField("stg_table_schema", StringType(), True),
            StructField("stg_table_name", StringType(), True),
            StructField("stg_table_write_mode", StringType(), True),  # 'overwrite', 'append'
            StructField("stg_table_partition_columns", ArrayType(StringType()), True),
            # ====================================================================
            # TARGET TABLE (Step 2: STAGING TABLE → TARGET TABLE)
            # ====================================================================
            StructField("target_workspace", StringType(), True),
            StructField("target_lakehouse", StringType(), True),
            StructField("target_schema", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("target_schema_columns", ArrayType(StructType([
                StructField("column_name", StringType(), False),
                StructField("data_type", StringType(), False),
            ])), True),
            StructField("target_schema_drift_enabled", BooleanType(), True),
            StructField("target_write_mode", StringType(), True),  # 'overwrite', 'append', 'merge'
            StructField("target_merge_keys", ArrayType(StringType()), True),
            StructField("target_partition_columns", ArrayType(StringType()), True),
            StructField("target_soft_delete_enabled", BooleanType(), True),
            StructField("target_cdc_config", StructType([
                StructField("operation_column", StringType(), False),
                StructField("insert_values", ArrayType(StringType()), False),
                StructField("update_values", ArrayType(StringType()), False),
                StructField("delete_values", ArrayType(StringType()), False),
            ]), True),
            StructField("target_load_type", StringType(), True),
            StructField("target_max_corrupt_records", IntegerType(), True),
            StructField("target_fail_on_rejection", BooleanType(), True),
            # ====================================================================
            # EXECUTION CONTROL (Used by BOTH frameworks)
            # ====================================================================
            StructField("execution_group", IntegerType(), True),
            StructField("active", BooleanType(), True),
            # ====================================================================
            # METADATA
            # ====================================================================
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("updated_by", StringType(), True),
        ]
    )


def row_to_resource_config(row) -> ResourceConfig:
    """
    Parse Delta table row back into ResourceConfig object.
    Internal helper used by ConfigIngestionManager.

    Args:
        row: Spark Row or dict representing a config_ingestion_resource row

    Returns:
        ResourceConfig object
    """
    from ingen_fab.python_libs.pyspark.ingestion.common.config import SchemaColumns, CDCConfig

    # Helper to convert MapType back to dict
    def maptype_to_dict(map_col) -> Dict[str, Any]:
        if not map_col:
            return {}
        return {k: v for k, v in map_col.items()}

    # Access row fields (works for both Row and dict)
    def get_field(name: str):
        if hasattr(row, name):
            return getattr(row, name)
        return row.get(name)

    # Reconstruct SourceConfig
    source_connection_params = maptype_to_dict(get_field("source_connection_params"))

    source_config = SourceConfig(
        source_type=get_field("source_type"),
        source_connection_params=source_connection_params,
    )

    # Reconstruct SchemaColumns from array of structs (optional - None means use schema inference)
    target_schema_columns_array = get_field("target_schema_columns")
    if target_schema_columns_array:
        # Convert Spark array of Row objects to list of dicts
        columns_list = [{"column_name": row.column_name, "data_type": row.data_type} for row in target_schema_columns_array]
        target_schema_columns = SchemaColumns.from_list(columns_list)
    else:
        target_schema_columns = None

    # Reconstruct source_extraction_params
    source_extraction_params = maptype_to_dict(get_field("source_extraction_params"))

    # Parse JSON string fields in source_extraction_params
    if source_extraction_params:
        if 'filename_metadata' in source_extraction_params and isinstance(source_extraction_params['filename_metadata'], str):
            source_extraction_params['filename_metadata'] = json.loads(source_extraction_params['filename_metadata'])
        if 'sort_by' in source_extraction_params and isinstance(source_extraction_params['sort_by'], str):
            source_extraction_params['sort_by'] = json.loads(source_extraction_params['sort_by'])

        # Convert to typed params based on source_type (triggers __post_init__ validation - FAIL FAST)
        source_type = get_field("source_type")
        if source_type == "filesystem":
            source_extraction_params = FileSystemExtractionParams.from_dict(source_extraction_params)
        elif source_type == "api":
            source_extraction_params = APIExtractionParams.from_dict(source_extraction_params)
        elif source_type == "database":
            source_extraction_params = DatabaseExtractionParams.from_dict(source_extraction_params)

    # Reconstruct CDCConfig from struct (optional)
    target_cdc_config = None
    target_cdc_config_struct = get_field("target_cdc_config")
    if target_cdc_config_struct:
        target_cdc_config = CDCConfig(
            operation_column=target_cdc_config_struct.operation_column,
            insert_values=list(target_cdc_config_struct.insert_values),
            update_values=list(target_cdc_config_struct.update_values),
            delete_values=list(target_cdc_config_struct.delete_values),
        )

    # Reconstruct ResourceConfig
    return ResourceConfig(
        resource_name=get_field("resource_name"),
        source_name=get_field("source_name"),
        source_config=source_config,
        extract_path=get_field("extract_path"),
        extract_file_format_params=FileFormatParams.from_dict(get_field("extract_file_format_params")),
        extract_storage_workspace=get_field("extract_storage_workspace"),
        extract_storage_lakehouse=get_field("extract_storage_lakehouse"),
        extract_error_path=get_field("extract_error_path"),
        extract_partition_columns=list(get_field("extract_partition_columns")) if get_field("extract_partition_columns") else ["ds"],
        target_schema_columns=target_schema_columns,
        target_schema_drift_enabled=get_field("target_schema_drift_enabled") if get_field("target_schema_drift_enabled") is not None else False,
        source_extraction_params=source_extraction_params,
        stg_table_workspace=get_field("stg_table_workspace") or "",
        stg_table_lakehouse=get_field("stg_table_lakehouse") or "",
        stg_table_schema=get_field("stg_table_schema") or "",
        stg_table_name=get_field("stg_table_name") or "",
        stg_table_write_mode=get_field("stg_table_write_mode") or "append",
        stg_table_partition_columns=list(get_field("stg_table_partition_columns")) if get_field("stg_table_partition_columns") else [],
        target_workspace=get_field("target_workspace") or "",
        target_lakehouse=get_field("target_lakehouse") or "",
        target_schema=get_field("target_schema") or "",
        target_table=get_field("target_table") or "",
        target_write_mode=get_field("target_write_mode") or "merge",
        target_merge_keys=list(get_field("target_merge_keys")) if get_field("target_merge_keys") else [],
        target_partition_columns=list(get_field("target_partition_columns")) if get_field("target_partition_columns") else [],
        target_soft_delete_enabled=get_field("target_soft_delete_enabled") if get_field("target_soft_delete_enabled") is not None else False,
        target_cdc_config=target_cdc_config,
        target_load_type=get_field("target_load_type") or "incremental",
        target_max_corrupt_records=get_field("target_max_corrupt_records") or 0,
        target_fail_on_rejection=get_field("target_fail_on_rejection") if get_field("target_fail_on_rejection") is not None else True,
        execution_group=get_field("execution_group") or 1,
        active=get_field("active") if get_field("active") is not None else True,
    )


def resource_config_to_row(config: ResourceConfig, created_by: str = "system", updated_by: str = "system") -> Dict[str, Any]:
    """
    Convert ResourceConfig object to dict matching Delta table schema.
    Used by ConfigIngestionManager for saving configs.

    Args:
        config: ResourceConfig object to serialize
        created_by: Username for created_by field
        updated_by: Username for updated_by field

    Returns:
        Dict with keys matching schema field names
    """
    from datetime import datetime

    # Convert SchemaColumns to array of structs (None if no schema defined)
    target_schema_columns_array = None
    if config.target_schema_columns:
        target_schema_columns_array = [
            {"column_name": col["column_name"], "data_type": col["data_type"]}
            for col in config.target_schema_columns.columns
        ]

    # Convert CDCConfig to struct (if present)
    target_cdc_config_struct = None
    if config.target_cdc_config:
        target_cdc_config_struct = {
            "operation_column": config.target_cdc_config.operation_column,
            "insert_values": config.target_cdc_config.insert_values,
            "update_values": config.target_cdc_config.update_values,
            "delete_values": config.target_cdc_config.delete_values,
        }

    # Build row dict
    return {
        # IDENTITY
        "resource_name": config.resource_name,
        "source_name": config.source_name,
        # SOURCE CONFIGURATION
        "source_type": config.source_config.source_type,
        "source_connection_params": config.source_config.source_connection_params,
        "source_extraction_params": config.source_extraction_params,
        # EXTRACT LAYER
        "extract_path": config.extract_path,
        "extract_file_format_params": config.extract_file_format_params.to_dict(),
        "extract_storage_workspace": config.extract_storage_workspace,
        "extract_storage_lakehouse": config.extract_storage_lakehouse,
        "extract_error_path": config.extract_error_path,
        "extract_partition_columns": config.extract_partition_columns,
        "target_schema_columns": target_schema_columns_array,
        "target_schema_drift_enabled": config.target_schema_drift_enabled,
        # STAGING TABLE
        "stg_table_workspace": config.stg_table_workspace,
        "stg_table_lakehouse": config.stg_table_lakehouse,
        "stg_table_schema": config.stg_table_schema,
        "stg_table_name": config.stg_table_name,
        "stg_table_write_mode": config.stg_table_write_mode,
        "stg_table_partition_columns": config.stg_table_partition_columns,
        # TARGET TABLE
        "target_workspace": config.target_workspace,
        "target_lakehouse": config.target_lakehouse,
        "target_schema": config.target_schema,
        "target_table": config.target_table,
        "target_write_mode": config.target_write_mode,
        "target_merge_keys": config.target_merge_keys,
        "target_partition_columns": config.target_partition_columns,
        "target_soft_delete_enabled": config.target_soft_delete_enabled,
        "target_cdc_config": target_cdc_config_struct,
        "target_load_type": config.target_load_type,
        "target_max_corrupt_records": config.target_max_corrupt_records,
        "target_fail_on_rejection": config.target_fail_on_rejection,
        # EXECUTION CONTROL
        "execution_group": config.execution_group,
        "active": config.active,
        # METADATA
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "created_by": created_by,
        "updated_by": updated_by,
    }


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the config table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_ingestion_resource_config_schema()
    return [field.name for field in schema.fields]

# Centralized schema definition for ingestion resource configuration
# This ensures consistency between DDL creation and ConfigManager operations

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

from ingen_fab.python_libs.pyspark.ingestion.config import (
    ResourceConfig,
    SourceConfig,
)


def get_ingestion_resource_config_schema() -> StructType:
    """
    Returns the standardized schema for the config_ingestion_resource table.
    This schema stores ResourceConfig objects with polymorphic source parameters.

    Uses MapType for flexible source_params to support multiple source types:
    - filesystem (FileSystemLoadingParams)
    - api (APIExtractionParams)
    - database (DatabaseExtractionParams)

    Primary Key: config_id
    """
    return StructType(
        [
            # ====================================================================
            # IDENTITY
            # ====================================================================
            StructField("config_id", StringType(), False),  # PK
            StructField("resource_name", StringType(), False),
            StructField("source_name", StringType(), False),
            # ====================================================================
            # SOURCE CONFIGURATION
            # ====================================================================
            StructField(
                "source_type", StringType(), False
            ),  # 'filesystem', 'api', 'database'
            StructField(
                "connection_params", MapType(StringType(), StringType()), True
            ),
            # Authentication
            StructField("auth_type", StringType(), True),
            StructField("auth_params", MapType(StringType(), StringType()), True),
            # Source description
            StructField("source_description", StringType(), True),
            # ====================================================================
            # EXTRACTION SETTINGS (for future extraction framework)
            # ====================================================================
            StructField("extraction_output_path", StringType(), True),
            StructField(
                "extraction_params", MapType(StringType(), StringType()), True
            ),
            # ====================================================================
            # FILE LOADING SETTINGS
            # ====================================================================
            StructField("source_file_path", StringType(), True),
            StructField("source_file_format", StringType(), True),
            # POLYMORPHIC PARAMETERS (different per source_type!)
            StructField("loading_params", MapType(StringType(), StringType()), True),
            # ====================================================================
            # TARGET SETTINGS
            # ====================================================================
            StructField("target_workspace_name", StringType(), True),
            StructField("target_datastore_name", StringType(), True),
            StructField(
                "target_datastore_type", StringType(), True
            ),  # 'lakehouse', 'warehouse'
            StructField("target_schema_name", StringType(), True),
            StructField("target_table_name", StringType(), True),
            # ====================================================================
            # WRITE SETTINGS
            # ====================================================================
            StructField(
                "write_mode", StringType(), True
            ),  # 'overwrite', 'append', 'merge'
            StructField("merge_keys", ArrayType(StringType()), True),
            StructField("partition_columns", ArrayType(StringType()), True),
            StructField("enable_schema_evolution", BooleanType(), True),
            # ====================================================================
            # DATA VALIDATION
            # ====================================================================
            StructField("custom_schema_json", StringType(), True),
            StructField("data_validation_rules", StringType(), True),
            # ====================================================================
            # EXECUTION CONTROL
            # ====================================================================
            StructField("execution_group", IntegerType(), True),
            StructField("active_yn", StringType(), True),
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
    Internal helper used by ConfigManager.

    Args:
        row: Spark Row or dict representing a config_ingestion_resource row

    Returns:
        ResourceConfig object
    """

    # Helper to parse string values back to proper types
    def parse_value(value: str, expected_type: type = str):
        if value is None or value == "":
            return None
        if expected_type == bool:
            return value.lower() == "true"
        elif expected_type == int:
            return int(value)
        elif expected_type == float:
            return float(value)
        elif expected_type in (list, dict):
            return json.loads(value)
        return value

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
    connection_params = maptype_to_dict(get_field("connection_params"))
    auth_params = maptype_to_dict(get_field("auth_params"))

    source_config = SourceConfig(
        source_type=get_field("source_type"),
        connection_params=connection_params,
        auth_type=get_field("auth_type"),
        auth_params=auth_params if auth_params else None,
        description=get_field("source_description"),
    )

    # Reconstruct loading_params from MapType
    # Type conversion based on known FileSystemLoadingParams fields
    loading_params_raw = maptype_to_dict(get_field("loading_params"))
    loading_params = {}

    for key, value in loading_params_raw.items():
        # Boolean fields
        if key in [
            "recursive",
            "has_header",
            "multiline_values",
            "require_control_file",
            "require_files",
            "enable_archive",
            "cleanup_empty_dirs",
        ]:
            loading_params[key] = parse_value(value, bool)
        # Keep as string
        else:
            loading_params[key] = value

    # Reconstruct extraction_params (for future use)
    extraction_params = maptype_to_dict(get_field("extraction_params"))

    # Reconstruct ResourceConfig
    return ResourceConfig(
        resource_name=get_field("resource_name"),
        source_name=get_field("source_name"),
        source_config=source_config,
        extraction_output_path=get_field("extraction_output_path"),
        extraction_params=extraction_params,
        source_file_path=get_field("source_file_path"),
        source_file_format=get_field("source_file_format"),
        loading_params=loading_params,
        target_workspace_name=get_field("target_workspace_name") or "",
        target_datastore_name=get_field("target_datastore_name") or "",
        target_datastore_type=get_field("target_datastore_type") or "",
        target_schema_name=get_field("target_schema_name") or "",
        target_table_name=get_field("target_table_name") or "",
        write_mode=get_field("write_mode") or "overwrite",
        merge_keys=list(get_field("merge_keys")) if get_field("merge_keys") else [],
        partition_columns=list(get_field("partition_columns"))
        if get_field("partition_columns")
        else [],
        enable_schema_evolution=get_field("enable_schema_evolution")
        if get_field("enable_schema_evolution") is not None
        else True,
        custom_schema_json=get_field("custom_schema_json"),
        data_validation_rules=get_field("data_validation_rules"),
        execution_group=get_field("execution_group") or 1,
        active_yn=get_field("active_yn") or "Y",
    )


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the config table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_ingestion_resource_config_schema()
    return [field.name for field in schema.fields]

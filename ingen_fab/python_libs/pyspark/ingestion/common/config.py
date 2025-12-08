# Shared Configuration for Data Integration Frameworks
# Used by both Extraction Framework (python/extraction) and File Loading Framework (pyspark/file_loading)

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pyspark.sql.types import StructType, StructField, StringType


# ============================================================================
# METADATA COLUMN CONFIGURATION
# ============================================================================

@dataclass
class MetadataColumns:
    """
    Metadata column names used by the loading framework.

    Defaults match current framework conventions. Clients can override via
    LoadingOrchestrator for custom naming preferences (dbt-style overrides).

    Example:
        # Override specific columns
        MetadataColumns.from_dict({
            "_raw_created_load_id": "batch_id",
            "_raw_created_at": "created_ts"
        })
    """
    # Staging table metadata (Step 1: Files → Staging)
    stg_created_load_id: str = "_stg_created_load_id"
    stg_file_path: str = "_stg_file_path"
    stg_created_at: str = "_stg_created_at"
    stg_corrupt_record: str = "_stg_corrupt_record"

    # Target table metadata (Step 2: Staging → Target)
    raw_created_load_id: str = "_raw_created_load_id"
    raw_updated_load_id: str = "_raw_updated_load_id"
    raw_file_path: str = "_raw_file_path"
    raw_loaded_at: str = "_raw_loaded_at"
    raw_corrupt_record: str = "_raw_corrupt_record"
    raw_created_at: str = "_raw_created_at"
    raw_updated_at: str = "_raw_updated_at"
    raw_is_deleted: str = "_raw_is_deleted"
    raw_filename: str = "_raw_filename"

    @classmethod
    def from_dict(cls, overrides: Optional[Dict[str, str]] = None) -> "MetadataColumns":
        """
        Create MetadataColumns from override dict.

        Keys are default column names, values are custom names.
        Supports 0, 1, or many overrides - only specified columns change.

        Args:
            overrides: Dict mapping default names to custom names, e.g.:
                {"_raw_created_load_id": "batch_id", "_raw_created_at": "created_ts"}

        Returns:
            MetadataColumns instance with overrides applied
        """
        if not overrides:
            return cls()

        defaults = cls()
        return cls(
            stg_created_load_id=overrides.get("_stg_created_load_id", defaults.stg_created_load_id),
            stg_file_path=overrides.get("_stg_file_path", defaults.stg_file_path),
            stg_created_at=overrides.get("_stg_created_at", defaults.stg_created_at),
            stg_corrupt_record=overrides.get("_stg_corrupt_record", defaults.stg_corrupt_record),
            raw_created_load_id=overrides.get("_raw_created_load_id", defaults.raw_created_load_id),
            raw_updated_load_id=overrides.get("_raw_updated_load_id", defaults.raw_updated_load_id),
            raw_file_path=overrides.get("_raw_file_path", defaults.raw_file_path),
            raw_loaded_at=overrides.get("_raw_loaded_at", defaults.raw_loaded_at),
            raw_corrupt_record=overrides.get("_raw_corrupt_record", defaults.raw_corrupt_record),
            raw_created_at=overrides.get("_raw_created_at", defaults.raw_created_at),
            raw_updated_at=overrides.get("_raw_updated_at", defaults.raw_updated_at),
            raw_is_deleted=overrides.get("_raw_is_deleted", defaults.raw_is_deleted),
            raw_filename=overrides.get("_raw_filename", defaults.raw_filename),
        )


# ============================================================================
# SOURCE CONFIGURATION
# ============================================================================

@dataclass
class SourceConfig:
    """
    Configuration for a data source.

    Used by Extraction Framework to determine how to connect and extract data.
    """

    source_type: str                        # 'api', 'database', 'filesystem'

    # Connection parameters (source-type specific, includes auth if needed)
    source_connection_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.source_type:
            raise ValueError("source_type is required")


# ============================================================================
# SCHEMA CONFIGURATION
# ============================================================================

@dataclass
class SchemaColumns:
    """
    Schema definition for a table.

    Stores column definitions and converts to Spark StructType on demand.
    Uses Spark's native type system (integer, string, decimal(10,2), etc.)

    Example:
        SchemaColumns.from_list([
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "amount", "data_type": "decimal(10,2)"},
            {"column_name": "name", "data_type": "string"}
        ])
    """
    columns: List[Dict[str, str]]  # [{"column_name": "id", "data_type": "integer"}, ...]

    def to_spark_schema(self) -> StructType:
        """
        Convert to Spark StructType using native JSON schema parsing.

        Returns:
            pyspark.sql.types.StructType

        Raises:
            ValueError: If schema contains invalid Spark types
        """
        # Convert to Spark JSON schema format
        fields = []
        for col in self.columns:
            fields.append({
                "name": col["column_name"],
                "type": col["data_type"],
                "nullable": True,
                "metadata": {}
            })

        spark_json_schema = {
            "type": "struct",
            "fields": fields
        }

        try:
            return StructType.fromJson(spark_json_schema)
        except Exception as e:
            col_summary = ", ".join([f"{c['column_name']}:{c['data_type']}" for c in self.columns])
            raise ValueError(f"Invalid Spark schema - {str(e)}. Columns: {col_summary}")

    def to_raw_schema(self) -> StructType:
        """
        Convert to raw table schema (all fields as StringType).

        Raw tables store everything as string for schema evolution safety
        and to preserve original values before type casting.

        Returns:
            StructType with all fields converted to StringType
        """
        typed_schema = self.to_spark_schema()
        return StructType([
            StructField(field.name, StringType(), True)
            for field in typed_schema.fields
        ])

    def to_target_schema(self) -> StructType:
        """
        Convert to target table schema (typed fields).

        Returns typed schema for target table writes and type casting validation.
        Alias for to_spark_schema() for clarity.

        Returns:
            StructType with fields using configured data types
        """
        return self.to_spark_schema()

    @classmethod
    def from_list(cls, schema_columns: List[Dict[str, str]]) -> "SchemaColumns":
        """
        Create SchemaColumns from list of dicts with validation.

        Args:
            schema_columns: [{"column_name": "id", "data_type": "integer"}, ...]

        Returns:
            SchemaColumns instance

        Raises:
            ValueError: If schema_columns has invalid structure or empty
        """
        if not schema_columns:
            raise ValueError("schema_columns is required and cannot be empty")

        # Validate structure
        for idx, col_def in enumerate(schema_columns):
            if not isinstance(col_def, dict):
                raise ValueError(f"schema_columns[{idx}]: expected dict, got {type(col_def).__name__}")

            if "column_name" not in col_def:
                raise ValueError(f"schema_columns[{idx}]: missing 'column_name'")

            if "data_type" not in col_def:
                raise ValueError(f"schema_columns[{idx}]: missing 'data_type'")

        return cls(columns=schema_columns)


# ============================================================================
# RESOURCE CONFIGURATION (Used by BOTH frameworks)
# ============================================================================

@dataclass
class ResourceConfig:
    """
    Complete configuration for a data resource.

    Contains settings for BOTH extraction and file loading.
    Each framework uses only the fields relevant to it.

    Architecture (dlt-inspired pattern):
    - Extraction writes TO extract_path
    - Loading reads FROM extract_path
    - On successful bronze write, files STAY in extract_path
    - On failure, files STAY in extract_path for manual intervention
    - State tracked in log_resource_extract_batch.load_state
    """

    # ========================================================================
    # IDENTITY
    # ========================================================================

    resource_name: str                      # Unique identifier (primary key)
    source_name: str                        # Logical source grouping (e.g., "sales", "customer")
    source_config: SourceConfig             # Source connection info

    # ========================================================================
    # EXTRACT LAYER (Used by BOTH frameworks)
    # ========================================================================

    # Extract layer locations (extraction writes to landing, loading reads from landing)
    extract_path: str               # e.g., "Files/raw/edl/fct_sales/" - files stay here (successful or failed)
    extract_file_format_params: FileFormatParams  # File format configuration (format type + options)
    extract_storage_workspace: str          # Workspace where raw files are stored
    extract_storage_lakehouse: str          # Lakehouse where raw files are stored
    extract_error_path: str                 # e.g., "Files/errors/edl/fct_sales/" - rejected files moved here
    extract_partition_columns: List[str] = field(default_factory=lambda: ["ds"])  # Hive partition structure for extract layer
    target_schema_columns: Optional[SchemaColumns] = None  # Optional - if None, use schema inference
    target_schema_drift_enabled: bool = False  # If True, allow new columns from source when inferring schema

    # ========================================================================
    # EXTRACTION SETTINGS (Used by Extraction Framework ONLY)
    # ========================================================================

    # Source-type specific extraction parameters
    source_extraction_params: Union[Dict[str, Any], BaseExtractionParams] = field(default_factory=dict)

    # ========================================================================
    # STEP 1: FILES → STAGING TABLE (Used by File Loading Framework ONLY)
    # ========================================================================

    stg_table_workspace: str = ""
    stg_table_lakehouse: str = ""
    stg_table_schema: str = ""
    stg_table_name: str = ""
    stg_table_write_mode: str = "append"    # 'overwrite' or 'append'
    stg_table_partition_columns: List[str] = field(default_factory=list)

    # ========================================================================
    # STEP 2: STAGING TABLE → TARGET TABLE (Used by File Loading Framework ONLY)
    # ========================================================================

    target_workspace: str = ""
    target_lakehouse: str = ""              # or target_warehouse
    target_schema: str = ""
    target_table: str = ""
    target_write_mode: str = "merge"        # 'overwrite', 'append', 'merge'
    target_merge_keys: List[str] = field(default_factory=list)
    target_partition_columns: List[str] = field(default_factory=list)

    # Soft delete (applies to ALL merge operations - with or without CDC)
    target_soft_delete_enabled: bool = False    # If True, DELETE becomes UPDATE SET _raw_is_deleted=True

    # CDC configuration (optional, only for CDC sources)
    target_cdc_config: Optional['CDCConfig'] = None  # None = standard merge (no CDC operations)

    # Load type (incremental vs full snapshot)
    target_load_type: str = "incremental"  # 'incremental' or 'full' - full loads mark missing records as deleted

    # ========================================================================
    # DATA VALIDATION (Used by File Loading Framework ONLY)
    # ========================================================================

    target_max_corrupt_records: int = 0  # Max corrupt records allowed (0 = reject any corrupt records)
    target_fail_on_rejection: bool = True  # If True, fail on rejections. If False, skip rejected batches and continue.

    # ========================================================================
    # EXECUTION CONTROL (Used by BOTH frameworks)
    # ========================================================================

    execution_group: int = 1
    active: bool = True

    def __post_init__(self):
        """Validate configuration after initialization"""
        # Required fields
        if not self.resource_name:
            raise ValueError("resource_name is required")
        if not self.source_name:
            raise ValueError("source_name is required")

        # Write mode validation
        valid_write_modes = ["overwrite", "append", "merge"]
        if self.target_write_mode and self.target_write_mode not in valid_write_modes:
            raise ValueError(f"target_write_mode must be one of {valid_write_modes}, got '{self.target_write_mode}'")

        # Merge requires keys
        if self.target_write_mode == "merge" and not self.target_merge_keys:
            raise ValueError("target_merge_keys required when target_write_mode='merge'")

        # Validate pipeline params for database sources
        if self.source_config.source_type == "database":
            required_params = [
                "pipeline_workspace_name",
                "pipeline_name",
                "pipeline_source_connection_id",
                "pipeline_source_database",
            ]
            missing = [p for p in required_params if not self.source_config.source_connection_params.get(p)]
            if missing:
                raise ValueError(
                    f"Database source requires {', '.join(required_params)} in source_connection_params. "
                    f"Missing: {', '.join(missing)}"
                )

    @property
    def extract_full_path(self) -> str:
        """
        Build full ABFSS path to extract layer.

        Constructs OneLake path from workspace, lakehouse, and extract_path.
        Format: abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Files/{path}

        Returns:
            str: Full ABFSS path to extract layer
        """
        return (
            f"abfss://{self.extract_storage_workspace}@onelake.dfs.fabric.microsoft.com/"
            f"{self.extract_storage_lakehouse}.Lakehouse/Files/{self.extract_path.strip('/')}"
        )

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any], source_config: SourceConfig) -> "ResourceConfig":
        """Create ResourceConfig from dictionary"""

        def split_csv(value):
            if not value or not isinstance(value, str):
                return []
            return [item.strip() for item in value.split(",") if item.strip()]

        # Parse CDC config if present
        target_cdc_config = None
        if "target_cdc_config" in config_dict and config_dict["target_cdc_config"]:
            target_cdc_config = CDCConfig.from_dict(config_dict["target_cdc_config"])

        # Backward compatibility: map old field names to new ones
        target_write_mode = config_dict.get("target_write_mode") or config_dict.get("write_mode", "overwrite")
        target_merge_keys = config_dict.get("target_merge_keys") or config_dict.get("merge_keys")
        target_partition_columns = config_dict.get("target_partition_columns") or config_dict.get("partition_columns")

        return cls(
            resource_name=config_dict["resource_name"],
            source_name=config_dict["source_name"],
            source_config=source_config,
            extract_path=config_dict["extract_path"],
            extract_file_format_params=FileFormatParams.from_dict(config_dict["extract_file_format_params"]),
            extract_storage_workspace=config_dict["extract_storage_workspace"],
            extract_storage_lakehouse=config_dict["extract_storage_lakehouse"],
            extract_error_path=config_dict["extract_error_path"],
            extract_partition_columns=split_csv(config_dict.get("extract_partition_columns")) or ["ds"],
            source_extraction_params=config_dict.get("source_extraction_params", {}),
            stg_table_workspace=config_dict.get("stg_table_workspace", ""),
            stg_table_lakehouse=config_dict.get("stg_table_lakehouse", ""),
            stg_table_schema=config_dict.get("stg_table_schema", ""),
            stg_table_name=config_dict.get("stg_table_name", ""),
            stg_table_write_mode=config_dict.get("stg_table_write_mode", "append"),
            stg_table_partition_columns=split_csv(config_dict.get("stg_table_partition_columns")),
            target_workspace=config_dict.get("target_workspace", ""),
            target_lakehouse=config_dict.get("target_lakehouse", ""),
            target_schema=config_dict.get("target_schema", ""),
            target_table=config_dict.get("target_table", ""),
            target_write_mode=target_write_mode,
            target_merge_keys=split_csv(target_merge_keys),
            target_partition_columns=split_csv(target_partition_columns),
            target_soft_delete_enabled=config_dict.get("target_soft_delete_enabled", False),
            target_cdc_config=target_cdc_config,
            target_load_type=config_dict.get("target_load_type", "incremental"),
            target_schema_columns=SchemaColumns.from_list(config_dict["target_schema_columns"]) if config_dict.get("target_schema_columns") else None,
            target_schema_drift_enabled=config_dict.get("target_schema_drift_enabled", False),
            target_max_corrupt_records=config_dict.get("target_max_corrupt_records", 0),
            target_fail_on_rejection=config_dict.get("target_fail_on_rejection", True),
            execution_group=config_dict.get("execution_group", 1),
            active=config_dict.get("active", True),
        )


# ============================================================================
# EXTRACTION PARAMETERS BASE CLASS
# ============================================================================

class BaseExtractionParams(ABC):
    """
    Abstract base class for all source extraction parameters.

    Enforces consistent interface across filesystem, API, database, and future extractors.
    Each extractor type implements its own params class with source-specific validation.
    """

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert params to dict for storage in Delta table"""
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, params: Dict[str, Any]) -> "BaseExtractionParams":
        """
        Create params instance from dict (triggers __post_init__ validation).

        This is where validation happens - invalid configs will raise ValueError.
        """
        pass


# ============================================================================
# FILE FORMAT PARAMETERS (Used by Loading Framework)
# ============================================================================

@dataclass
class FileFormatParams:
    """
    File format configuration for reading/writing files.

    Supports multiple formats with flexible options:
    - CSV: has_header, file_delimiter, encoding, quote_character, etc.
    - JSON: multiline, json_lines, flatten_depth, etc.
    - Parquet: compression, merge_schema, etc.
    """
    file_format: str  # 'csv', 'json', 'parquet'
    format_options: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Flatten to single-level dict for Delta storage"""
        return {
            "file_format": self.file_format,
            **self.format_options
        }

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "FileFormatParams":
        """Parse from Delta table dict"""
        file_format = params.get("file_format", "csv")
        format_options = {k: v for k, v in params.items() if k != "file_format"}
        return cls(file_format=file_format, format_options=format_options)


# ============================================================================
# EXTRACTION PARAMETERS (Used by Extraction Framework)
# ============================================================================

@dataclass
class FileSystemExtractionParams(BaseExtractionParams):
    """
    Parameters for filesystem extraction (discovery & validation only).

    File format params (CSV delimiters, etc.) are in FileFormatParams.
    """

    # Source location
    inbound_path: str

    # File discovery
    discovery_pattern: str = "*.csv"
    recursive: bool = False
    batch_by: str = "file"  # 'file' (individual files), 'folder' (folder batches), or 'all' (entire directory as one batch)

    # Validation
    control_file_pattern: Optional[str] = None  # If set, only process files with control files
    duplicate_handling: str = "warn"  # 'warn', 'allow', 'fail'
    no_data_handling: str = "allow"  # 'allow', 'warn', 'fail' - when no data extracted

    # Metadata extraction from filename/path (NEW)
    filename_metadata: List[Dict[str, Any]] = field(default_factory=list)
    # Example: [
    #     {"name": "file_date", "regex": r"sales_(\d{8})", "type": "date", "format": "yyyyMMdd"},
    #     {"name": "region", "regex": r"_([A-Z]{3})_", "type": "string"}
    # ]
    # Supported types: string, date, timestamp, int, long, double, boolean

    # Sorting configuration (NEW)
    sort_by: List[str] = field(default_factory=list)  # List of metadata field names to sort by
    sort_order: str = "asc"  # 'asc' or 'desc'

    partition_depth: Optional[int] = None  # Folder depth for batch_by="folder" (e.g., 3 for YYYY/MM/DD)

    # Watermark-based incremental extraction (NEW)
    move_source_file: bool = True  # True = move (copy + delete source), False = copy only
    incremental_column: Optional[str] = None  # "modified_time" for watermark-based extraction

    def __post_init__(self):
        """Validate extraction parameters"""
        # Required fields
        if not self.inbound_path:
            raise ValueError("inbound_path is required")

        # Enum validations
        valid_duplicate = ["warn", "allow", "fail"]
        if self.duplicate_handling not in valid_duplicate:
            raise ValueError(f"duplicate_handling must be one of {valid_duplicate}, got '{self.duplicate_handling}'")

        valid_no_data = ["allow", "warn", "fail"]
        if self.no_data_handling not in valid_no_data:
            raise ValueError(f"no_data_handling must be one of {valid_no_data}, got '{self.no_data_handling}'")

        valid_batch_by = ["file", "folder", "all"]
        if self.batch_by not in valid_batch_by:
            raise ValueError(f"batch_by must be one of {valid_batch_by}, got '{self.batch_by}'")

        valid_sort_order = ["asc", "desc"]
        if self.sort_order not in valid_sort_order:
            raise ValueError(f"sort_order must be one of {valid_sort_order}, got '{self.sort_order}'")

        # Validate incremental_column
        # Can be None, "modified_time", or a filename_metadata field name
        if self.incremental_column is not None and self.incremental_column != "modified_time":
            # Check if it references a valid filename_metadata field
            metadata_field_names = [p["name"] for p in self.filename_metadata] if self.filename_metadata else []
            if self.incremental_column not in metadata_field_names:
                raise ValueError(
                    f"incremental_column '{self.incremental_column}' must be 'modified_time' or "
                    f"a filename_metadata field name. Available metadata fields: {metadata_field_names}"
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for storage in ResourceConfig.extraction_params"""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "FileSystemExtractionParams":
        """Create from dict"""
        return cls(**{k: v for k, v in params.items() if k in cls.__annotations__})


@dataclass
class APIExtractionParams(BaseExtractionParams):
    """Parameters for extracting data from REST APIs"""

    endpoint: str
    method: str = "GET"
    headers: Optional[Dict[str, str]] = None
    query_params: Optional[Dict[str, Any]] = None
    pagination_type: str = "none"
    partition_by_date: bool = True  # Creates YYYY/MM/DD folders in raw

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "APIExtractionParams":
        return cls(**{k: v for k, v in params.items() if k in cls.__annotations__})


def _validate_incremental_value(value: str, column_type: str) -> None:
    """
    Validate incremental_start/end string can be parsed for the given column_type.

    Args:
        value: String value (ISO date/datetime or integer string)
        column_type: "date", "timestamp", or "integer"

    Raises:
        ValueError: If value cannot be parsed for the given column_type
    """
    try:
        if column_type == "integer":
            int(value)
        elif column_type == "date":
            datetime.strptime(value, "%Y-%m-%d")
        elif column_type == "timestamp":
            if "T" in value or " " in value:
                v = value.replace("T", " ")
                if "." in v:
                    datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
            else:
                datetime.strptime(value, "%Y-%m-%d")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot parse '{value}' as {column_type}: {e}")


@dataclass
class DatabaseExtractionParams(BaseExtractionParams):
    """
    Parameters for database extraction (JDBC or Pipeline-based).

    Supports three extraction modes:
    - "table": Generate SELECT from source_schema + source_table with optional filtering
    - "query": Use provided custom SQL query
    - "cetas": Wrap SELECT in Synapse CETAS statement (requires db_type="synapse")

    Strategy selection (automatic):
    - If pipeline_name in source_connection_params → delegate to pipeline
    - Otherwise → direct JDBC extraction
    """

    # Database type (dialect for SQL generation)
    db_type: str = "sqlserver"  # sqlserver, synapse, postgres, oracle

    # Extraction mode
    extraction_mode: str = "table"  # "table", "query", or "cetas"

    # Table-based extraction
    source_schema: Optional[str] = None
    source_table: Optional[str] = None

    # Column selection (optional - if None, extract all columns)
    columns: Optional[List[str]] = None

    # Filtering (optional)
    where_clause: Optional[str] = None

    # Query-based extraction (used when extraction_mode="query" or as inner SELECT for "cetas")
    query: Optional[str] = None

    # Incremental extraction (optional)
    incremental_column: Optional[str] = None

    # Incremental column type - determines how intervals are interpreted
    # "date", "timestamp", or "integer"
    incremental_column_type: Optional[str] = None

    # Chunking for backfills - splits large incremental loads into sequential ranges
    # For date/timestamp: interval in hours (e.g., 24=day, 168=week, 720=month)
    # For integer: step size (e.g., 100000)
    incremental_chunk_size: Optional[int] = None

    # Lookback period for late-arriving data - applied to watermark before extraction
    # For date/timestamp: lookback in hours (e.g., 72 = 3 days)
    # For integer: offset subtracted from watermark (e.g., 1000)
    incremental_lookback: Optional[int] = None

    # Explicit bounds for chunked extraction (REQUIRED when incremental_chunk_size is set)
    # ISO date/datetime string for date/timestamp, integer string for integer
    incremental_start: Optional[str] = None  # REQUIRED for chunking
    incremental_end: Optional[str] = None    # Optional for date/timestamp (defaults to now), REQUIRED for integer

    # Performance tuning
    fetch_size: int = 10000

    # CETAS-specific (only used when extraction_mode="cetas" and db_type="synapse")
    cetas_data_source: Optional[str] = None  # Synapse external data source name (REQUIRED for CETAS)
    cetas_file_format: Optional[str] = None  # Synapse file format name (REQUIRED for CETAS)
    cetas_external_schema: Optional[str] = None  # Schema for external tables (REQUIRED for CETAS)

    def __post_init__(self):
        """Validate database extraction parameters"""
        # Validate db_type
        valid_db_types = {"sqlserver", "synapse", "postgres", "oracle"}
        if self.db_type not in valid_db_types:
            raise ValueError(
                f"Invalid db_type: '{self.db_type}'. Must be one of: {sorted(valid_db_types)}"
            )

        # Validate extraction_mode
        valid_modes = {"table", "query", "cetas"}
        if self.extraction_mode not in valid_modes:
            raise ValueError(
                f"Invalid extraction_mode: '{self.extraction_mode}'. Must be one of: {sorted(valid_modes)}"
            )

        # CETAS requires synapse
        if self.extraction_mode == "cetas" and self.db_type != "synapse":
            raise ValueError(
                f"extraction_mode='cetas' requires db_type='synapse', got '{self.db_type}'"
            )

        # CETAS requires data_source, file_format, and external_schema
        if self.extraction_mode == "cetas":
            missing = []
            if not self.cetas_data_source:
                missing.append("cetas_data_source")
            if not self.cetas_file_format:
                missing.append("cetas_file_format")
            if not self.cetas_external_schema:
                missing.append("cetas_external_schema")
            if missing:
                raise ValueError(
                    f"extraction_mode='cetas' requires: {', '.join(missing)}"
                )

        # Must have either query or table (for table and cetas modes)
        if self.extraction_mode in ("table", "cetas") and not self.source_table and not self.query:
            raise ValueError("Must specify either 'query' or 'source_table'")

        # Query mode requires query field
        if self.extraction_mode == "query" and not self.query:
            raise ValueError("extraction_mode='query' requires 'query' field to be specified")

        # Validate incremental_column_type
        if self.incremental_column_type:
            valid_column_types = {"date", "timestamp", "integer"}
            if self.incremental_column_type not in valid_column_types:
                raise ValueError(
                    f"Invalid incremental_column_type: '{self.incremental_column_type}'. "
                    f"Must be one of: {sorted(valid_column_types)}"
                )

        # Chunking requires explicit bounds (no watermark mixing)
        if self.incremental_chunk_size:
            if not self.incremental_column:
                raise ValueError("incremental_chunk_size requires incremental_column to be specified")
            if not self.incremental_column_type:
                raise ValueError("incremental_chunk_size requires incremental_column_type to be specified")
            if not self.incremental_start:
                raise ValueError("incremental_chunk_size requires incremental_start to be specified")
            # NOTE: incremental_end is optional for ALL types
            # - date/timestamp: defaults to "now"
            # - integer: uses progressive mode (stops on first empty chunk)
            if self.incremental_chunk_size < 1:
                raise ValueError(f"incremental_chunk_size must be >= 1, got {self.incremental_chunk_size}")

        # Lookback requires incremental_column_type
        if self.incremental_lookback:
            if not self.incremental_column_type:
                raise ValueError("incremental_lookback requires incremental_column_type to be specified")
            if self.incremental_lookback < 0:
                raise ValueError(f"incremental_lookback must be >= 0, got {self.incremental_lookback}")

        # Validate incremental_start/end can be parsed
        if self.incremental_start and self.incremental_column_type:
            _validate_incremental_value(self.incremental_start, self.incremental_column_type)
        if self.incremental_end and self.incremental_column_type:
            _validate_incremental_value(self.incremental_end, self.incremental_column_type)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for storage in ResourceConfig.extraction_params"""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "DatabaseExtractionParams":
        """Create from dict (triggers __post_init__ validation)"""
        return cls(**{k: v for k, v in params.items() if k in cls.__annotations__})


# ============================================================================
# CDC CONFIGURATION (Used by File Loading Framework)
# ============================================================================

@dataclass
class CDCConfig:
    """
    Configuration for CDC (Change Data Capture) data processing.

    Maps operation type values in source data to INSERT/UPDATE/DELETE operations.
    Supports various CDC formats (Debezium, SQL Server CDC, generic CDC, etc.)

    Example - Debezium:
        CDCConfig(
            operation_column="op",
            insert_values=["c", "r"],  # c=create, r=read (snapshot)
            update_values=["u"],
            delete_values=["d"]
        )

    Example - SQL Server CDC:
        CDCConfig(
            operation_column="__$operation",
            insert_values=["2"],  # 2=insert
            update_values=["4"],  # 4=update
            delete_values=["1"]   # 1=delete
        )
    """

    operation_column: str                    # Column containing operation type indicator
    insert_values: List[str]                 # Values indicating INSERT operation
    update_values: List[str]                 # Values indicating UPDATE operation
    delete_values: List[str]                 # Values indicating DELETE operation

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "CDCConfig":
        """Create from dict, converting single strings to lists for convenience"""
        data = params.copy()
        for key in ['insert_values', 'update_values', 'delete_values']:
            if key in data and isinstance(data[key], str):
                data[key] = [data[key]]  # Convert single string to list
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

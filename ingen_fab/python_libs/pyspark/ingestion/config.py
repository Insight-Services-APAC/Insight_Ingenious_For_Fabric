# Shared Configuration for Data Integration Frameworks
# Used by both Extraction Framework (python/extraction) and File Loading Framework (pyspark/file_loading)

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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

    # Connection parameters (source-type specific)
    connection_params: Dict[str, Any] = field(default_factory=dict)

    # Authentication
    auth_type: Optional[str] = None
    auth_params: Optional[Dict[str, Any]] = None

    # Metadata
    description: Optional[str] = None

    def __post_init__(self):
        if not self.source_type:
            raise ValueError("source_type is required")


# ============================================================================
# RESOURCE CONFIGURATION (Used by BOTH frameworks)
# ============================================================================

@dataclass
class ResourceConfig:
    """
    Complete configuration for a data resource.

    Contains settings for BOTH extraction and file loading.
    Each framework uses only the fields relevant to it.

    Architecture (dlt-inspired quarantine-only pattern):
    - Extraction writes TO raw_landing_path
    - Loading reads FROM raw_landing_path
    - On successful bronze write, files STAY in raw_landing_path (replay-friendly!)
    - On failure, files move to raw_quarantined_path (corrupt/invalid files only)
    - State tracked in log_resource_extract_batch.load_state for easy replay
    """

    # ========================================================================
    # IDENTITY
    # ========================================================================

    resource_name: str                      # Unique identifier (primary key)
    source_name: str                        # Logical source grouping (e.g., "sales", "customer")
    source_config: SourceConfig             # Source connection info

    # ========================================================================
    # RAW LAYER (Used by BOTH frameworks)
    # ========================================================================

    # Raw layer locations (extraction writes to landing, loading only moves failed files to quarantine)
    raw_landing_path: str                   # e.g., "Files/raw/edl/fct_sales/" - files stay here after successful loads!
    raw_quarantined_path: str               # e.g., "Files/raw/quarantined/edl/fct_sales/" - only corrupt/invalid files
    file_format: str                        # 'csv', 'parquet', 'json'

    # ========================================================================
    # EXTRACTION SETTINGS (Used by Extraction Framework ONLY)
    # ========================================================================

    # Source-type specific extraction parameters
    extraction_params: Dict[str, Any] = field(default_factory=dict)

    # ========================================================================
    # LOADING SETTINGS (Used by File Loading Framework ONLY)
    # ========================================================================

    # Loading strategy
    import_mode: str = "incremental"        # 'incremental', 'full'
    batch_by: str = "folder"                # 'file', 'folder'

    # Advanced loading parameters (optional, rarely needed)
    loading_params: Dict[str, Any] = field(default_factory=dict)

    # ========================================================================
    # TARGET SETTINGS (Used by File Loading Framework ONLY)
    # ========================================================================

    target_workspace: str = ""
    target_lakehouse: str = ""              # or target_warehouse
    target_schema: str = ""
    target_table: str = ""

    # ========================================================================
    # WRITE SETTINGS (Used by File Loading Framework ONLY)
    # ========================================================================

    write_mode: str = "overwrite"           # 'overwrite', 'append', 'merge'
    merge_keys: List[str] = field(default_factory=list)
    partition_columns: List[str] = field(default_factory=list)
    enable_schema_evolution: bool = True

    # Soft delete (applies to ALL merge operations - with or without CDC)
    soft_delete_enabled: bool = False       # If True, DELETE becomes UPDATE SET _raw_is_deleted=True

    # CDC configuration (optional, only for CDC sources)
    cdc_config: Optional['CDCConfig'] = None  # None = standard merge (no CDC operations)

    # ========================================================================
    # DATA VALIDATION (Used by File Loading Framework ONLY)
    # ========================================================================

    custom_schema_json: Optional[str] = None
    data_validation_rules: Optional[str] = None
    corrupt_record_tolerance: int = 0  # Max corrupt records allowed (0 = reject any corrupt records)

    # ========================================================================
    # EXECUTION CONTROL (Used by BOTH frameworks)
    # ========================================================================

    execution_group: int = 1
    active: bool = True

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any], source_config: SourceConfig) -> "ResourceConfig":
        """Create ResourceConfig from dictionary"""

        def split_csv(value):
            if not value or not isinstance(value, str):
                return []
            return [item.strip() for item in value.split(",") if item.strip()]

        # Parse CDC config if present
        cdc_config = None
        if "cdc_config" in config_dict and config_dict["cdc_config"]:
            cdc_config = CDCConfig.from_dict(config_dict["cdc_config"])

        return cls(
            resource_name=config_dict["resource_name"],
            source_name=config_dict["source_name"],
            source_config=source_config,
            raw_landing_path=config_dict["raw_landing_path"],
            raw_quarantined_path=config_dict["raw_quarantined_path"],
            file_format=config_dict["file_format"],
            extraction_params=config_dict.get("extraction_params", {}),
            import_mode=config_dict.get("import_mode", "incremental"),
            batch_by=config_dict.get("batch_by", "folder"),
            loading_params=config_dict.get("loading_params", {}),
            target_workspace=config_dict.get("target_workspace", ""),
            target_lakehouse=config_dict.get("target_lakehouse", ""),
            target_schema=config_dict.get("target_schema", ""),
            target_table=config_dict.get("target_table", ""),
            write_mode=config_dict.get("write_mode", "overwrite"),
            merge_keys=split_csv(config_dict.get("merge_keys")),
            partition_columns=split_csv(config_dict.get("partition_columns")),
            enable_schema_evolution=config_dict.get("enable_schema_evolution", True),
            soft_delete_enabled=config_dict.get("soft_delete_enabled", False),
            cdc_config=cdc_config,
            custom_schema_json=config_dict.get("custom_schema_json"),
            data_validation_rules=config_dict.get("data_validation_rules"),
            corrupt_record_tolerance=config_dict.get("corrupt_record_tolerance", 0),
            execution_group=config_dict.get("execution_group", 1),
            active=config_dict.get("active", True),
        )


# ============================================================================
# EXTRACTION PARAMETERS (Used by Extraction Framework)
# ============================================================================

@dataclass
class FileSystemExtractionParams:
    """Parameters for extracting files from inbound to raw layer"""

    # Source location
    inbound_path: str

    # File discovery
    discovery_pattern: str = "*.csv"
    recursive: bool = False
    batch_by: str = "file"  # 'file' (individual files), 'folder' (folder batches), or 'all' (entire directory as one batch)

    # File format (CSV-specific)
    has_header: bool = True
    file_delimiter: str = ","
    encoding: str = "utf-8"
    quote_character: str = '"'
    escape_character: str = "\\"
    multiline_values: bool = True
    null_value: str = ""  # String to treat as NULL when reading CSV

    # Validation
    require_control_file: bool = False
    control_file_pattern: Optional[str] = None
    duplicate_handling: str = "fail"  # 'fail', 'skip', 'allow'
    require_files: bool = False  # If True, fail extraction when no files found

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

    # Raw layer Hive partitioning (REQUIRED)
    # Raw layer is ALWAYS partitioned by process date (when file arrived)
    # Business dates are extracted as columns for querying in bronze
    raw_partition_columns: List[str] = field(default_factory=lambda: ["date"])
    # Supported patterns (based on list length):
    # - 1 column: ["date"] → date=2025-11-09/
    # - 3 columns: ["year", "month", "day"] → year=2025/month=11/day=09/
    # - 4 columns: ["year", "month", "day", "hour"] → year=2025/month=11/day=09/hour=14/
    # Custom names supported: ["ds"], ["process_date"], ["yr", "mo", "dy"], etc.

    partition_depth: Optional[int] = None  # Folder depth for batch_by="folder" (e.g., 3 for YYYY/MM/DD)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for storage in ResourceConfig.extraction_params"""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "FileSystemExtractionParams":
        """Create from dict"""
        return cls(**{k: v for k, v in params.items() if k in cls.__annotations__})


@dataclass
class APIExtractionParams:
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


@dataclass
class DatabaseExtractionParams:
    """Parameters for extracting data from databases"""

    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    query: Optional[str] = None
    watermark_column: Optional[str] = None
    watermark_type: str = "timestamp"
    partition_by_date: bool = True  # Creates YYYY/MM/DD folders in raw
    partition_column: Optional[str] = None  # Column to use for date partitioning

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "DatabaseExtractionParams":
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

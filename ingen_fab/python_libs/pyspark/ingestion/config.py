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

    Architecture:
    - Extraction writes TO raw_file_path (raw layer)
    - Loading reads FROM raw_file_path (raw layer)
    - Single source of truth for file location and format
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

    # Raw layer location (extraction writes here, loading reads from here)
    raw_file_path: str                      # e.g., "Files/raw/vendor_sales/"
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

    # ========================================================================
    # DATA VALIDATION (Used by File Loading Framework ONLY)
    # ========================================================================

    custom_schema_json: Optional[str] = None
    data_validation_rules: Optional[str] = None

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

        return cls(
            resource_name=config_dict["resource_name"],
            source_name=config_dict["source_name"],
            source_config=source_config,
            raw_file_path=config_dict["raw_file_path"],
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
            custom_schema_json=config_dict.get("custom_schema_json"),
            data_validation_rules=config_dict.get("data_validation_rules"),
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

    # Validation
    require_control_file: bool = False
    control_file_pattern: Optional[str] = None
    duplicate_handling: str = "fail"  # 'fail', 'skip', 'allow'
    require_files: bool = False  # If True, fail extraction when no files found

    # Date extraction and output structure
    date_regex: Optional[str] = None  # Regex to extract date from file path (e.g., r"daily_sales_(\d{8})")
    output_structure: str = "{YYYY}/{MM}/{DD}/"  # How to organize in raw layer
    use_process_date: bool = False  # If True, use current execution date instead of extracting from path
    partition_depth: Optional[int] = None  # Folder depth for batch_by="folder" (e.g., 3 for YYYY/MM/DD, 4 for YYYY/MM/DD/HH)

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

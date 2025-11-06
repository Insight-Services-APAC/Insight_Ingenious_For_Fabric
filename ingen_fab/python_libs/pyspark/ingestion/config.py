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
    """

    # ========================================================================
    # IDENTITY
    # ========================================================================

    resource_name: str                      # Unique identifier (primary key)
    source_name: str                        # Logical source grouping (e.g., "sales", "customer")
    source_config: SourceConfig             # Source connection info

    # ========================================================================
    # EXTRACTION SETTINGS (Used by Extraction Framework ONLY)
    # ========================================================================

    # Where extraction writes files (API/DB → Files)
    extraction_output_path: Optional[str] = None

    # Source-type specific extraction parameters
    extraction_params: Dict[str, Any] = field(default_factory=dict)

    # ========================================================================
    # FILE LOADING SETTINGS (Used by File Loading Framework ONLY)
    # ========================================================================

    # Where file loading reads files FROM (should match extraction_output_path!)
    source_file_path: Optional[str] = None
    source_file_format: Optional[str] = None

    # File loading specific parameters
    loading_params: Dict[str, Any] = field(default_factory=dict)

    # ========================================================================
    # TARGET SETTINGS (Used by File Loading Framework ONLY)
    # ========================================================================

    target_workspace_name: str = ""
    target_datastore_name: str = ""
    target_datastore_type: str = ""         # 'lakehouse', 'warehouse'
    target_schema_name: str = ""
    target_table_name: str = ""

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
    active_yn: str = "Y"

    def __post_init__(self):
        if not self.resource_name:
            raise ValueError("resource_name is required")
        if not self.source_name:
            raise ValueError("source_name is required")
        if not self.source_config:
            raise ValueError("source_config is required")

        # Validate write_mode
        if self.write_mode not in ['overwrite', 'append', 'merge']:
            raise ValueError(f"Invalid write_mode: {self.write_mode}")

        # Validate merge requirements
        if self.write_mode == 'merge' and not self.merge_keys:
            raise ValueError("merge_keys required when write_mode='merge'")

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any], source_config: SourceConfig) -> "ResourceConfig":
        """Create ResourceConfig from dictionary"""

        def split_csv(value):
            if not value or not isinstance(value, str):
                return []
            return [item.strip() for item in value.split(",") if item.strip()]

        return cls(
            resource_name=config_dict.get("resource_name", ""),
            source_name=config_dict.get("source_name", ""),
            source_config=source_config,
            extraction_output_path=config_dict.get("extraction_output_path"),
            extraction_params=config_dict.get("extraction_params", {}),
            source_file_path=config_dict.get("source_file_path"),
            source_file_format=config_dict.get("source_file_format"),
            loading_params=config_dict.get("loading_params", {}),
            target_workspace_name=config_dict.get("target_workspace_name", ""),
            target_datastore_name=config_dict.get("target_datastore_name", ""),
            target_datastore_type=config_dict.get("target_datastore_type", ""),
            target_schema_name=config_dict.get("target_schema_name", ""),
            target_table_name=config_dict.get("target_table_name", ""),
            write_mode=config_dict.get("write_mode", "overwrite"),
            merge_keys=split_csv(config_dict.get("merge_keys")),
            partition_columns=split_csv(config_dict.get("partition_columns")),
            enable_schema_evolution=config_dict.get("enable_schema_evolution", True),
            custom_schema_json=config_dict.get("custom_schema_json"),
            data_validation_rules=config_dict.get("data_validation_rules"),
            execution_group=config_dict.get("execution_group", 1),
            active_yn=config_dict.get("active_yn", "Y"),
        )


# ============================================================================
# FILE LOADING PARAMETERS (Used by File Loading Framework)
# ============================================================================

@dataclass
class FileSystemLoadingParams:
    """Parameters for loading files into Delta tables"""

    # File discovery
    import_pattern: str = "full"            # 'incremental', 'full'
    batch_by: str = "all"                    # 'file', 'folder', 'all'
    discovery_pattern: Optional[str] = None  # Glob pattern: "*.csv", "batch_*"
    recursive: bool = True                   # Search subdirectories recursively

    # CSV options
    file_delimiter: str = ","
    has_header: bool = True
    encoding: str = "utf-8"
    quote_character: str = '"'
    escape_character: str = "\\"
    multiline_values: bool = True

    # Date extraction
    date_pattern: Optional[str] = None
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None

    # Duplicate handling
    duplicate_handling: str = "skip"        # 'skip', 'allow', 'fail'

    # Control files
    require_control_file: bool = False
    control_file_pattern: Optional[str] = None

    # Validation
    require_files: bool = False

    # Archive settings
    enable_archive: bool = False
    archive_path: Optional[str] = None  # Template: "archive/{YYYY}/{MM}/{DD}/{filename}"
    cleanup_empty_dirs: bool = False    # Clean up empty directories after archiving

    # Archive destination (optional - defaults to source lakehouse/workspace)
    archive_workspace_name: Optional[str] = None  # Target workspace for archive (None = source workspace)
    archive_lakehouse_name: Optional[str] = None  # Target lakehouse for archive (None = source lakehouse)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for storage in ResourceConfig.loading_params"""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "FileSystemLoadingParams":
        """Create from dict"""
        return cls(**{k: v for k, v in params.items() if k in cls.__annotations__})


# ============================================================================
# EXTRACTION PARAMETERS (Used by Extraction Framework)
# ============================================================================

@dataclass
class APIExtractionParams:
    """Parameters for extracting data from REST APIs"""

    endpoint: str
    method: str = "GET"
    headers: Optional[Dict[str, str]] = None
    query_params: Optional[Dict[str, Any]] = None
    pagination_type: str = "none"

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

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "DatabaseExtractionParams":
        return cls(**{k: v for k, v in params.items() if k in cls.__annotations__})

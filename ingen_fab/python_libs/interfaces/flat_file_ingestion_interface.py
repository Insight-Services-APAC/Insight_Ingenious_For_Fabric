# Flat File Ingestion Interface
# Defines the interface for flat file ingestion components

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class DuplicateFilesError(Exception):
    """Raised when duplicate files are found and duplicate_file_handling='fail'"""
    pass


@dataclass
class FlatFileIngestionConfig:
    """Configuration class for flat file ingestion"""

    # Core configuration
    config_id: str
    source_file_path: str
    source_file_format: str

    # Source location fields (optional - defaults applied during processing)
    source_workspace_id: Optional[str] = None
    source_datastore_id: Optional[str] = None
    source_workspace_name: Optional[str] = None  # Name-based alternative to ID
    source_datastore_name: Optional[str] = None  # Name-based alternative to ID
    source_datastore_type: Optional[str] = None
    source_file_root_path: Optional[str] = None

    # Target location fields
    target_workspace_id: str = ""
    target_datastore_id: str = ""
    target_workspace_name: str = ""  # Name-based alternative to ID
    target_datastore_name: str = ""  # Name-based alternative to ID
    target_datastore_type: str = ""
    target_schema_name: str = ""
    target_table_name: str = ""

    # Optional configuration
    staging_table_name: Optional[str] = None
    file_delimiter: str = ","
    has_header: bool = True
    encoding: str = "utf-8"
    date_format: str = "yyyy-MM-dd"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    schema_inference: bool = True

    # Advanced CSV handling options
    quote_character: str = '"'
    escape_character: str = "\\"
    multiline_values: bool = True
    ignore_leading_whitespace: bool = False
    ignore_trailing_whitespace: bool = False
    null_value: str = ""
    empty_value: str = ""
    comment_character: Optional[str] = None
    max_columns: int = 20480
    max_chars_per_column: int = -1

    # Schema and processing options
    custom_schema_json: Optional[str] = None
    data_validation_rules: Optional[str] = None
    partition_columns: List[str] = None
    sort_columns: List[str] = None
    write_mode: str = "overwrite"
    merge_keys: List[str] = None
    enable_schema_evolution: bool = True
    config_group_id: Optional[str] = None
    execution_group: int = 1
    active_yn: str = "Y"

    # Discovery and Import Configuration
    import_pattern: str = "single_file"  # 'single_file', 'incremental_files', 'incremental_folders'
    discovery_pattern: Optional[str] = None  # Glob pattern: "*.csv", "batch_*", "*/*/*"

    # Date extraction and filtering
    date_pattern: Optional[str] = None  # 'YYYYMMDD', 'YYYY-MM-DD', 'YYYY/MM/DD', 'DD-MM-YYYY'
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None

    # Duplicate handling
    duplicate_handling: str = "skip"  # 'skip', 'allow', 'fail'

    # Validation
    require_files: bool = False

    # Control file settings (validation enabled when pattern is set)
    control_file_pattern: Optional[str] = None  # e.g., "{basename}.CTL", "_SUCCESS"

    def __post_init__(self):
        """Post-initialization validation"""
        # Initialize list fields
        if self.partition_columns is None:
            self.partition_columns = []
        if self.sort_columns is None:
            self.sort_columns = []
        if self.merge_keys is None:
            self.merge_keys = []

        # Validate import_pattern
        valid_patterns = ["single_file", "incremental_files", "incremental_folders"]
        if self.import_pattern not in valid_patterns:
            raise ValueError(
                f"Invalid import_pattern '{self.import_pattern}'. "
                f"Must be one of: {valid_patterns}"
            )

        # Validate duplicate_handling
        valid_duplicate_handling = ["skip", "allow", "fail"]
        if self.duplicate_handling not in valid_duplicate_handling:
            raise ValueError(
                f"Invalid duplicate_handling '{self.duplicate_handling}'. "
                f"Must be one of: {valid_duplicate_handling}"
            )

    @classmethod
    def from_dict(cls, config_row: Dict[str, Any]) -> "FlatFileIngestionConfig":
        """Create configuration from dictionary/row data"""

        # Helper function to split comma-separated values
        def split_csv(value):
            if not value or not isinstance(value, str):
                return []
            return [item.strip() for item in value.split(",") if item.strip()]

        return cls(
            config_id=config_row.get("config_id", ""),
            source_file_path=config_row.get("source_file_path", ""),
            source_file_format=config_row.get("source_file_format", "csv"),
            # Source location fields (optional)
            source_workspace_id=config_row.get("source_workspace_id"),
            source_datastore_id=config_row.get("source_datastore_id"),
            source_workspace_name=config_row.get("source_workspace_name"),
            source_datastore_name=config_row.get("source_datastore_name"),
            source_datastore_type=config_row.get("source_datastore_type"),
            source_file_root_path=config_row.get("source_file_root_path"),
            # Target location fields
            target_workspace_id=config_row.get("target_workspace_id", ""),
            target_datastore_id=config_row.get("target_datastore_id", ""),
            target_workspace_name=config_row.get("target_workspace_name", ""),
            target_datastore_name=config_row.get("target_datastore_name", ""),
            target_datastore_type=config_row.get("target_datastore_type", "lakehouse"),
            target_schema_name=config_row.get("target_schema_name", ""),
            target_table_name=config_row.get("target_table_name", ""),
            staging_table_name=config_row.get("staging_table_name"),
            file_delimiter=config_row.get("file_delimiter", ","),
            has_header=config_row.get("has_header", True),
            encoding=config_row.get("encoding", "utf-8"),
            date_format=config_row.get("date_format", "yyyy-MM-dd"),
            timestamp_format=config_row.get("timestamp_format", "yyyy-MM-dd HH:mm:ss"),
            schema_inference=config_row.get("schema_inference", True),
            quote_character=config_row.get("quote_character", '"'),
            escape_character=config_row.get("escape_character", "\\"),
            multiline_values=config_row.get("multiline_values", True),
            ignore_leading_whitespace=config_row.get(
                "ignore_leading_whitespace", False
            ),
            ignore_trailing_whitespace=config_row.get(
                "ignore_trailing_whitespace", False
            ),
            null_value=config_row.get("null_value", ""),
            empty_value=config_row.get("empty_value", ""),
            comment_character=config_row.get("comment_character"),
            max_columns=config_row.get("max_columns", 20480),
            max_chars_per_column=config_row.get("max_chars_per_column", -1),
            custom_schema_json=config_row.get("custom_schema_json"),
            data_validation_rules=config_row.get("data_validation_rules"),
            partition_columns=split_csv(config_row.get("partition_columns")),
            sort_columns=split_csv(config_row.get("sort_columns")),
            write_mode=config_row.get("write_mode", "overwrite"),
            merge_keys=split_csv(config_row.get("merge_keys")),
            enable_schema_evolution=config_row.get("enable_schema_evolution", True),
            config_group_id=config_row.get("config_group_id"),
            execution_group=config_row.get("execution_group", 1),
            active_yn=config_row.get("active_yn", "Y"),
            # Discovery configuration
            import_pattern=config_row.get("import_pattern", "single_file"),
            discovery_pattern=config_row.get("discovery_pattern"),
            date_pattern=config_row.get("date_pattern"),
            date_range_start=config_row.get("date_range_start"),
            date_range_end=config_row.get("date_range_end"),
            duplicate_handling=config_row.get("duplicate_handling", "skip"),
            require_files=config_row.get("require_files", False),
            # Control file settings (validation enabled when pattern is set)
            control_file_pattern=config_row.get("control_file_pattern"),
        )


@dataclass
class FileDiscoveryResult:
    """Result of file discovery operation"""

    file_path: str
    date_partition: Optional[str] = None
    folder_name: Optional[str] = None
    size: int = 0
    modified_time: Optional[datetime] = None
    nested_structure: bool = False
    filename_attributes: Optional[Dict[str, str]] = None
    control_file_path: Optional[str] = None


@dataclass
class ProcessingMetrics:
    """Metrics for file processing performance"""

    read_duration_ms: int = 0
    write_duration_ms: int = 0
    total_duration_ms: int = 0
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    source_row_count: int = 0
    target_row_count_before: int = 0
    target_row_count_after: int = 0
    row_count_reconciliation_status: str = "not_verified"
    corrupt_records_count: int = 0


@dataclass
class ConfigExecutionResult:
    """Result of configuration execution for summary reporting"""
    config_id: str
    config_name: str
    status: str  # 'pending', 'completed', 'failed', 'no_data_processed'
    metrics: ProcessingMetrics
    errors: List[str]
    files_discovered: int
    files_processed: int
    files_failed: int
    files_skipped: int


class FlatFileDiscoveryInterface(ABC):
    """Interface for file discovery operations"""

    @abstractmethod
    def discover_files(self) -> List[FileDiscoveryResult]:
        """Discover files based on configuration (uses self.config)"""
        pass

    @abstractmethod
    def extract_date_from_folder_name(
        self, folder_name: str, date_format: str
    ) -> Optional[str]:
        """Extract date from folder name based on format"""
        pass

    @abstractmethod
    def extract_date_from_path(
        self, file_path: str, base_path: str, date_format: str
    ) -> Optional[str]:
        """Extract date from file path based on format"""
        pass

    @abstractmethod
    def is_date_in_range(self, date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        pass

    @abstractmethod
    def date_already_processed(self, date_partition: str) -> bool:
        """Check if a date has already been processed (uses self.config)"""
        pass


class FlatFileProcessorInterface(ABC):
    """Interface for flat file processing operations"""

    @abstractmethod
    def read_file(self, file: FileDiscoveryResult) -> Tuple[Any, ProcessingMetrics]:
        """Read a file based on configuration (uses self.config) and return DataFrame with metrics"""
        pass

    @abstractmethod
    def read_folder(self, folder_path: str) -> Tuple[Any, ProcessingMetrics]:
        """Read all files in a folder based on configuration (uses self.config)"""
        pass

    @abstractmethod
    def write_data(self, data: Any) -> ProcessingMetrics:
        """Write data to target destination (uses self.config)"""
        pass

    @abstractmethod
    def validate_data(self, data: Any) -> Dict[str, Any]:
        """Validate data based on configuration rules (uses self.config)"""
        pass


class FlatFileLoggingInterface(ABC):
    """Interface for flat file ingestion logging and monitoring"""

    @abstractmethod
    def log_execution_start(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Log the start of a file processing execution"""
        pass

    @abstractmethod
    def log_execution_completion(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        metrics: ProcessingMetrics,
        status: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Log the completion of a file processing execution"""
        pass

    @abstractmethod
    def log_execution_error(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        error_message: str,
        error_details: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Log an execution error"""
        pass

    @abstractmethod
    def log_duplicate_file(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        file_result: FileDiscoveryResult,
    ) -> None:
        """Log a duplicate file that was skipped"""
        pass


class FlatFileIngestionOrchestrator(ABC):
    """Main orchestrator interface for flat file ingestion"""

    def __init__(
        self,
        discovery_service: FlatFileDiscoveryInterface,
        processor_service: FlatFileProcessorInterface,
        logging_service: FlatFileLoggingInterface,
    ):
        self.discovery_service = discovery_service
        self.processor_service = processor_service
        self.logging_service = logging_service

    @abstractmethod
    def process_configuration(
        self, config: FlatFileIngestionConfig, execution_id: str
    ) -> ConfigExecutionResult:
        """Process a single configuration"""
        pass

    @abstractmethod
    def process_configurations(
        self, configs: List[FlatFileIngestionConfig], execution_id: str
    ) -> Dict[str, Any]:
        """Process multiple configurations"""
        pass

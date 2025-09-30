# Flat File Ingestion Interface
# Defines the interface for flat file ingestion components

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class FlatFileIngestionConfig:
    """Configuration class for flat file ingestion"""

    # Core configuration
    config_id: str
    config_name: str
    source_file_path: str
    source_file_format: str

    # Source location fields (optional - defaults applied during processing)
    source_workspace_id: Optional[str] = None
    source_datastore_id: Optional[str] = None
    source_datastore_type: Optional[str] = None
    source_file_root_path: Optional[str] = None

    # Target location fields
    target_workspace_id: str = ""
    target_datastore_id: str = ""
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
    partition_columns: List[str] = None
    sort_columns: List[str] = None
    write_mode: str = "overwrite"
    merge_keys: List[str] = None
    data_validation_rules: Optional[str] = None
    error_handling_strategy: str = "fail"
    execution_group: int = 1
    active_yn: str = "Y"

    # Date partitioning and batch processing
    import_pattern: str = "single_file"
    date_partition_format: str = "YYYY/MM/DD"
    table_relationship_group: Optional[str] = None
    batch_import_enabled: bool = False
    file_discovery_pattern: Optional[str] = None
    import_sequence_order: int = 1
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    skip_existing_dates: bool = True
    source_is_folder: bool = False

    # Hierarchical nested structure support
    table_subfolder: Optional[str] = None  # Table name within date partition (e.g., "orders", "customers")
    hierarchical_date_structure: bool = False  # Enable YYYY/MM/DD hierarchical date discovery
    nested_path_separator: str = "/"  # Path separator for nested structures

    def __post_init__(self):
        """Post-initialization processing"""
        if self.partition_columns is None:
            self.partition_columns = []
        if self.sort_columns is None:
            self.sort_columns = []
        if self.merge_keys is None:
            self.merge_keys = []

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
            config_name=config_row.get("config_name", ""),
            source_file_path=config_row.get("source_file_path", ""),
            source_file_format=config_row.get("source_file_format", "csv"),
            # Source location fields (optional)
            source_workspace_id=config_row.get("source_workspace_id"),
            source_datastore_id=config_row.get("source_datastore_id"),
            source_datastore_type=config_row.get("source_datastore_type"),
            source_file_root_path=config_row.get("source_file_root_path"),
            # Target location fields
            target_workspace_id=config_row.get("target_workspace_id", ""),
            target_datastore_id=config_row.get("target_datastore_id", ""),
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
            ignore_leading_whitespace=config_row.get("ignore_leading_whitespace", False),
            ignore_trailing_whitespace=config_row.get("ignore_trailing_whitespace", False),
            null_value=config_row.get("null_value", ""),
            empty_value=config_row.get("empty_value", ""),
            comment_character=config_row.get("comment_character"),
            max_columns=config_row.get("max_columns", 20480),
            max_chars_per_column=config_row.get("max_chars_per_column", -1),
            custom_schema_json=config_row.get("custom_schema_json"),
            partition_columns=split_csv(config_row.get("partition_columns")),
            sort_columns=split_csv(config_row.get("sort_columns")),
            write_mode=config_row.get("write_mode", "overwrite"),
            merge_keys=split_csv(config_row.get("merge_keys")),
            data_validation_rules=config_row.get("data_validation_rules"),
            error_handling_strategy=config_row.get("error_handling_strategy", "fail"),
            execution_group=config_row.get("execution_group", 1),
            active_yn=config_row.get("active_yn", "Y"),
            import_pattern=config_row.get("import_pattern", "single_file"),
            date_partition_format=config_row.get("date_partition_format", "YYYY/MM/DD"),
            table_relationship_group=config_row.get("table_relationship_group"),
            batch_import_enabled=config_row.get("batch_import_enabled", False),
            file_discovery_pattern=config_row.get("file_discovery_pattern"),
            import_sequence_order=config_row.get("import_sequence_order", 1),
            date_range_start=config_row.get("date_range_start"),
            date_range_end=config_row.get("date_range_end"),
            skip_existing_dates=config_row.get("skip_existing_dates", True),
            source_is_folder=config_row.get("source_is_folder", False),
            # Hierarchical nested structure support
            table_subfolder=config_row.get("table_subfolder"),
            hierarchical_date_structure=config_row.get("hierarchical_date_structure", False),
            nested_path_separator=config_row.get("nested_path_separator", "/"),
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
    records_failed: int = 0
    source_row_count: int = 0
    target_row_count_before: int = 0
    target_row_count_after: int = 0
    row_count_reconciliation_status: str = "not_verified"
    avg_rows_per_second: float = 0.0
    data_size_mb: float = 0.0
    throughput_mb_per_second: float = 0.0


class FlatFileDiscoveryInterface(ABC):
    """Interface for file discovery operations"""

    @abstractmethod
    def discover_files(self, config: FlatFileIngestionConfig) -> List[FileDiscoveryResult]:
        """Discover files based on configuration"""
        pass

    @abstractmethod
    def extract_date_from_folder_name(self, folder_name: str, date_format: str) -> Optional[str]:
        """Extract date from folder name based on format"""
        pass

    @abstractmethod
    def extract_date_from_path(self, file_path: str, base_path: str, date_format: str) -> Optional[str]:
        """Extract date from file path based on format"""
        pass

    @abstractmethod
    def is_date_in_range(self, date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        pass

    @abstractmethod
    def date_already_processed(self, date_partition: str, config: FlatFileIngestionConfig) -> bool:
        """Check if a date has already been processed"""
        pass


class FlatFileProcessorInterface(ABC):
    """Interface for flat file processing operations"""

    @abstractmethod
    def read_file(self, config: FlatFileIngestionConfig, file: FileDiscoveryResult) -> Tuple[Any, ProcessingMetrics]:
        """Read a file based on configuration and return DataFrame with metrics"""
        pass

    @abstractmethod
    def read_folder(self, config: FlatFileIngestionConfig, folder_path: str) -> Tuple[Any, ProcessingMetrics]:
        """Read all files in a folder based on configuration"""
        pass

    @abstractmethod
    def write_data(self, data: Any, config: FlatFileIngestionConfig) -> ProcessingMetrics:
        """Write data to target destination"""
        pass

    @abstractmethod
    def validate_data(self, data: Any, config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Validate data based on configuration rules"""
        pass


class FlatFileLoggingInterface(ABC):
    """Interface for flat file ingestion logging and monitoring"""

    @abstractmethod
    def log_execution_start(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        partition_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Log the start of a file processing execution"""
        pass

    @abstractmethod
    def log_execution_completion(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        metrics: ProcessingMetrics,
        status: str,
        partition_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Log the completion of a file processing execution"""
        pass

    @abstractmethod
    def log_execution_error(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        error_message: str,
        error_details: str,
        partition_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Log an execution error"""
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
    def process_configuration(self, config: FlatFileIngestionConfig, execution_id: str) -> Dict[str, Any]:
        """Process a single configuration"""
        pass

    @abstractmethod
    def process_configurations(self, configs: List[FlatFileIngestionConfig], execution_id: str) -> Dict[str, Any]:
        """Process multiple configurations"""
        pass

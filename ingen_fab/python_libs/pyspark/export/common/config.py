"""Configuration dataclasses for the export framework."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ingen_fab.python_libs.pyspark.export.common.constants import (
    FileFormat,
    CompressionType,
    SourceType,
)


@dataclass
class FileFormatParams:
    """File format configuration for exports."""

    file_format: str = "csv"
    compression: Optional[str] = None
    format_options: Dict[str, Any] = field(default_factory=dict)

    # CSV/TSV/DAT specific options
    delimiter: str = ","
    has_header: bool = True
    quote_char: str = '"'
    escape_char: str = "\\"
    null_value: str = ""

    def __post_init__(self):
        """Validate file_format on construction."""
        valid_formats = {f.value for f in FileFormat}
        if self.file_format not in valid_formats:
            raise ValueError(
                f"Unsupported file_format: '{self.file_format}'. "
                f"Valid options: {', '.join(sorted(valid_formats))}"
            )

    def to_spark_options(self) -> Dict[str, Any]:
        """Convert to Spark DataFrame write options."""
        options = dict(self.format_options)

        if self.file_format in [FileFormat.CSV, FileFormat.TSV, FileFormat.DAT]:
            options["header"] = str(self.has_header).lower()

            # Set delimiter based on format
            if self.file_format == FileFormat.TSV:
                options["sep"] = "\t"
            elif self.file_format == FileFormat.DAT:
                options["sep"] = self.delimiter if self.delimiter else "|"
            else:
                options["sep"] = self.delimiter

            options["quote"] = self.quote_char
            options["escape"] = self.escape_char
            options["nullValue"] = self.null_value

            if self.compression and self.compression != CompressionType.NONE:
                options["compression"] = self.compression

        elif self.file_format == FileFormat.PARQUET:
            if self.compression and self.compression != CompressionType.NONE:
                options["compression"] = self.compression
            else:
                options["compression"] = "snappy"  # Default for Parquet

        elif self.file_format == FileFormat.JSON:
            if self.compression and self.compression != CompressionType.NONE:
                options["compression"] = self.compression

        return options


@dataclass
class ExportSourceConfig:
    """Source configuration for export."""

    source_type: str  # "lakehouse" or "warehouse"
    source_workspace: str
    source_datastore: str  # Lakehouse or Warehouse name
    source_schema: Optional[str] = None  # Schema name (warehouse)
    source_table: Optional[str] = None  # Table name (mutually exclusive with query)
    source_query: Optional[str] = None  # Custom SQL query


@dataclass
class ExportConfig:
    """Full export configuration."""

    export_group_name: str
    export_name: str
    is_active: bool
    execution_group: int

    # Source configuration
    source_config: ExportSourceConfig

    # Target configuration (Lakehouse Files only)
    target_workspace: str
    target_lakehouse: str
    target_path: str  # Path within Files/, e.g., "exports/sales/"
    target_filename_pattern: Optional[str] = None  # e.g., "sales_{date}.csv"
    compressed_filename_pattern: Optional[str] = None  # e.g., "sales_{date}.zip" for zipdeflate

    # File format configuration
    file_format_params: FileFormatParams = field(default_factory=FileFormatParams)

    # Optional features
    max_rows_per_file: Optional[int] = None  # File splitting
    compress_bundle_files: bool = False  # Bundle all files into one ZIP (vs individual ZIPs)

    # Trigger file configuration
    trigger_file_enabled: bool = False
    trigger_file_pattern: str = "{filename}.done"

    # Extract type config (for period date calculation)
    extract_type: Optional[str] = None  # "full", "period", "incremental"
    period_length_days: Optional[int] = None  # Days in period window (period type only)
    period_end_day: Optional[str] = None  # Day of week period ends, e.g., "Saturday"
    incremental_column: Optional[str] = None  # Column for incremental tracking

    # Metadata
    description: Optional[str] = None

    def __post_init__(self):
        """Validate config on construction."""
        if self.extract_type == "incremental" and not self.incremental_column:
            raise ValueError("incremental_column is required when extract_type='incremental'")

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "ExportConfig":
        """Create ExportConfig from a dictionary/row."""
        # Parse source config
        source_config = ExportSourceConfig(
            source_type=row.get("source_type", SourceType.LAKEHOUSE),
            source_workspace=row.get("source_workspace", ""),
            source_datastore=row.get("source_datastore", ""),
            source_schema=row.get("source_schema"),
            source_table=row.get("source_table"),
            source_query=row.get("source_query"),
        )

        # Parse file format params
        format_options = row.get("format_options", {})
        if isinstance(format_options, str):
            import json
            try:
                format_options = json.loads(format_options)
            except (json.JSONDecodeError, TypeError):
                format_options = {}

        file_format_params = FileFormatParams(
            file_format=row.get("file_format", FileFormat.CSV),
            compression=row.get("compression"),
            format_options=format_options,
            delimiter=row.get("delimiter", ","),
            has_header=row.get("has_header", True),
            quote_char=row.get("quote_char", '"'),
            escape_char=row.get("escape_char", "\\"),
            null_value=row.get("null_value", ""),
        )

        return cls(
            export_group_name=row.get("export_group_name", ""),
            export_name=row.get("export_name", ""),
            is_active=row.get("is_active", True),
            execution_group=row.get("execution_group", 1),
            source_config=source_config,
            target_workspace=row.get("target_workspace", ""),
            target_lakehouse=row.get("target_lakehouse", ""),
            target_path=row.get("target_path", ""),
            target_filename_pattern=row.get("target_filename_pattern"),
            compressed_filename_pattern=row.get("compressed_filename_pattern"),
            file_format_params=file_format_params,
            max_rows_per_file=row.get("max_rows_per_file"),
            compress_bundle_files=row.get("compress_bundle_files", False),
            trigger_file_enabled=row.get("trigger_file_enabled", False),
            trigger_file_pattern=row.get("trigger_file_pattern", "{filename}.done"),
            extract_type=row.get("extract_type"),
            period_length_days=row.get("period_length_days"),
            period_end_day=row.get("period_end_day"),
            incremental_column=row.get("incremental_column"),
            description=row.get("description"),
        )

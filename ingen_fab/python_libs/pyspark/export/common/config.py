"""Configuration dataclasses for the export framework."""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ingen_fab.python_libs.pyspark.export.common.constants import (
    CompressionType,
    FileFormat,
    SourceType,
)


@dataclass
class FileFormatParams:
    """File format configuration for exports.

    All file format options (header, sep, quote, etc.) go in file_format_options.
    No defaults - config table is the source of truth.
    """

    file_format: str
    file_format_options: Dict[str, Any] = field(default_factory=dict)
    compression: Optional[str] = None
    compression_level: Optional[int] = None

    def __post_init__(self):
        """Validate file_format, compression, and compression_level on construction."""
        valid_formats = {f.value for f in FileFormat}
        if self.file_format not in valid_formats:
            raise ValueError(
                f"Unsupported file_format: '{self.file_format}'. "
                f"Valid options: {', '.join(sorted(valid_formats))}"
            )

        # Validate compression against enum if set
        if self.compression:
            valid_compressions = {c.value for c in CompressionType}
            if self.compression not in valid_compressions:
                raise ValueError(
                    f"Invalid compression: '{self.compression}'. "
                    f"Valid options: {', '.join(sorted(valid_compressions))}"
                )

        # Validate compression_level if provided
        if self.compression_level is not None:
            if not self.compression:
                raise ValueError("compression_level requires compression to be set")
            self._validate_compression_level()

    def _validate_compression_level(self):
        """Validate compression_level is acceptable for the compression type."""
        level = self.compression_level
        compression = self.compression

        # Define valid ranges per compression type
        valid_ranges = {
            "gzip": (1, 9),
            "zip": (0, 9),
            "zipdeflate": (0, 9),
            "brotli": (0, 11),
        }

        # Types that don't support levels
        no_level_types = {"snappy", "lz4", "none", None}

        if compression in no_level_types:
            raise ValueError(
                f"compression_level not supported for compression type '{compression}'. "
                f"Supported types: gzip, zip, zipdeflate, brotli"
            )

        if compression in valid_ranges:
            min_val, max_val = valid_ranges[compression]
            if not (min_val <= level <= max_val):
                raise ValueError(
                    f"compression_level {level} invalid for '{compression}'. "
                    f"Valid range: {min_val}-{max_val}"
                )

    def to_spark_options(self) -> Dict[str, Any]:
        """Pass file_format_options to Spark as-is, plus compression if set."""
        options = dict(self.file_format_options)

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
    source_columns: Optional[List[str]] = None  # Column list (used with source_table)

    def __post_init__(self):
        """Validate source configuration on construction."""
        # Validate source_type against enum
        valid_types = {t.value for t in SourceType}
        if self.source_type not in valid_types:
            raise ValueError(
                f"Invalid source_type: '{self.source_type}'. "
                f"Valid options: {', '.join(sorted(valid_types))}"
            )

        # Require source_table OR source_query (not both, not neither)
        if self.source_table and self.source_query:
            raise ValueError("Cannot specify both source_table and source_query")
        if not self.source_table and not self.source_query:
            raise ValueError("Must specify either source_table or source_query")

        # Warehouse requires schema
        if self.source_type == SourceType.WAREHOUSE and not self.source_schema:
            raise ValueError("source_schema is required for warehouse source_type")

        # source_query and source_columns are mutually exclusive
        if self.source_query and self.source_columns:
            raise ValueError("Cannot specify both source_query and source_columns")


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

    # Trigger file configuration (None = disabled)
    trigger_file_pattern: Optional[str] = None

    # Extract type config
    extract_type: Optional[str] = None  # "full", "incremental", "period"
    incremental_column: Optional[str] = None  # Column for incremental/watermark tracking
    incremental_initial_watermark: Optional[str] = None  # Starting point for first incremental run
    period_filter_column: Optional[str] = None  # Column for period-based filtering

    # Period date query - SQL returning start_date, end_date columns
    # e.g., "SELECT * FROM dbo.fn_GetFiscalBounds('{run_date}')"
    period_date_query: Optional[str] = None

    # Metadata
    description: Optional[str] = None

    def __post_init__(self):
        """Validate config on construction."""
        # Validate extract_type value
        valid_extract_types = {"full", "incremental", "period", None}
        if self.extract_type not in valid_extract_types:
            raise ValueError(
                f"Invalid extract_type: '{self.extract_type}'. "
                f"Valid options: full, incremental, period"
            )

        # Extract type cross-field validation
        if self.extract_type == "incremental" and not self.incremental_column:
            raise ValueError("incremental_column is required when extract_type='incremental'")

        # Period exports validation
        if self.extract_type == "period":
            if not self.period_date_query:
                raise ValueError("period_date_query is required when extract_type='period'")

            # source_table mode requires period_filter_column for auto-filtering
            # source_query mode: user handles filtering via placeholders (no validation)
            if self.source_config.source_table and not self.period_filter_column:
                raise ValueError(
                    "Period exports with source_table require period_filter_column"
                )

        # Validate required target fields
        if not self.target_workspace:
            raise ValueError("target_workspace is required")
        if not self.target_lakehouse:
            raise ValueError("target_lakehouse is required")
        if not self.target_path:
            raise ValueError("target_path is required")

        # Validate max_rows_per_file
        if self.max_rows_per_file is not None and self.max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must be > 0")

        # Validate execution_group
        if self.execution_group <= 0:
            raise ValueError("execution_group must be > 0")

        # Validate filename patterns have valid placeholders
        if self.target_filename_pattern:
            self._validate_filename_pattern(
                self.target_filename_pattern, "target_filename_pattern"
            )
        if self.compressed_filename_pattern:
            self._validate_filename_pattern(
                self.compressed_filename_pattern, "compressed_filename_pattern"
            )

        # Validate trigger_file_pattern (same placeholders as filename patterns)
        if self.trigger_file_pattern:
            self._validate_filename_pattern(
                self.trigger_file_pattern, "trigger_file_pattern"
            )

        # Validate compressed_filename_pattern requires compression
        if self.compressed_filename_pattern and not self.file_format_params.compression:
            raise ValueError("compressed_filename_pattern requires compression to be set")

    def _validate_filename_pattern(self, pattern: str, field_name: str):
        """Validate filename pattern has only known placeholders."""
        # Find all {placeholder} or {placeholder:format} patterns
        placeholders = re.findall(r"\{([^}:]+)(?::[^}]*)?\}", pattern)
        valid_placeholders = {
            "run_date",
            "process_date",
            "period_start_date",
            "period_end_date",
            "export_name",
            "run_id",
            "part",
        }
        invalid = set(placeholders) - valid_placeholders
        if invalid:
            raise ValueError(
                f"Invalid placeholder(s) in {field_name}: {invalid}. "
                f"Valid: {sorted(valid_placeholders)}"
            )


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
            source_columns=row.get("source_columns"),
        )

        # Parse file format params - no defaults, config table is source of truth
        file_format_options = row.get("file_format_options") or {}
        if isinstance(file_format_options, str):
            import json
            try:
                file_format_options = json.loads(file_format_options)
            except (json.JSONDecodeError, TypeError):
                file_format_options = {}

        file_format_params = FileFormatParams(
            file_format=row.get("file_format"),
            file_format_options=file_format_options,
            compression=row.get("compression"),
            compression_level=row.get("compression_level"),
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
            trigger_file_pattern=row.get("trigger_file_pattern"),
            extract_type=row.get("extract_type"),
            incremental_column=row.get("incremental_column"),
            incremental_initial_watermark=row.get("incremental_initial_watermark"),
            period_filter_column=row.get("period_filter_column"),
            period_date_query=row.get("period_date_query"),
            description=row.get("description"),
        )

# Database Extractor (Pipeline Mode)
# Extracts data from on-premises databases via Fabric Pipeline delegation

import asyncio
import logging
import re
import time
import uuid
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union, cast

import nest_asyncio

from pyspark.sql.functions import max as spark_max, col

from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    DatabaseExtractionParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.base_extractor import BaseExtractor
from ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.sql_dialects import (
    get_dialect,
    build_select,
    build_cetas_wrapper,
    format_value,
)
from ingen_fab.python_libs.pyspark.ingestion.extraction.extraction_logger import ExtractionLogger
from ingen_fab.python_libs.pyspark.ingestion.common.results import BatchExtractionResult
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """Resolved pipeline identifiers for extraction."""
    pipeline_utils: PipelineUtils
    workspace_id: str
    pipeline_id: str
    source_connection_id: str
    source_database: str


class DatabaseExtractor(BaseExtractor[DatabaseExtractionParams], source_type="database"):
    """
    Extractor for database sources via Fabric Pipeline delegation.

    Automatically registered for source_type="database".
    Handles all database types (sql_server, postgres, mysql, oracle) via db_type parameter.

    Uses Fabric Data Pipelines as a connectivity proxy for databases (supports Data Gateway).

    Features:
    - Full and incremental extraction with watermark tracking
    - Simplified single-call pattern (no partitioning)
    - Watermark calculation via file read-back
    - Works with any database supported by Data Gateway

    Implements BaseExtractor interface - yields BatchExtractionResult objects.
    """

    @classmethod
    def from_config(
        cls,
        config: ResourceConfig,
        extraction_logger: ExtractionLogger,
    ) -> "DatabaseExtractor":
        """
        Production constructor.

        Args:
            config: ResourceConfig with database source configuration
            extraction_logger: ExtractionLogger instance

        Returns:
            Fully initialized DatabaseExtractor
        """
        return cls(
            resource_config=config,
            extraction_logger=extraction_logger,
        )

    def __init__(
        self,
        resource_config: ResourceConfig,
        extraction_logger: ExtractionLogger,
    ):
        """
        Initialize database extractor.

        Args:
            resource_config: Resource configuration with database extraction params
            extraction_logger: Logger instance for batch tracking
        """
        self.config = resource_config
        self.extraction_logger = extraction_logger
        self.logger = logger  # Module logger (context added via filter)

        # Parse extraction params (support dict or dataclass instance)
        if isinstance(resource_config.source_extraction_params, dict):
            self.extraction_params = DatabaseExtractionParams.from_dict(
                resource_config.source_extraction_params
            )
        elif hasattr(resource_config.source_extraction_params, 'to_dict'):
            # Duck typing: if it has to_dict(), it's a params object
            # (Handles module identity issues in notebooks where isinstance fails)
            self.extraction_params = cast(DatabaseExtractionParams, resource_config.source_extraction_params)
        else:
            raise ValueError(
                f"source_extraction_params must be dict or have to_dict() method, "
                f"got {type(resource_config.source_extraction_params)}"
            )

        self.connection_params = resource_config.source_config.source_connection_params

        # Get Spark session from extraction logger
        self.spark = extraction_logger.lakehouse.spark

        # Storage lakehouse for reading back extracted files
        self.storage_lakehouse = lakehouse_utils(
            target_workspace_name=resource_config.extract_storage_workspace,
            target_lakehouse_name=resource_config.extract_storage_lakehouse,
            spark=self.spark,
        )

    @property
    def source_path(self) -> str:
        """Source identifier for logging and batch tracking."""
        schema = self.extraction_params.source_schema or ''
        table = self.extraction_params.source_table or 'custom_query'
        return f"{schema}.{table}" if schema else table

    def extract(self) -> Generator[BatchExtractionResult, None, None]:
        """
        Extract data from database source via Fabric Pipeline.

        Yields:
            BatchExtractionResult: One result per batch extracted
        """
        yield from self._extract_via_pipeline()

    def _build_query(
        self,
        watermark_value: Optional[Any] = None,
        output_path: Optional[str] = None,
        range_start: Optional[Any] = None,
        range_end: Optional[Any] = None,
        range_start_inclusive: bool = False,
        batch_id: Optional[str] = None,
    ) -> str:
        """
        Build SQL query for extraction based on extraction_mode.

        Modes:
        - "table": Generate SELECT from source_table config
        - "query": Use provided SQL query (with optional watermark placeholder)
        - "cetas": Wrap SELECT in Synapse CETAS statement

        Args:
            watermark_value: Last watermark value (for incremental extraction)
            output_path: Output path (required for CETAS mode)
            range_start: Range start value (exclusive by default, >= if range_start_inclusive)
            range_end: Range end value (exclusive, <) - pass day AFTER last day to include
            range_start_inclusive: If True, use >= for range_start; if False, use >
            batch_id: Batch ID for generating unique external table names (required for CETAS mode)

        Returns:
            SQL query string (SELECT for table/query modes, CETAS for cetas mode)

        Raises:
            ValueError: If required fields are missing
        """
        dialect = get_dialect(self.extraction_params.db_type)
        extraction_mode = self.extraction_params.extraction_mode

        # Build the inner SELECT query
        if self.extraction_params.query:
            # Query-based: Use provided SQL (with optional watermark placeholder)
            select_query = self.extraction_params.query

            if watermark_value is not None and self.extraction_params.incremental_column:
                # Format watermark value using dialect and replace placeholder
                formatted_watermark = format_value(watermark_value, dialect)
                select_query = select_query.format(incremental_value=formatted_watermark)
        else:
            # Table-based: Generate SELECT using dialect-aware function
            # source_table is guaranteed by config validation, assert for type narrowing
            assert self.extraction_params.source_table is not None
            select_query = build_select(
                table=self.extraction_params.source_table,
                dialect=dialect,
                schema=self.extraction_params.source_schema,
                columns=self.extraction_params.columns,
                where_clause=self.extraction_params.where_clause,
                incremental_column=self.extraction_params.incremental_column,
                watermark_value=watermark_value,
                range_start=range_start,
                range_end=range_end,
                range_start_inclusive=range_start_inclusive,
            )

        # For CETAS mode, wrap in CETAS statement
        if extraction_mode == "cetas":
            if not output_path:
                raise ValueError("CETAS mode requires output_path")
            if not batch_id:
                raise ValueError("CETAS mode requires batch_id")

            # Generate unique external table name: {schema}.{resource_name}_{batch_id_short}
            # Sanitize resource_name for SQL identifier (replace non-alphanumeric with _)
            safe_resource_name = re.sub(r'[^a-zA-Z0-9_]', '_', self.config.resource_name)
            batch_id_short = batch_id.replace('-', '')[:8]
            external_table = f"{self.extraction_params.cetas_external_schema}.{safe_resource_name}_{batch_id_short}"

            return build_cetas_wrapper(
                select_query=select_query,
                output_path=output_path,
                data_source=self.extraction_params.cetas_data_source,
                file_format=self.extraction_params.cetas_file_format,
                external_table=external_table,
            )

        return select_query

    # ========================================================================
    # CHUNKED EXTRACTION HELPERS
    # ========================================================================

    def _parse_incremental_value(
        self,
        value: str,
        column_type: str,
    ) -> Union[date, datetime, int]:
        """
        Parse incremental_start/end string to typed value.

        Args:
            value: String value (ISO date/datetime or integer string)
            column_type: "date", "timestamp", or "integer"

        Returns:
            Typed value (date, datetime, or int)

        Raises:
            ValueError: If value cannot be parsed for the given column_type
        """
        if column_type == "integer":
            return int(value)
        elif column_type == "date":
            # Parse ISO date (YYYY-MM-DD)
            return datetime.strptime(value, "%Y-%m-%d").date()
        elif column_type == "timestamp":
            # Parse ISO datetime (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
            if "T" in value or " " in value:
                # Full datetime
                value = value.replace("T", " ")
                # Handle optional microseconds
                if "." in value:
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            else:
                # Date only - assume start of day
                return datetime.strptime(value, "%Y-%m-%d")
        else:
            raise ValueError(f"Unknown column_type: {column_type}")

    def _apply_lookback(
        self,
        value: Union[date, datetime, int],
        lookback: int,
        column_type: str,
    ) -> Union[date, datetime, int]:
        """
        Apply lookback period to a value.

        For date/timestamp: subtracts lookback hours
        For integer: subtracts lookback as offset

        Args:
            value: The value to apply lookback to
            lookback: Lookback amount (hours for date/timestamp, offset for integer)
            column_type: "date", "timestamp", or "integer"

        Returns:
            Value with lookback applied
        """
        if column_type == "integer":
            if not isinstance(value, int):
                raise ValueError(f"Expected int for integer column_type, got {type(value)}")
            return value - lookback
        elif column_type == "date":
            if isinstance(value, datetime):
                dt = value
            elif isinstance(value, date):
                dt = datetime.combine(value, datetime.min.time())
            else:
                raise ValueError(f"Expected date or datetime for date column_type, got {type(value)}")
            dt = dt - timedelta(hours=lookback)
            return dt.date()
        elif column_type == "timestamp":
            if isinstance(value, datetime):
                return value - timedelta(hours=lookback)
            elif isinstance(value, date):
                dt = datetime.combine(value, datetime.min.time())
                return dt - timedelta(hours=lookback)
            else:
                raise ValueError(f"Expected datetime for timestamp column_type, got {type(value)}")
        else:
            raise ValueError(f"Unknown column_type: {column_type}")

    def _generate_ranges(
        self,
        start: Union[date, datetime, int],
        end: Union[date, datetime, int],
        interval: int,
        column_type: str,
    ) -> List[Tuple[Any, Any]]:
        """
        Generate (start, end) tuples for chunked extraction.

        Both start and end in each tuple are used with < operator (exclusive).
        The caller passes range_start_inclusive to control >= vs > for start.

        Args:
            start: Start value (watermark or incremental_start)
            end: End value (exclusive) - day AFTER last day to include
            interval: Chunk size (hours for date/timestamp, step for integer)
            column_type: "date", "timestamp", or "integer"

        Returns:
            List of (range_start, range_end) tuples where range_end is exclusive
        """
        ranges: List[Tuple[Any, Any]] = []

        if column_type == "integer":
            if not isinstance(start, int) or not isinstance(end, int):
                raise ValueError("Expected int values for integer column_type")
            current = start
            while current < end:
                next_val = current + interval
                ranges.append((current, min(next_val, end)))
                current = next_val

        elif column_type == "date":
            # Convert to datetime for arithmetic, return dates
            if isinstance(start, datetime):
                current_dt = start
            elif isinstance(start, date):
                current_dt = datetime.combine(start, datetime.min.time())
            else:
                raise ValueError(f"Expected date for date column_type, got {type(start)}")

            if isinstance(end, datetime):
                end_dt = end
            elif isinstance(end, date):
                end_dt = datetime.combine(end, datetime.min.time())
            else:
                raise ValueError(f"Expected date for date column_type, got {type(end)}")

            delta = timedelta(hours=interval)
            while current_dt < end_dt:
                next_dt = current_dt + delta
                ranges.append((current_dt.date(), min(next_dt, end_dt).date()))
                current_dt = next_dt

        elif column_type == "timestamp":
            # Work with datetime values
            if isinstance(start, datetime):
                current_dt = start
            elif isinstance(start, date):
                current_dt = datetime.combine(start, datetime.min.time())
            else:
                raise ValueError(f"Expected datetime for timestamp column_type, got {type(start)}")

            if isinstance(end, datetime):
                end_dt = end
            elif isinstance(end, date):
                end_dt = datetime.combine(end, datetime.min.time())
            else:
                raise ValueError(f"Expected datetime for timestamp column_type, got {type(end)}")

            delta = timedelta(hours=interval)
            while current_dt < end_dt:
                next_dt = current_dt + delta
                ranges.append((current_dt, min(next_dt, end_dt)))
                current_dt = next_dt

        else:
            raise ValueError(f"Unknown column_type: {column_type}")

        return ranges

    # ========================================================================
    # PIPELINE EXTRACTION
    # ========================================================================

    def _build_pipeline_payload(
        self,
        query: str,
        output_path: str,
        source_connection_id: str,
        source_database: str,
    ) -> Dict[str, Any]:
        """
        Build payload for Fabric Pipeline invocation.

        The pipeline executes the query using the specified connection
        and writes output to the specified path.

        Args:
            query: Complete SQL query to execute (SELECT or CETAS)
            output_path: Lakehouse path for output (e.g., Files/raw/db/sales/)
            source_connection_id: Fabric connection GUID to use for the query
            source_database: Database name to query (e.g., "sample_wh")

        Returns:
            Pipeline payload dict matching Fabric Pipeline parameter format
        """
        return {
            "executionData": {
                "parameters": {
                    "source_query": query,
                    "output_path": output_path,
                    "source_connection_id": source_connection_id,
                    "source_database": source_database,
                }
            }
        }

    def _get_pipeline_context(self) -> PipelineContext:
        """Initialize and resolve pipeline identifiers."""
        pipeline_utils = PipelineUtils()
        pipeline_workspace = self.connection_params.get("pipeline_workspace_name")
        pipeline_name = self.connection_params.get("pipeline_name")
        source_connection_id = self.connection_params.get("pipeline_source_connection_id")
        source_database = self.connection_params.get("pipeline_source_database")
        assert pipeline_workspace and pipeline_name and source_connection_id and source_database

        workspace_id = pipeline_utils.resolve_workspace_id(pipeline_workspace)
        pipeline_id = pipeline_utils.resolve_pipeline_id(workspace_id, pipeline_name)

        return PipelineContext(
            pipeline_utils=pipeline_utils,
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            source_connection_id=source_connection_id,
            source_database=source_database,
        )

    def _make_error_result(
        self, batch_id: str, error_msg: str, start_time: float, paths: Optional[List[str]] = None
    ) -> BatchExtractionResult:
        """Create error result with timing."""
        return BatchExtractionResult(
            extraction_id=batch_id,
            source_path=self.source_path,
            extract_file_paths=paths or [],
            status=ExecutionStatus.ERROR,
            error_message=error_msg,
            file_count=0,
            file_size_bytes=0,
            duration_ms=int((time.time() - start_time) * 1000),
        )

    def _make_success_result(
        self, batch_id: str, paths: List[str], start_time: float
    ) -> BatchExtractionResult:
        """Create success result with timing."""
        return BatchExtractionResult(
            extraction_id=batch_id,
            source_path=self.source_path,
            extract_file_paths=paths,
            status=ExecutionStatus.SUCCESS,
            error_message=None,
            file_count=len(paths),
            file_size_bytes=0,
            duration_ms=int((time.time() - start_time) * 1000),
        )

    def _make_warning_result(
        self, batch_id: str, warning_msg: str, paths: List[str], start_time: float
    ) -> BatchExtractionResult:
        """Create warning result with timing."""
        return BatchExtractionResult(
            extraction_id=batch_id,
            source_path=self.source_path,
            extract_file_paths=paths,
            status=ExecutionStatus.WARNING,
            error_message=warning_msg,
            file_count=0,
            file_size_bytes=0,
            duration_ms=int((time.time() - start_time) * 1000),
        )

    def _invoke_pipeline(
        self, ctx: PipelineContext, payload: Dict[str, Any], label: str
    ) -> str:
        """Invoke pipeline and return result status."""
        nest_asyncio.apply()
        return asyncio.run(
            ctx.pipeline_utils.trigger_pipeline_with_polling(
                workspace_id=ctx.workspace_id,
                pipeline_id=ctx.pipeline_id,
                payload=payload,
                table_name=label
            )
        )

    def _update_watermark_from_df(self, df, batch_id: str) -> Optional[Any]:
        """Calculate max from dataframe and update watermark."""
        try:
            max_val = df.agg(
                spark_max(col(self.extraction_params.incremental_column))
            ).collect()[0][0]
            if max_val is not None:
                self.extraction_logger.update_watermark(
                    self.config.source_name,
                    self.config.resource_name,
                    self.extraction_params.incremental_column,
                    max_val,
                    batch_id
                )
            return max_val
        except Exception as e:
            self.logger.warning(f"Could not update watermark: {e}")
            return None

    def _extract_via_pipeline(self) -> Generator[BatchExtractionResult, None, None]:
        """
        Pipeline extraction - supports full, incremental, and chunked incremental loads.

        Delegates to Fabric Pipeline for database connectivity via Data Gateway.
        We handle watermark management by reading back extracted files.

        Modes:
        - Full extraction: No incremental_column configured
        - Single-batch incremental: incremental_column configured, no incremental_chunk_size
        - Chunked incremental: incremental_column + incremental_chunk_size configured

        Yields:
            BatchExtractionResult for each batch extracted (one for single-batch, multiple for chunked)
        """
        # Check if chunked mode is enabled
        if self.extraction_params.incremental_chunk_size:
            yield from self._extract_chunked()
        else:
            yield from self._extract_single_batch()

    def _extract_single_batch(self) -> Generator[BatchExtractionResult, None, None]:
        """
        Single-batch extraction (full or incremental without chunking).

        Flow:
        1. Get watermark (if incremental configured)
        2. Apply lookback to watermark (if configured)
        3. Build query with incremental WHERE clause
        4. Call pipeline to extract and write data
        5. Read back files to calculate new watermark
        6. Update watermark

        Yields:
            Single BatchExtractionResult
        """
        batch_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            # 1. Get watermark (if incremental extraction configured)
            watermark_value = self.extraction_logger.get_watermark(
                self.config.source_name,
                self.config.resource_name,
                self.extraction_params.incremental_column
            )

            # 2. Apply lookback to watermark if configured
            effective_watermark = watermark_value
            if (
                watermark_value is not None
                and self.extraction_params.incremental_lookback
                and self.extraction_params.incremental_column_type
            ):
                effective_watermark = self._apply_lookback(
                    watermark_value,
                    self.extraction_params.incremental_lookback,
                    self.extraction_params.incremental_column_type,
                )
                self.logger.info(
                    f"Incremental extraction with lookback: {self.extraction_params.incremental_column} > {effective_watermark} "
                    f"(original watermark: {watermark_value}, lookback: {self.extraction_params.incremental_lookback}h)"
                )
            elif watermark_value:
                self.logger.info(
                    f"Incremental extraction: {self.extraction_params.incremental_column} > {watermark_value}"
                )
            else:
                self.logger.info("Full extraction")

            # 3. Build output paths with Hive partitioning and batch_id
            relative_path, full_path = self._build_batch_path(batch_id)
            relative_path = relative_path.rstrip("/")
            full_path = full_path.rstrip("/")

            # 4. Build query (CETAS uses relative_path - DATA_SOURCE has the storage account/container)
            query = self._build_query(effective_watermark, output_path=relative_path, batch_id=batch_id)
            self.logger.debug(f"Extraction mode: {self.extraction_params.extraction_mode}")
            self.logger.debug(f"Extracting query: {query[:200]}...")
            self.logger.debug(f"Output path: {relative_path}")

            # 5. Initialize pipeline and resolve IDs
            ctx = self._get_pipeline_context()

            # 6. Build pipeline payload (use relative path)
            payload = self._build_pipeline_payload(
                query=query,
                output_path=relative_path,
                source_connection_id=ctx.source_connection_id,
                source_database=ctx.source_database,
            )

            # 7. Invoke pipeline and wait for completion
            self.logger.debug(f"Invoking pipeline {ctx.pipeline_id} in workspace {ctx.workspace_id}...")
            result = self._invoke_pipeline(ctx, payload, self.source_path)

            # 8. Check pipeline status
            if result != "Completed":
                self.logger.error(f"Pipeline execution failed with status: {result}")
                yield self._make_error_result(batch_id, f"Pipeline execution failed: {result}", start_time)
                return

            self.logger.debug(f"Pipeline completed successfully: {result}")

            # 9. Read back files to calculate watermark (if incremental)
            new_watermark = None
            row_count = 0

            if self.extraction_params.incremental_column:
                self.logger.debug(f"Reading back files to calculate watermark for {self.extraction_params.incremental_column}...")

                try:
                    df = self.storage_lakehouse.read_file(full_path, self.config.extract_file_format_params.file_format)
                    row_count = df.count()

                    if row_count == 0:
                        # No new data extracted (valid for incremental)
                        self.logger.warning(
                            f"No new data extracted (0 rows). Watermark remains: {watermark_value}"
                        )
                        yield self._make_warning_result(batch_id, "No new data extracted (0 rows)", [full_path], start_time)
                        return

                    # Calculate new watermark as MAX(incremental_column)
                    new_watermark = df.agg(
                        spark_max(col(self.extraction_params.incremental_column))
                    ).collect()[0][0]

                    if new_watermark is None:
                        # All values in incremental_column are NULL - this is an error
                        self.logger.error(
                            f"All values in incremental_column '{self.extraction_params.incremental_column}' are NULL. "
                            f"Cannot update watermark."
                        )
                        yield self._make_error_result(
                            batch_id,
                            f"Incremental column '{self.extraction_params.incremental_column}' has all NULL values",
                            start_time,
                            paths=[full_path]
                        )
                        return

                    self.logger.info(f"New watermark calculated: {new_watermark} ({row_count} rows)")

                except Exception as e:
                    self.logger.error(f"Failed to read back files for watermark calculation: {str(e)}")
                    yield self._make_error_result(batch_id, f"Failed to read back files for watermark: {str(e)}", start_time)
                    return

            # 10. Update watermark
            if new_watermark is not None:
                self.logger.info(f"Updating watermark: {watermark_value} → {new_watermark}")
                self.extraction_logger.update_watermark(
                    self.config.source_name,
                    self.config.resource_name,
                    self.extraction_params.incremental_column,
                    new_watermark,
                    batch_id
                )

            # 10. Yield success
            yield self._make_success_result(batch_id, [full_path], start_time)

        except Exception as e:
            # Extraction failed
            self.logger.error(f"Pipeline extraction failed: {str(e)}")
            yield self._make_error_result(batch_id, f"Pipeline extraction failed: {str(e)}", start_time)

    def _extract_chunked(self) -> Generator[BatchExtractionResult, None, None]:
        """
        Chunked extraction - splits large incremental loads into sequential ranges.

        Query pattern:
        - First chunk: `col > start AND col < end` (start is watermark or incremental_start)
        - Subsequent chunks: `col >= start AND col < end` (include boundary excluded by previous <)

        Start value priority:
        1. Watermark (if exists) - used for subsequent runs
        2. incremental_start (if no watermark) - seed for first run (treated as "first watermark")

        Two modes based on incremental_end:
        - **Pre-calculated mode**: When incremental_end is specified (or defaulted to now for date/timestamp)
        - **Progressive mode**: When incremental_end is NOT specified for integers,
          extract chunks until one returns 0 rows

        Yields:
            BatchExtractionResult for each chunk
        """
        column_type = self.extraction_params.incremental_column_type
        incremental_chunk_size = self.extraction_params.incremental_chunk_size
        # Type narrowing - config validates these are set when chunk_size is configured
        assert column_type and incremental_chunk_size

        # Get watermark (takes priority over incremental_start)
        watermark_value = self.extraction_logger.get_watermark(
            self.config.source_name,
            self.config.resource_name,
            self.extraction_params.incremental_column
        )

        # Determine start value
        # Watermark takes priority over incremental_start
        # Both are treated as "last processed" - queries use > (exclusive) for first chunk
        if watermark_value is not None:
            start_value = watermark_value

            # Apply lookback to watermark if configured
            if self.extraction_params.incremental_lookback:
                start_value = self._apply_lookback(
                    watermark_value,
                    self.extraction_params.incremental_lookback,
                    column_type,
                )
                self.logger.info(
                    f"Using watermark with lookback: {start_value} "
                    f"(watermark: {watermark_value}, lookback: {self.extraction_params.incremental_lookback})"
                )
            else:
                self.logger.info(f"Using watermark as start: {start_value}")
        elif self.extraction_params.incremental_start:
            # First run - incremental_start is treated as "first watermark" (exclusive)
            # e.g., incremental_start="2024-12-31" means first value to capture is Jan 1
            start_value = self._parse_incremental_value(
                self.extraction_params.incremental_start,
                column_type,
            )
            self.logger.info(f"First run, using incremental_start as first watermark: {start_value} (exclusive)")
        else:
            raise ValueError(
                "Chunked extraction requires either a watermark or incremental_start for first run"
            )

        # Determine mode based on whether incremental_end is specified
        if self.extraction_params.incremental_end:
            # Pre-calculated mode: generate all ranges upfront
            # User specifies inclusive end (last value to include), convert to exclusive for < comparison
            inclusive_end = self._parse_incremental_value(
                self.extraction_params.incremental_end,
                column_type,
            )
            # Convert to exclusive: +1 day for date/timestamp, +1 for integer
            if column_type == "integer":
                if not isinstance(inclusive_end, int):
                    raise ValueError(f"Expected int for integer column_type, got {type(inclusive_end)}")
                end_value = inclusive_end + 1
            elif column_type == "date":
                if isinstance(inclusive_end, datetime):
                    end_value = (inclusive_end + timedelta(days=1)).date()
                elif isinstance(inclusive_end, date):
                    end_value = inclusive_end + timedelta(days=1)
                else:
                    raise ValueError(f"Expected date for date column_type, got {type(inclusive_end)}")
            else:  # timestamp
                if isinstance(inclusive_end, datetime):
                    end_value = inclusive_end + timedelta(days=1)
                elif isinstance(inclusive_end, date):
                    end_value = datetime.combine(inclusive_end, datetime.min.time()) + timedelta(days=1)
                else:
                    raise ValueError(f"Expected datetime for timestamp column_type, got {type(inclusive_end)}")
            self.logger.info(f"Chunked extraction (pre-calculated): {start_value} to < {end_value} (user specified end: {inclusive_end}, interval={incremental_chunk_size})")
            ctx = self._get_pipeline_context()
            ranges = self._generate_ranges(start_value, end_value, incremental_chunk_size, column_type)
            if not ranges:
                self.logger.warning("No chunks to extract (start >= end)")
                return
            self.logger.info(f"Generated {len(ranges)} chunks with interval {incremental_chunk_size}")
            yield from self._extract_chunks(iter(ranges), ctx, stop_on_empty=False)
        elif column_type == "integer":
            # Progressive mode: extract chunks until empty (integers only)
            self.logger.info(f"Chunked extraction (progressive): starting from {start_value} (interval={incremental_chunk_size})")
            # start_value is guaranteed to be int when column_type == "integer"
            if not isinstance(start_value, int):
                raise ValueError(f"Expected int for integer column_type, got {type(start_value)}")
            ctx = self._get_pipeline_context()

            def progressive_ranges() -> Generator[Tuple[int, int], None, None]:
                current = start_value
                while True:
                    yield (current, current + incremental_chunk_size)
                    current += incremental_chunk_size

            yield from self._extract_chunks(progressive_ranges(), ctx, stop_on_empty=True)
        else:
            # Date/timestamp without end: default to tomorrow (exclusive, captures all of today)
            if column_type == "date":
                end_value = date.today() + timedelta(days=1)
            else:  # timestamp
                end_value = datetime.now() + timedelta(days=1)
            self.logger.info(f"Chunked extraction (pre-calculated): {start_value} to < {end_value} (interval={incremental_chunk_size})")
            ctx = self._get_pipeline_context()
            ranges = self._generate_ranges(start_value, end_value, incremental_chunk_size, column_type)
            if not ranges:
                self.logger.warning("No chunks to extract (start >= end)")
                return
            self.logger.info(f"Generated {len(ranges)} chunks with interval {incremental_chunk_size}")
            yield from self._extract_chunks(iter(ranges), ctx, stop_on_empty=False)

    def _extract_chunks(
        self,
        range_iterator: Iterator[Tuple[Any, Any]],
        ctx: PipelineContext,
        stop_on_empty: bool = False,
    ) -> Generator[BatchExtractionResult, None, None]:
        """
        Extract data in chunks from a range iterator.

        Unified implementation for both pre-calculated and progressive modes.

        Args:
            range_iterator: Iterator yielding (range_start, range_end) tuples
            ctx: Pipeline context with resolved IDs
            stop_on_empty: If True, stop when a chunk returns 0 rows (progressive mode)

        Yields:
            BatchExtractionResult for each chunk
        """
        overall_start_time = time.time()
        chunk_num = 0
        total_rows = 0

        for range_start, range_end in range_iterator:
            chunk_num += 1
            batch_id = str(uuid.uuid4())
            chunk_start_time = time.time()

            # First chunk: > (exclusive), subsequent: >= (inclusive)
            use_inclusive_start = chunk_num > 1
            self.logger.debug(
                f"Processing chunk {chunk_num}: {range_start} to < {range_end} "
                f"(inclusive_start={use_inclusive_start})"
            )

            try:
                # Build paths
                relative_path, full_path = self._build_batch_path(batch_id)
                relative_path = relative_path.rstrip("/")
                full_path = full_path.rstrip("/")

                # Build and execute query
                query = self._build_query(
                    output_path=full_path,
                    range_start=range_start,
                    range_end=range_end,
                    range_start_inclusive=use_inclusive_start,
                    batch_id=batch_id,
                )
                self.logger.debug(f"Chunk {chunk_num} query: {query[:200]}...")

                payload = self._build_pipeline_payload(
                    query=query,
                    output_path=relative_path,
                    source_connection_id=ctx.source_connection_id,
                    source_database=ctx.source_database,
                )

                result = self._invoke_pipeline(ctx, payload, f"{self.source_path}_chunk{chunk_num}")

                if result != "Completed":
                    self.logger.error(f"Chunk {chunk_num} failed: {result}")
                    yield self._make_error_result(batch_id, f"Chunk {chunk_num} failed: {result}", chunk_start_time)
                    return

                # Handle progressive mode: check for empty result
                if stop_on_empty:
                    try:
                        df = self.storage_lakehouse.read_file(full_path, self.config.extract_file_format_params.file_format)
                        row_count = df.count()
                    except Exception:
                        self.logger.info(f"Chunk {chunk_num}: 0 rows - stopping (no files)")
                        break

                    if row_count == 0:
                        self.logger.info(f"Chunk {chunk_num}: 0 rows - stopping")
                        break

                    total_rows += row_count
                    self.logger.info(f"Chunk {chunk_num}: {row_count} rows")
                    self._update_watermark_from_df(df, batch_id)
                else:
                    # Pre-calculated mode: update watermark from file
                    try:
                        df = self.storage_lakehouse.read_file(full_path, self.config.extract_file_format_params.file_format)
                        self._update_watermark_from_df(df, batch_id)
                    except Exception as e:
                        self.logger.warning(f"Could not update watermark for chunk {chunk_num}: {e}")

                yield self._make_success_result(batch_id, [full_path], chunk_start_time)

            except Exception as e:
                self.logger.error(f"Chunk {chunk_num} failed: {str(e)}")
                yield self._make_error_result(batch_id, f"Chunk {chunk_num} failed: {str(e)}", chunk_start_time)
                return

        total_duration_ms = int((time.time() - overall_start_time) * 1000)
        if stop_on_empty:
            self.logger.info(f"Progressive extraction complete: {chunk_num} chunks, {total_rows} rows in {total_duration_ms}ms")
        else:
            self.logger.info(f"Chunked extraction complete: {chunk_num} chunks in {total_duration_ms}ms")

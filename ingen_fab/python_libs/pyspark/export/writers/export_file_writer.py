"""ExportFileWriter for exporting DataFrames to various file formats."""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.pyspark.export.common.file_utils import (
    get_archive_extension,
    get_file_extension,
)
from ingen_fab.python_libs.pyspark.export.common.param_resolver import (
    Params,
    resolve_params,
)
from ingen_fab.python_libs.pyspark.export.writers.compression_service import (
    CompressionService,
)

logger = logging.getLogger(__name__)

# Staging directory for intermediate files (prevents premature blob storage events)
STAGING_DIR = "temp"


@dataclass
class WriteResult:
    """Result of a file write operation."""

    success: bool
    file_paths: List[str]
    rows_written: int
    bytes_written: int
    trigger_file_path: Optional[str] = None
    error_message: Optional[str] = None


class ExportFileWriter:
    """Writes DataFrames to Lakehouse Files in various formats using mount-based operations.

    Features:
    - Multiple file formats (CSV, Parquet, JSON)
    - Post-process compression (gzip, zipdeflate)
    - File splitting by row count
    - Bundled compression (multiple files → single archive)
    - Trigger file creation
    - Staging area pattern (prevents premature blob events)
    """

    def __init__(
        self,
        config: ExportConfig,
        mount_path: str,
        run_date: Optional[datetime] = None,
        period_start_date: Optional[datetime] = None,
        period_end_date: Optional[datetime] = None,
        timezone: Optional[str] = None,
    ):
        """Initialize ExportFileWriter.

        Args:
            config: Export configuration
            mount_path: Path to mounted target lakehouse (from ExportOrchestrator)
            run_date: Logical date for the export. Defaults to today but can be
                overridden for backfills.
            period_start_date: Optional period start datetime for filename patterns.
            period_end_date: Optional period end datetime for filename patterns.
            timezone: Timezone string (e.g., "Australia/Sydney") for process_date.
        """
        self.config = config
        self._mount_path = mount_path
        self.run_date = run_date or datetime.combine(date.today(), datetime.min.time())
        self._timezone = timezone

        # Initialize extracted services
        self._compression = CompressionService(
            compression_type=config.file_format_params.compression,
            compression_level=config.file_format_params.compression_level,
        )

        # Initialize params for pattern resolution
        # process_date will be resolved at runtime using the timezone
        self._params = Params(
            export_name=config.export_name,
            run_date=self.run_date,
            period_start_date=period_start_date,
            period_end_date=period_end_date,
            timezone=timezone,
        )

    def _now(self) -> datetime:
        """Get current time in configured timezone."""
        if self._timezone:
            return datetime.now(ZoneInfo(self._timezone))
        return datetime.now()

    # -------------------------------------------------------------------------
    # Public Interface
    # -------------------------------------------------------------------------

    def write(
        self, df: DataFrame, export_run_id: str, row_count: Optional[int] = None
    ) -> WriteResult:
        """Write DataFrame to Lakehouse Files.

        Uses staging area to prevent premature blob storage events.
        Files are written to staging, processed, then moved to target.

        Args:
            df: DataFrame to write
            export_run_id: Unique export run identifier
            row_count: Optional pre-computed row count (avoids extra scan if provided)

        Returns:
            WriteResult with file paths and metrics
        """
        try:
            # Update params with run ID
            self._params.run_id = export_run_id

            # Get row count to determine write strategy (use provided or compute)
            if row_count is None:
                row_count = df.count()
            logger.info(f"Writing {row_count:,} rows")

            # Route to appropriate write method
            if self._should_split_files(row_count):
                return self._write_split(df, export_run_id, row_count)
            else:
                return self._write_single(df, export_run_id, row_count)

        except Exception as e:
            return self._handle_write_error(e)

    # -------------------------------------------------------------------------
    # Single File Write
    # -------------------------------------------------------------------------

    def _write_single(
        self,
        df: DataFrame,
        export_run_id: str,
        row_count: int,
    ) -> WriteResult:
        """Write DataFrame as a single file with optional compression.

        Args:
            df: DataFrame to write
            export_run_id: Export run identifier
            row_count: Pre-calculated row count
        """
        is_post_compress = self._compression.is_post_process_compression()

        # Build paths
        target_path = self._build_target_path(exclude_compression_ext=is_post_compress)
        staging_path = self._get_staging_path(os.path.basename(target_path))

        # Prepare Spark options
        format_options = self._get_spark_options(exclude_compression=is_post_compress)
        spark_format = self._get_spark_format()

        logger.info(f"Writing to staging: {staging_path}")

        # Write to staging
        bytes_written = self._write_dataframe(df, staging_path, format_options, spark_format)

        # Apply post-process compression if needed
        final_staging_path = staging_path
        if is_post_compress:
            final_staging_path, bytes_written = self._apply_compression(staging_path, export_run_id)

        # Determine final target path
        final_target_path = self._get_final_target_path(target_path, final_staging_path, is_post_compress)

        # Move to target
        self._move_to_target(final_staging_path, final_target_path)
        logger.info(f"Published: {final_target_path}")

        # Write trigger file
        trigger_path = self._write_trigger_file(final_target_path, export_run_id)

        # Cleanup staging
        self._cleanup_staging()

        return WriteResult(
            success=True,
            file_paths=[final_target_path],
            rows_written=row_count,
            bytes_written=bytes_written,
            trigger_file_path=trigger_path,
        )

    # -------------------------------------------------------------------------
    # Split File Write
    # -------------------------------------------------------------------------

    def _write_split(
        self,
        df: DataFrame,
        export_run_id: str,
        total_rows: int,
    ) -> WriteResult:
        """Write DataFrame split into multiple files.

        Args:
            df: DataFrame to write
            export_run_id: Export run identifier
            total_rows: Pre-calculated total row count
        """
        max_rows = self.config.max_rows_per_file
        assert max_rows is not None, "max_rows_per_file must be set for split writes"
        num_files = (total_rows + max_rows - 1) // max_rows
        is_post_compress = self._compression.is_post_process_compression()

        logger.info(f"Splitting {total_rows:,} rows into {num_files} files ({max_rows:,} rows each)")

        # Prepare options
        format_options = self._get_spark_options(exclude_compression=is_post_compress)
        spark_format = self._get_spark_format()
        target_base_path = self._build_target_path(exclude_compression_ext=True)

        # Add row numbers for splitting
        # Cache indexed_df to avoid re-computing window function for each chunk filter
        window = Window.orderBy(F.monotonically_increasing_id())
        indexed_df = df.withColumn("_row_num", F.row_number().over(window)).cache()

        # Write chunks
        staging_raw_paths = []
        total_bytes = 0

        for i in range(num_files):
            start_row = i * max_rows + 1
            end_row = min((i + 1) * max_rows, total_rows)

            chunk_df = indexed_df.filter(
                (F.col("_row_num") >= start_row) & (F.col("_row_num") <= end_row)
            ).drop("_row_num")

            # Generate numbered filename (exclude compression ext if we'll bundle later)
            filename = self._get_split_filename(target_base_path, i + 1, is_post_compress)
            staging_path = self._get_staging_path(filename)

            logger.info(f"Writing file {i + 1}/{num_files}: {filename} ({end_row - start_row + 1:,} rows)")

            chunk_bytes = self._write_dataframe(chunk_df, staging_path, format_options, spark_format)
            total_bytes += chunk_bytes
            staging_raw_paths.append(staging_path)

        # Release cached DataFrame
        indexed_df.unpersist()

        # Apply compression - always bundle split files into single archive
        staging_final_paths, final_bytes = self._compress_split_files(
            staging_raw_paths, is_post_compress, total_bytes
        )

        # Move to target
        target_paths = self._move_split_to_target(staging_final_paths, target_base_path)

        # Write trigger file (before cleanup - trigger signals completion)
        trigger_path = self._write_trigger_file(target_paths[0] if target_paths else target_base_path, export_run_id)

        # Cleanup staging
        self._cleanup_staging()

        return WriteResult(
            success=True,
            file_paths=target_paths,
            rows_written=total_rows,
            bytes_written=final_bytes,
            trigger_file_path=trigger_path,
        )

    def _compress_split_files(
        self,
        staging_raw_paths: List[str],
        is_post_compress: bool,
        raw_bytes: int,
    ) -> Tuple[List[str], int]:
        """Apply compression to split files.

        When compression is enabled, always bundles all split files into a single archive.

        Args:
            staging_raw_paths: List of raw file staging paths
            is_post_compress: If True, post-process compression enabled
            raw_bytes: Total raw bytes before compression

        Returns:
            Tuple of (list of final staging paths, actual bytes written)
        """
        if is_post_compress:
            # Bundle all files into single archive
            archive_filename = self._get_bundle_archive_filename()
            archive_staging_path = self._get_staging_path(archive_filename)
            mount_archive_path = self._get_mount_file_path(archive_staging_path)

            mount_raw_paths = [self._get_mount_file_path(p) for p in staging_raw_paths]

            result = self._compression.bundle_files(
                source_paths=mount_raw_paths,
                output_path=mount_archive_path,
                delete_sources=True,
            )
            return [archive_staging_path], result.bytes_written

        else:
            # No compression - return raw paths and original bytes
            return staging_raw_paths, raw_bytes

    def _move_split_to_target(
        self,
        staging_paths: List[str],
        target_base_path: str,
    ) -> List[str]:
        """Move split files from staging to target.

        Args:
            staging_paths: List of staging file paths
            target_base_path: Base target path for all files

        Returns:
            List of final target paths
        """
        target_dir = os.path.dirname(target_base_path)
        target_paths = []

        for staging_path in staging_paths:
            filename = os.path.basename(staging_path)
            target_path = f"{target_dir}/{filename}"
            self._move_to_target(staging_path, target_path)
            target_paths.append(target_path)
            logger.info(f"Published: {target_path}")

        return target_paths

    # -------------------------------------------------------------------------
    # Compression Helpers
    # -------------------------------------------------------------------------

    def _apply_compression(
        self,
        staging_path: str,
        export_run_id: Optional[str] = None,
    ) -> Tuple[str, int]:
        """Apply compression to a staging file.

        Args:
            staging_path: Relative staging path to raw file
            export_run_id: Optional export run ID for compressed filename pattern

        Returns:
            Tuple of (relative staging path to compressed file, compressed bytes)
        """
        mount_raw_path = self._get_mount_file_path(staging_path)
        raw_filename = os.path.basename(staging_path)

        # Determine compressed filename
        if export_run_id and self.config.compressed_filename_pattern:
            compressed_filename = resolve_params(
                self.config.compressed_filename_pattern, self._params
            )
        else:
            # Generate compressed filename based on compression type
            base_name = raw_filename.rsplit(".", 1)[0]
            if self._compression.is_gzip():
                compressed_filename = f"{raw_filename}.gz"
            else:
                compressed_filename = f"{base_name}.zip"

        compressed_staging_path = os.path.join(os.path.dirname(staging_path), compressed_filename)
        mount_compressed_path = self._get_mount_file_path(compressed_staging_path)

        result = self._compression.compress_file(
            source_path=mount_raw_path,
            output_path=mount_compressed_path,
            delete_source=True,
        )

        return compressed_staging_path, result.bytes_written

    def _get_bundle_archive_filename(self) -> str:
        """Get filename for bundled archive.

        Returns:
            Archive filename with appropriate extension
        """
        compression = self.config.file_format_params.compression
        archive_ext = get_archive_extension(compression)

        if self.config.compressed_filename_pattern:
            archive_filename = resolve_params(
                self.config.compressed_filename_pattern, self._params
            )
            if not archive_filename.endswith(archive_ext):
                archive_filename = archive_filename.rsplit(".", 1)[0] + archive_ext
        else:
            timestamp = self._now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"{self.config.export_name}_{timestamp}{archive_ext}"

        return archive_filename

    # -------------------------------------------------------------------------
    # Path Helpers
    # -------------------------------------------------------------------------

    def _get_mount_file_path(self, relative_path: str) -> str:
        """Convert relative path to mount path."""
        return os.path.join(self._mount_path, "Files", relative_path)

    def _get_staging_path(self, filename: str) -> str:
        """Get staging path for intermediate files."""
        return f"{STAGING_DIR}/{self.config.export_group_name}/{self.config.export_name}/{filename}"

    def _build_target_path(self, exclude_compression_ext: bool = False) -> str:
        """Build output path relative to Files/."""
        # Resolve placeholders in target_path
        base_path = resolve_params(self.config.target_path.strip("/"), self._params)

        # Generate filename
        if self.config.target_filename_pattern:
            filename = resolve_params(self.config.target_filename_pattern, self._params)
        else:
            # Default: export_name_YYYYMMDD_HHMMSS.ext
            timestamp = self._now().strftime("%Y%m%d_%H%M%S")
            ext = get_file_extension(
                str(self.config.file_format_params.file_format),
                self.config.file_format_params.compression,
                exclude_compression_ext,
            )
            filename = f"{self.config.export_name}_{timestamp}{ext}"

        return f"{base_path}/{filename}"

    def _get_final_target_path(
        self,
        original_target: str,
        final_staging: str,
        is_post_compress: bool,
    ) -> str:
        """Get final target path accounting for compression extension changes."""
        if is_post_compress:
            return os.path.join(
                os.path.dirname(original_target),
                os.path.basename(final_staging),
            )
        return original_target

    def _get_split_filename(
        self,
        target_base_path: str,
        part_number: int,
        exclude_compression_ext: bool,
    ) -> str:
        """Generate filename for split file part."""
        target_base_name = os.path.splitext(os.path.basename(target_base_path))[0]
        ext = get_file_extension(
            file_format=str(self.config.file_format_params.file_format),
            compression=self.config.file_format_params.compression,
            exclude_compression_ext=exclude_compression_ext,
        )
        return f"{target_base_name}_part{part_number:04d}{ext}"

    # -------------------------------------------------------------------------
    # Staging Management
    # -------------------------------------------------------------------------

    def _move_to_target(self, staging_path: str, target_path: str) -> None:
        """Move file from staging to final target location."""
        mount_staging = self._get_mount_file_path(staging_path)
        mount_target = self._get_mount_file_path(target_path)
        os.makedirs(os.path.dirname(mount_target), exist_ok=True)
        shutil.move(mount_staging, mount_target)
        logger.debug(f"Moved {staging_path} to {target_path}")

    def _cleanup_staging(self) -> None:
        """Remove staging directory for this export."""
        staging_dir = self._get_mount_file_path(
            f"{STAGING_DIR}/{self.config.export_group_name}/{self.config.export_name}"
        )
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
            logger.debug(f"Cleaned up staging: {staging_dir}")

    # -------------------------------------------------------------------------
    # DataFrame Writing
    # -------------------------------------------------------------------------

    def _should_split_files(self, row_count: int) -> bool:
        """Check if files should be split."""
        return bool(
            self.config.max_rows_per_file
            and row_count > self.config.max_rows_per_file
        )

    def _get_spark_format(self) -> str:
        """Map file format to Spark writer format."""
        spark_format_map = {
            "csv": "csv",
            "tsv": "csv",
            "dat": "csv",
            "parquet": "parquet",
            "json": "json",
        }
        return spark_format_map[str(self.config.file_format_params.file_format)]

    def _get_spark_options(self, exclude_compression: bool = False) -> dict:
        """Get Spark write options from config.

        Args:
            exclude_compression: If True, remove compression from options
        """
        options = self.config.file_format_params.to_spark_options()
        if exclude_compression:
            options.pop("compression", None)
        return options

    def _write_dataframe(
        self,
        df: DataFrame,
        output_path: str,
        options: dict,
        spark_format: str,
    ) -> int:
        """Write DataFrame to mount path as a single file.

        Spark writes to a directory with part files. This method:
        1. Writes to a temp directory on the mount path
        2. Finds the part file and moves it to the final path
        3. Cleans up the temp directory

        Returns:
            Actual file size in bytes
        """
        df_single = df.coalesce(1)

        # Write to temp directory
        temp_dir = f"{output_path}_temp"
        mount_temp_dir = self._get_mount_file_path(temp_dir)
        os.makedirs(os.path.dirname(mount_temp_dir), exist_ok=True)

        # Spark write
        writer = df_single.write.format(spark_format).mode("overwrite")
        for key, value in options.items():
            if key != "mode":
                writer = writer.option(key, value)
        writer.save(f"file://{mount_temp_dir}")

        # Finalize: move part file to final location
        self._finalize_spark_output(temp_dir, output_path)

        # Get file size
        mount_final_path = self._get_mount_file_path(output_path)
        file_size = os.path.getsize(mount_final_path)

        logger.info(f"Wrote: {output_path} ({file_size:,} bytes)")
        return file_size

    def _finalize_spark_output(self, temp_dir_path: str, final_path: str) -> str:
        """Handle Spark's directory output by finding the part file and moving it.

        Spark writes: temp_dir_path/ containing part-00000-*.csv and _SUCCESS
        We want: final_path (single file)
        """
        mount_temp_dir = self._get_mount_file_path(temp_dir_path)
        mount_final_path = self._get_mount_file_path(final_path)

        try:
            files = os.listdir(mount_temp_dir)
            part_file = next((f for f in files if f.startswith("part-")), None)

            if part_file:
                part_file_path = os.path.join(mount_temp_dir, part_file)
                os.makedirs(os.path.dirname(mount_final_path), exist_ok=True)
                shutil.move(part_file_path, mount_final_path)
                shutil.rmtree(mount_temp_dir)
            else:
                raise FileNotFoundError(f"No part file found in {mount_temp_dir}")

        except Exception as e:
            logger.error(f"Failed to finalize Spark output: {e}")
            raise

        return final_path

    # -------------------------------------------------------------------------
    # Trigger Files
    # -------------------------------------------------------------------------

    def _write_trigger_file(
        self,
        data_file_path: str,
        export_run_id: str,
    ) -> Optional[str]:
        """Write trigger/control file."""
        pattern = self.config.trigger_file_pattern
        if not pattern:
            return None

        # Use same param resolution as filename patterns
        self._params.run_id = export_run_id
        trigger_filename = resolve_params(pattern, self._params)
        trigger_path = os.path.join(os.path.dirname(data_file_path), trigger_filename)
        mount_trigger_path = self._get_mount_file_path(trigger_path)

        try:
            os.makedirs(os.path.dirname(mount_trigger_path), exist_ok=True)
            with open(mount_trigger_path, "w") as f:
                f.write("")
            logger.info(f"Created trigger file: {trigger_path}")
            return trigger_path
        except Exception as e:
            logger.warning(f"Failed to create trigger file: {e}")
            return None

    # -------------------------------------------------------------------------
    # Error Handling
    # -------------------------------------------------------------------------

    def _handle_write_error(self, error: Exception) -> WriteResult:
        """Handle write errors with cleanup."""
        logger.error(f"Failed to write export: {error}")

        try:
            self._cleanup_staging()
        except Exception as cleanup_error:
            logger.warning(f"Cleanup after error failed: {cleanup_error}")

        return WriteResult(
            success=False,
            file_paths=[],
            rows_written=0,
            bytes_written=0,
            error_message=str(error),
        )

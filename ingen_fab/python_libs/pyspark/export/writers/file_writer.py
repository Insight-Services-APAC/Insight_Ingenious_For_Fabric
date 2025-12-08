"""FileWriter for exporting DataFrames to various file formats."""

from __future__ import annotations

import logging
import os
import re
import shutil
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ingen_fab.python_libs.pyspark.export.common.config import (
    CompressionType,
    ExportConfig,
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


class FileWriter:
    """Writes DataFrames to Lakehouse Files in various formats using mount-based operations."""

    def __init__(self, config: ExportConfig, run_date: Optional[date] = None):
        """
        Initialize FileWriter.

        Args:
            config: Export configuration
            run_date: Date for period calculations. Defaults to today.
        """
        self.config = config
        self.run_date = run_date or date.today()
        self.logger = logger
        self._mount_point = None
        self._mount_path = None

    def _setup_mount(self) -> str:
        """Mount target lakehouse and return the mount path."""
        if self._mount_path:
            return self._mount_path

        import notebookutils

        # Create unique mount point per export to avoid conflicts
        self._mount_point = f"/mnt/export_{self.config.export_name}"

        # Build ABFSS URL for target lakehouse
        lakehouse_name = self.config.target_lakehouse
        if not lakehouse_name.endswith(".Lakehouse"):
            lakehouse_name = f"{lakehouse_name}.Lakehouse"

        abfss_url = f"abfss://{self.config.target_workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}"

        # Mount
        _ = notebookutils.fs.mount(abfss_url, self._mount_point)

        # Get accurate path
        self._mount_path = notebookutils.fs.getMountPath(self._mount_point)
        self.logger.info(f"Mounted {abfss_url} at {self._mount_path}")

        return self._mount_path

    def _teardown_mount(self):
        """Unmount the target lakehouse."""
        if self._mount_point:
            import notebookutils
            try:
                notebookutils.fs.unmount(self._mount_point)
                self.logger.info(f"Unmounted {self._mount_point}")
            except Exception as e:
                self.logger.warning(f"Failed to unmount {self._mount_point}: {e}")
            finally:
                self._mount_point = None
                self._mount_path = None

    def _get_mount_file_path(self, relative_path: str) -> str:
        """Convert relative path to mount path."""
        mount_path = self._setup_mount()
        return os.path.join(mount_path, "Files", relative_path)

    def _get_staging_path(self, filename: str) -> str:
        """Get staging path for intermediate files."""
        return f"{STAGING_DIR}/{self.config.export_group_name}/{self.config.export_name}/{filename}"

    def _move_to_target(self, staging_path: str, target_path: str) -> None:
        """Move file from staging to final target location."""
        mount_staging = self._get_mount_file_path(staging_path)
        mount_target = self._get_mount_file_path(target_path)
        os.makedirs(os.path.dirname(mount_target), exist_ok=True)
        shutil.move(mount_staging, mount_target)
        self.logger.debug(f"Moved {staging_path} to {target_path}")

    def _cleanup_staging(self) -> None:
        """Remove staging directory for this export."""
        staging_dir = self._get_mount_file_path(
            f"{STAGING_DIR}/{self.config.export_group_name}/{self.config.export_name}"
        )
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
            self.logger.debug(f"Cleaned up staging directory: {staging_dir}")

    def _is_zipdeflate(self) -> bool:
        """Check if using zipdeflate compression."""
        compression = self.config.file_format_params.compression
        return compression in (CompressionType.ZIP, CompressionType.ZIPDEFLATE, "zip", "zipdeflate")

    def _verify_archive(self, mount_path: str, relative_path: str) -> int:
        """Verify archive exists and is not empty, return size."""
        if not os.path.exists(mount_path):
            raise IOError(f"Archive not found: {relative_path}")

        size = os.path.getsize(mount_path)
        if size == 0:
            raise IOError(f"Archive is empty: {relative_path}")

        self.logger.info(f"Created archive: {relative_path} ({size:,} bytes)")
        return size

    def _calculate_period_dates(self) -> Tuple[Optional[date], Optional[date]]:
        """Calculate period dates from config + run_date.

        Returns:
            (period_start_date, period_end_date) - both None for non-period extracts
        """
        if self.config.extract_type != "period":
            return None, None

        if not self.config.period_length_days or not self.config.period_end_day:
            return None, None

        # Find most recent occurrence of period_end_day
        day_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
            "friday": 4, "saturday": 5, "sunday": 6
        }
        target_day = day_map.get(self.config.period_end_day.lower())
        if target_day is None:
            self.logger.warning(f"Invalid period_end_day: {self.config.period_end_day}")
            return None, None

        current_day = self.run_date.weekday()
        days_back = (current_day - target_day) % 7

        period_end = self.run_date - timedelta(days=days_back)
        period_start = period_end - timedelta(days=self.config.period_length_days - 1)

        return period_start, period_end

    def _finalize_spark_output(self, temp_dir_path: str, final_path: str) -> str:
        """
        Handle Spark's directory output by finding the part file and moving it.

        Spark writes: temp_dir_path/ containing part-00000-*.csv and _SUCCESS
        We want: final_path (single file)

        Args:
            temp_dir_path: Relative path to temp directory Spark wrote to
            final_path: Relative path for the final single file

        Returns:
            The final_path after moving the part file
        """
        mount_temp_dir = self._get_mount_file_path(temp_dir_path)
        mount_final_path = self._get_mount_file_path(final_path)

        try:
            # Find the part file in the temp directory
            # Note: Must use startswith to avoid matching .part-xxx.crc files
            files = os.listdir(mount_temp_dir)
            part_file = next((f for f in files if f.startswith("part-")), None)

            if part_file:
                part_file_path = os.path.join(mount_temp_dir, part_file)

                # Ensure parent directory exists
                os.makedirs(os.path.dirname(mount_final_path), exist_ok=True)

                # Move part file to final location
                shutil.move(part_file_path, mount_final_path)
                self.logger.debug(f"Moved {part_file_path} to {mount_final_path}")

                # Delete the temp directory
                shutil.rmtree(mount_temp_dir)
                self.logger.debug(f"Deleted temp directory: {mount_temp_dir}")
            else:
                raise FileNotFoundError(f"No part file found in {mount_temp_dir}")

        except Exception as e:
            self.logger.error(f"Failed to finalize Spark output: {e}")
            raise

        return final_path

    def write(
        self,
        df: DataFrame,
        export_run_id: str,
    ) -> WriteResult:
        """
        Write DataFrame to Lakehouse Files.

        Uses staging area to prevent premature blob storage events.
        Files are written to staging, processed, then moved to target.

        Args:
            df: DataFrame to write
            export_run_id: Unique export run identifier

        Returns:
            WriteResult with file paths and metrics
        """
        try:
            # Setup mount at start
            self._setup_mount()

            # Check if using zipdeflate compression
            is_zipdeflate = self._is_zipdeflate()

            # Build TARGET path (where file should end up)
            target_path = self._build_output_path(export_run_id, exclude_zip_extension=is_zipdeflate)

            # Build STAGING path (where we write first)
            staging_filename = os.path.basename(target_path)
            staging_path = self._get_staging_path(staging_filename)

            # Get Spark write options
            format_options = self.config.file_format_params.to_spark_options()
            spark_format = self._get_spark_format()

            # For zipdeflate, remove compression from Spark options (we'll handle it manually)
            if is_zipdeflate:
                format_options.pop("compression", None)

            # Get row count before write
            row_count = df.count()
            self.logger.info(f"Writing {row_count:,} rows (staging: {staging_path})")

            # Handle file splitting if configured
            if self.config.max_rows_per_file and row_count > self.config.max_rows_per_file:
                return self._write_split_files(df, target_path, format_options, spark_format, export_run_id, row_count)

            # Write to staging
            bytes_written = self._write_dataframe(df, staging_path, format_options, spark_format)

            # Handle zipdeflate compression in staging
            final_staging_path = staging_path
            if is_zipdeflate:
                final_staging_path = self._apply_zipdeflate_compression(staging_path, export_run_id)

            # Determine final target path (may have .zip extension now)
            final_target_path = target_path
            if is_zipdeflate:
                final_target_path = os.path.join(
                    os.path.dirname(target_path),
                    os.path.basename(final_staging_path)
                )

            # Move from staging to target
            self._move_to_target(final_staging_path, final_target_path)
            self.logger.info(f"Published to target: {final_target_path}")

            # Write trigger file (directly to target)
            trigger_path = None
            if self.config.trigger_file_enabled:
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

        except Exception as e:
            self.logger.error(f"Failed to write export: {e}")
            # Try to cleanup staging on error
            try:
                self._cleanup_staging()
            except Exception:
                pass
            return WriteResult(
                success=False,
                file_paths=[],
                rows_written=0,
                bytes_written=0,
                error_message=str(e),
            )
        finally:
            # Always teardown mount
            self._teardown_mount()

    def _get_spark_format(self) -> str:
        """Map file format to Spark writer format (csv/tsv/dat all use Spark's csv writer)."""
        spark_format_map = {
            "csv": "csv",
            "tsv": "csv",
            "dat": "csv",
            "parquet": "parquet",
            "json": "json",
        }
        return spark_format_map[str(self.config.file_format_params.file_format)]

    def _build_output_path(
        self,
        export_run_id: str,
        exclude_zip_extension: bool = False,
    ) -> str:
        """Build output path relative to Files/ (without Files/ prefix)."""
        # Resolve placeholders in target_path (for date-based folders)
        base_path = self._resolve_filename_pattern(
            self.config.target_path.strip('/'), export_run_id
        )

        # Generate filename
        if self.config.target_filename_pattern:
            filename = self._resolve_filename_pattern(
                self.config.target_filename_pattern, export_run_id
            )
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            ext = self._get_file_extension(exclude_zip_extension=exclude_zip_extension)
            filename = f"{self.config.export_name}_{timestamp}{ext}"

        return f"{base_path}/{filename}"

    def _get_file_extension(self, exclude_zip_extension: bool = False) -> str:
        """Get file extension including compression."""
        format_ext = {
            "csv": ".csv",
            "tsv": ".tsv",
            "dat": ".dat",
            "parquet": ".parquet",
            "json": ".json",
        }
        ext = format_ext[str(self.config.file_format_params.file_format)]

        # Add compression extension for text formats
        compression = self.config.file_format_params.compression
        if compression and compression != CompressionType.NONE:
            if compression == CompressionType.GZIP:
                ext += ".gz"
            elif self._is_zipdeflate():
                # For zipdeflate, only add .zip if not excluded (i.e., for final zip file name)
                if not exclude_zip_extension:
                    ext = ".zip"
                # When exclude_zip_extension=True, return just the data format extension

        return ext

    def _resolve_filename_pattern(self, pattern: str, export_run_id: str) -> str:
        """Resolve filename pattern with variables and format specifiers.

        Supports:
        - Simple placeholders: {period_end} → 20251206
        - Format specifiers: {period_end:%Y/%m/%d} → 2025/12/06
        - {date}, {datetime}, etc. - Process date/time
        """
        now = datetime.now()
        period_start, period_end = self._calculate_period_dates()

        # Date values available for format specifiers
        date_values = {
            "period_start": period_start,
            "period_end": period_end,
            "now": now,
            "date": now,
        }

        # Handle format specifiers: {name:format}
        def replace_with_format(match):
            name = match.group(1)
            fmt = match.group(2)
            if name in date_values and date_values[name]:
                return date_values[name].strftime(fmt)
            return match.group(0)  # Leave unchanged if not found

        # Replace {name:format} patterns first
        result = re.sub(r'\{(\w+):([^}]+)\}', replace_with_format, pattern)

        # Simple replacements (existing logic)
        replacements = {
            "{export_name}": self.config.export_name,
            "{date}": now.strftime("%Y%m%d"),
            "{datetime}": now.strftime("%Y%m%d_%H%M%S"),
            "{timestamp}": now.strftime("%Y%m%d_%H%M%S"),
            "{year}": now.strftime("%Y"),
            "{month}": now.strftime("%m"),
            "{day}": now.strftime("%d"),
            "{hour}": now.strftime("%H"),
            "{minute}": now.strftime("%M"),
            "{run_id}": export_run_id[:8],
        }

        if period_start:
            replacements["{period_start}"] = period_start.strftime("%Y%m%d")
        if period_end:
            replacements["{period_end}"] = period_end.strftime("%Y%m%d")

        for key, value in replacements.items():
            result = result.replace(key, value)

        return result

    def _write_dataframe(
        self,
        df: DataFrame,
        output_path: str,
        options: dict,
        spark_format: str,
    ) -> int:
        """Write DataFrame to mount path as a single file.

        Spark always writes to a directory with part files. This method:
        1. Writes to a temp directory on the mount path
        2. Finds the part file and moves it to the final path
        3. Cleans up the temp directory

        Returns:
            Actual file size in bytes
        """
        # Coalesce to single file for clean output
        df_single = df.coalesce(1)

        # Write to temp directory on mount path
        temp_dir = f"{output_path}_temp"
        mount_temp_dir = self._get_mount_file_path(temp_dir)

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(mount_temp_dir), exist_ok=True)

        # Spark writes directly to mount path
        writer = df_single.write.format(spark_format).mode("overwrite")
        for key, value in options.items():
            if key != "mode":  # mode is already set
                writer = writer.option(key, value)
        writer.save(f"file://{mount_temp_dir}")

        # Move part file to final location and clean up temp dir
        self._finalize_spark_output(temp_dir, output_path)

        # Get actual file size
        mount_final_path = self._get_mount_file_path(output_path)
        file_size = os.path.getsize(mount_final_path)

        self.logger.info(f"Successfully wrote file: {output_path} ({file_size:,} bytes)")
        return file_size

    def _write_split_files(
        self,
        df: DataFrame,
        target_base_path: str,
        options: dict,
        spark_format: str,
        export_run_id: str,
        total_rows: int,
    ) -> WriteResult:
        """Write DataFrame split into multiple files using staging area.

        All files are written to staging first, then moved to target.
        """
        max_rows = self.config.max_rows_per_file
        assert max_rows is not None, "max_rows_per_file must be set for split files"
        num_files = (total_rows + max_rows - 1) // max_rows

        # Check compression settings
        is_zipdeflate = self._is_zipdeflate()
        bundle_files = self.config.compress_bundle_files

        self.logger.info(f"Splitting {total_rows:,} rows into {num_files} files ({max_rows:,} rows each)")
        if bundle_files:
            self.logger.info("Bundle mode: all files will be compressed together")

        # Add row numbers for splitting
        window = Window.orderBy(F.monotonically_increasing_id())
        indexed_df = df.withColumn("_row_num", F.row_number().over(window))

        staging_raw_paths = []  # Raw files in staging (for bundling)
        staging_final_paths = []  # Final files in staging (after compression if any)
        target_paths = []  # Final target paths
        total_bytes = 0

        for i in range(num_files):
            start_row = i * max_rows + 1
            end_row = min((i + 1) * max_rows, total_rows)

            # Filter chunk
            chunk_df = indexed_df.filter(
                (F.col("_row_num") >= start_row) & (F.col("_row_num") <= end_row)
            ).drop("_row_num")

            chunk_rows = end_row - start_row + 1

            # Generate numbered filename
            # When bundling or using zipdeflate, write raw files first
            target_base_name = os.path.splitext(os.path.basename(target_base_path))[0]
            ext = self._get_file_extension(exclude_zip_extension=(bundle_files or is_zipdeflate))
            filename = f"{target_base_name}_part{i+1:04d}{ext}"

            # Build staging path
            staging_path = self._get_staging_path(filename)

            self.logger.info(f"Writing file {i+1}/{num_files}: {filename} ({chunk_rows:,} rows)")

            # Write chunk to staging
            chunk_bytes = self._write_dataframe(chunk_df, staging_path, options, spark_format)
            total_bytes += chunk_bytes

            if bundle_files:
                # Collect raw staging paths for bundled compression later
                staging_raw_paths.append(staging_path)
            elif is_zipdeflate:
                # Compress each file individually in staging
                compressed_staging_path = self._apply_zipdeflate_compression(staging_path)
                staging_final_paths.append(compressed_staging_path)
            else:
                staging_final_paths.append(staging_path)

        # Bundle all files into single archive if configured
        if bundle_files:
            bundled_staging_path = self._apply_bundled_compression_staging(
                staging_raw_paths, export_run_id
            )
            staging_final_paths = [bundled_staging_path]

        # Move all final files from staging to target
        target_dir = os.path.dirname(target_base_path)
        for staging_path in staging_final_paths:
            filename = os.path.basename(staging_path)
            target_path = f"{target_dir}/{filename}"
            self._move_to_target(staging_path, target_path)
            target_paths.append(target_path)
            self.logger.info(f"Published to target: {target_path}")

        # Cleanup staging
        self._cleanup_staging()

        # Write trigger file if enabled (use the first file path for pattern)
        trigger_path = None
        if self.config.trigger_file_enabled:
            trigger_path = self._write_trigger_file(
                target_paths[0] if target_paths else target_base_path, export_run_id
            )

        return WriteResult(
            success=True,
            file_paths=target_paths,
            rows_written=total_rows,
            bytes_written=total_bytes,
            trigger_file_path=trigger_path,
        )

    def _write_trigger_file(self, data_file_path: str, export_run_id: str) -> Optional[str]:
        """Write trigger/control file using standard file operations."""
        pattern = self.config.trigger_file_pattern
        data_filename = os.path.basename(data_file_path)
        data_basename = os.path.splitext(data_filename)[0]

        trigger_filename = pattern.format(
            filename=data_filename,
            basename=data_basename,
            export_name=self.config.export_name,
            run_id=export_run_id[:8],
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S"),
        )

        trigger_path = os.path.join(os.path.dirname(data_file_path), trigger_filename)
        mount_trigger_path = self._get_mount_file_path(trigger_path)

        try:
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(mount_trigger_path), exist_ok=True)

            # Write empty trigger file
            with open(mount_trigger_path, "w") as f:
                f.write("")

            self.logger.info(f"Created trigger file: {trigger_path}")
            return trigger_path
        except Exception as e:
            self.logger.warning(f"Failed to create trigger file: {e}")
            return None

    def _apply_zipdeflate_compression(
        self,
        raw_file_path: str,
        export_run_id: Optional[str] = None,
    ) -> str:
        """
        Apply zipdeflate compression to a raw file using streaming.

        Args:
            raw_file_path: Relative path to raw data file
            export_run_id: If provided, use compressed_filename_pattern; otherwise use simple .zip

        Returns:
            Relative path to the created ZIP file
        """
        mount_raw_path = self._get_mount_file_path(raw_file_path)
        data_filename = os.path.basename(raw_file_path)

        # Determine zip filename
        if export_run_id and self.config.compressed_filename_pattern:
            zip_filename = self._resolve_filename_pattern(
                self.config.compressed_filename_pattern, export_run_id
            )
        else:
            base_name = os.path.splitext(data_filename)[0]
            zip_filename = f"{base_name}.zip"

        zip_file_path = os.path.join(os.path.dirname(raw_file_path), zip_filename)
        mount_zip_path = self._get_mount_file_path(zip_file_path)

        self.logger.info(f"Compressing: {raw_file_path} -> {zip_file_path}")

        # STREAMING: Write directly to ZIP file, read source in chunks
        with zipfile.ZipFile(mount_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            with zf.open(data_filename, "w") as dest:
                with open(mount_raw_path, "rb") as src:
                    shutil.copyfileobj(src, dest)

        # Verify
        self._verify_archive(mount_zip_path, zip_file_path)

        # Delete raw file
        os.remove(mount_raw_path)
        self.logger.debug(f"Deleted raw file: {raw_file_path}")

        return zip_file_path

    def _apply_bundled_compression_staging(
        self,
        staging_raw_paths: List[str],
        export_run_id: str,
    ) -> str:
        """
        Bundle multiple files into a single archive in staging area.

        Archive format is determined by compression setting:
        - zip/zipdeflate -> .zip
        - gzip -> .tar.gz

        Args:
            staging_raw_paths: List of staging paths to raw data files
            export_run_id: Export run identifier

        Returns:
            Staging path to the created archive
        """
        compression = self.config.file_format_params.compression or "none"

        # Determine archive format
        if self._is_zipdeflate():
            archive_ext = ".zip"
            is_zip = True
        elif compression in (CompressionType.GZIP, "gzip"):
            archive_ext = ".tar.gz"
            is_zip = False
        else:
            raise ValueError(f"Bundling not supported for compression type: {compression}")

        # Determine archive filename
        if self.config.compressed_filename_pattern:
            archive_filename = self._resolve_filename_pattern(
                self.config.compressed_filename_pattern, export_run_id
            )
            if not archive_filename.endswith(archive_ext):
                archive_filename = os.path.splitext(archive_filename)[0] + archive_ext
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"{self.config.export_name}_{timestamp}{archive_ext}"

        # Write archive to staging
        staging_archive_path = self._get_staging_path(archive_filename)
        mount_archive_path = self._get_mount_file_path(staging_archive_path)

        self.logger.info(f"Bundling {len(staging_raw_paths)} files into staging: {archive_filename}")

        # STREAMING: Write directly to archive file
        if is_zip:
            with zipfile.ZipFile(mount_archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for raw_path in staging_raw_paths:
                    mount_raw_path = self._get_mount_file_path(raw_path)
                    filename = os.path.basename(raw_path)
                    with zf.open(filename, "w") as dest:
                        with open(mount_raw_path, "rb") as src:
                            shutil.copyfileobj(src, dest)
        else:
            with tarfile.open(mount_archive_path, "w:gz") as tf:
                for raw_path in staging_raw_paths:
                    mount_raw_path = self._get_mount_file_path(raw_path)
                    tf.add(mount_raw_path, arcname=os.path.basename(raw_path))

        # Verify
        self._verify_archive(mount_archive_path, staging_archive_path)

        # Delete raw files from staging
        for raw_path in staging_raw_paths:
            mount_raw_path = self._get_mount_file_path(raw_path)
            os.remove(mount_raw_path)

        return staging_archive_path

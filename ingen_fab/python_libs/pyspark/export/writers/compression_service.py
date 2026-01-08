"""Compression service for export file operations."""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import tarfile
import zipfile
from dataclasses import dataclass
from typing import List, Optional

from ingen_fab.python_libs.pyspark.export.common.constants import CompressionType

logger = logging.getLogger(__name__)


@dataclass
class CompressionResult:
    """Result of a compression operation."""

    output_path: str
    bytes_written: int


class CompressionService:
    """Handles file compression for exports.

    Supports:
    - gzip compression (single file → .gz)
    - zipdeflate compression (single file → .zip)
    - Bundled archives (multiple files → .zip or .tar.gz)
    """

    def __init__(
        self,
        compression_type: Optional[str],
        compression_level: Optional[int] = None,
    ):
        """Initialize CompressionService.

        Args:
            compression_type: Compression type (gzip, zip, zipdeflate, etc.)
            compression_level: Optional compression level (gzip: 1-9, zip: 0-9)
        """
        self.compression_type = compression_type
        self.compression_level = compression_level

    def is_post_process_compression(self) -> bool:
        """Check if compression requires post-processing (not native Spark)."""
        return self.is_gzip() or self.is_zipdeflate()

    def is_gzip(self) -> bool:
        """Check if using gzip compression."""
        return self.compression_type in (CompressionType.GZIP, "gzip")

    def is_zipdeflate(self) -> bool:
        """Check if using zipdeflate/zip compression."""
        return self.compression_type in (
            CompressionType.ZIP,
            CompressionType.ZIPDEFLATE,
            "zip",
            "zipdeflate",
        )

    def compress_file(
        self,
        source_path: str,
        output_path: str,
        delete_source: bool = True,
    ) -> CompressionResult:
        """Compress a single file using configured compression type.

        Args:
            source_path: Absolute path to source file
            output_path: Absolute path for compressed output
            delete_source: If True, delete source file after compression

        Returns:
            CompressionResult with output path and bytes written
        """
        if self.is_gzip():
            return self._apply_gzip(source_path, output_path, delete_source)
        elif self.is_zipdeflate():
            return self._apply_zip(source_path, output_path, delete_source)
        raise ValueError(f"Unsupported compression type: {self.compression_type}")

    def bundle_files(
        self,
        source_paths: List[str],
        output_path: str,
        delete_sources: bool = True,
    ) -> CompressionResult:
        """Bundle multiple files into a single archive.

        Archive format determined by compression type:
        - zip/zipdeflate → .zip
        - gzip → .tar.gz

        Args:
            source_paths: List of absolute paths to source files
            output_path: Absolute path for archive output
            delete_sources: If True, delete source files after bundling

        Returns:
            CompressionResult with archive path and bytes written
        """
        if self.is_zipdeflate():
            return self._bundle_zip(source_paths, output_path, delete_sources)
        elif self.is_gzip():
            return self._bundle_tar_gz(source_paths, output_path, delete_sources)
        raise ValueError(f"Bundling not supported for: {self.compression_type}")

    def _apply_gzip(
        self,
        source_path: str,
        output_path: str,
        delete_source: bool,
    ) -> CompressionResult:
        """Apply gzip compression to a single file using streaming.

        Args:
            source_path: Absolute path to source file
            output_path: Absolute path for .gz output
            delete_source: If True, delete source file after compression
        """
        level = self.compression_level or 9

        logger.info(f"Compressing (gzip level {level}): {os.path.basename(source_path)}")

        with gzip.open(output_path, "wb", compresslevel=level) as dest:
            with open(source_path, "rb") as src:
                shutil.copyfileobj(src, dest)

        bytes_written = self._verify_archive(output_path)

        if delete_source:
            os.remove(source_path)
            logger.debug(f"Deleted source: {source_path}")

        return CompressionResult(output_path=output_path, bytes_written=bytes_written)

    def _apply_zip(
        self,
        source_path: str,
        output_path: str,
        delete_source: bool,
    ) -> CompressionResult:
        """Apply zipdeflate compression to a single file using streaming.

        Args:
            source_path: Absolute path to source file
            output_path: Absolute path for .zip output
            delete_source: If True, delete source file after compression
        """
        level = self.compression_level  # None uses library default
        filename_in_archive = os.path.basename(source_path)

        level_str = f"level {level}" if level is not None else "default level"
        logger.info(f"Compressing (zip {level_str}): {filename_in_archive}")

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=level, allowZip64=True) as zf:
            with zf.open(filename_in_archive, "w", force_zip64=True) as dest:
                with open(source_path, "rb") as src:
                    shutil.copyfileobj(src, dest)

        bytes_written = self._verify_archive(output_path)

        if delete_source:
            os.remove(source_path)
            logger.debug(f"Deleted source: {source_path}")

        return CompressionResult(output_path=output_path, bytes_written=bytes_written)

    def _bundle_zip(
        self,
        source_paths: List[str],
        output_path: str,
        delete_sources: bool,
    ) -> CompressionResult:
        """Bundle multiple files into a single ZIP archive.

        Args:
            source_paths: List of absolute paths to source files
            output_path: Absolute path for .zip output
            delete_sources: If True, delete source files after bundling
        """
        level = self.compression_level

        logger.info(f"Bundling {len(source_paths)} files into ZIP: {os.path.basename(output_path)}")

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=level) as zf:
            for source_path in source_paths:
                filename = os.path.basename(source_path)
                with zf.open(filename, "w") as dest:
                    with open(source_path, "rb") as src:
                        shutil.copyfileobj(src, dest)

        bytes_written = self._verify_archive(output_path)

        if delete_sources:
            for source_path in source_paths:
                os.remove(source_path)
            logger.debug(f"Deleted {len(source_paths)} source files")

        return CompressionResult(output_path=output_path, bytes_written=bytes_written)

    def _bundle_tar_gz(
        self,
        source_paths: List[str],
        output_path: str,
        delete_sources: bool,
    ) -> CompressionResult:
        """Bundle multiple files into a single tar.gz archive.

        Args:
            source_paths: List of absolute paths to source files
            output_path: Absolute path for .tar.gz output
            delete_sources: If True, delete source files after bundling
        """
        level = self.compression_level or 9

        logger.info(f"Bundling {len(source_paths)} files into tar.gz: {os.path.basename(output_path)}")

        with tarfile.open(output_path, "w:gz", compresslevel=level) as tf:
            for source_path in source_paths:
                tf.add(source_path, arcname=os.path.basename(source_path))

        bytes_written = self._verify_archive(output_path)

        if delete_sources:
            for source_path in source_paths:
                os.remove(source_path)
            logger.debug(f"Deleted {len(source_paths)} source files")

        return CompressionResult(output_path=output_path, bytes_written=bytes_written)

    def _verify_archive(self, archive_path: str) -> int:
        """Verify archive exists and is not empty, return size.

        Args:
            archive_path: Absolute path to archive

        Returns:
            Archive file size in bytes

        Raises:
            IOError: If archive doesn't exist or is empty
        """
        if not os.path.exists(archive_path):
            raise IOError(f"Archive not found: {archive_path}")

        size = os.path.getsize(archive_path)
        if size == 0:
            raise IOError(f"Archive is empty: {archive_path}")

        logger.info(f"Created archive: {os.path.basename(archive_path)} ({size:,} bytes)")
        return size

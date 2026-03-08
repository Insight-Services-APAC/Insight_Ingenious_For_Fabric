"""File utility functions for exports."""

from __future__ import annotations

from typing import Optional

from ingen_fab.python_libs.pyspark.export.common.constants import CompressionType


def get_file_extension(
    file_format: str,
    compression: Optional[str] = None,
    exclude_compression_ext: bool = False,
) -> str:
    """Get file extension including compression suffix.

    Args:
        file_format: File format (csv, parquet, json)
        compression: Compression type (gzip, zip, etc.)
        exclude_compression_ext: If True, don't add compression extension

    Returns:
        Extension string (e.g., ".csv", ".csv.gz", ".zip")
    """
    format_ext = {
        "csv": ".csv",
        "parquet": ".parquet",
        "json": ".json",
    }
    ext = format_ext.get(file_format, ".csv")

    if not exclude_compression_ext and compression and compression != CompressionType.NONE:
        if compression in (CompressionType.GZIP, "gzip"):
            ext += ".gz"
        elif compression in (
            CompressionType.ZIP,
            CompressionType.ZIPDEFLATE,
            "zip",
            "zipdeflate",
        ):
            ext = ".zip"

    return ext


def get_archive_extension(compression: Optional[str]) -> str:
    """Get archive extension for bundled files.

    Args:
        compression: Compression type

    Returns:
        Archive extension (.zip or .tar.gz)
    """
    if compression in (
        CompressionType.ZIP,
        CompressionType.ZIPDEFLATE,
        "zip",
        "zipdeflate",
    ):
        return ".zip"
    elif compression in (CompressionType.GZIP, "gzip"):
        return ".tar.gz"
    return ".zip"

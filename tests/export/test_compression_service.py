"""Tests for compression service."""

import gzip
import os
import tarfile
import tempfile
import zipfile

import pytest

from ingen_fab.python_libs.pyspark.export.writers.compression_service import (
    CompressionResult,
    CompressionService,
)


class TestCompressionServiceInit:
    """Tests for CompressionService initialization."""

    def test_init_with_gzip(self):
        """Test initialization with gzip compression."""
        svc = CompressionService("gzip", compression_level=6)
        assert svc.compression_type == "gzip"
        assert svc.compression_level == 6

    def test_init_with_zipdeflate(self):
        """Test initialization with zipdeflate compression."""
        svc = CompressionService("zipdeflate")
        assert svc.compression_type == "zipdeflate"
        assert svc.compression_level is None

    def test_init_with_none(self):
        """Test initialization with no compression."""
        svc = CompressionService(None)
        assert svc.compression_type is None


class TestCompressionServiceChecks:
    """Tests for compression type check methods."""

    def test_is_gzip(self):
        """Test is_gzip detection."""
        assert CompressionService("gzip").is_gzip() is True
        assert CompressionService("zip").is_gzip() is False
        assert CompressionService(None).is_gzip() is False

    def test_is_zipdeflate(self):
        """Test is_zipdeflate detection."""
        assert CompressionService("zipdeflate").is_zipdeflate() is True
        assert CompressionService("zip").is_zipdeflate() is True
        assert CompressionService("gzip").is_zipdeflate() is False

    def test_is_post_process_compression(self):
        """Test post-process compression detection."""
        assert CompressionService("gzip").is_post_process_compression() is True
        assert CompressionService("zipdeflate").is_post_process_compression() is True
        assert CompressionService("snappy").is_post_process_compression() is False
        assert CompressionService(None).is_post_process_compression() is False


class TestGzipCompression:
    """Tests for gzip compression."""

    def test_compress_file_gzip(self):
        """Test gzip compression of a single file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source file
            source_path = os.path.join(tmpdir, "test.csv")
            content = b"col1,col2\nval1,val2\nval3,val4\n"
            with open(source_path, "wb") as f:
                f.write(content)

            # Compress
            output_path = os.path.join(tmpdir, "test.csv.gz")
            svc = CompressionService("gzip", compression_level=9)
            result = svc.compress_file(source_path, output_path, delete_source=False)

            # Verify result
            assert isinstance(result, CompressionResult)
            assert result.output_path == output_path
            assert result.bytes_written > 0
            assert os.path.exists(output_path)
            assert os.path.exists(source_path)  # Not deleted

            # Verify content
            with gzip.open(output_path, "rb") as f:
                decompressed = f.read()
            assert decompressed == content

    def test_compress_file_gzip_delete_source(self):
        """Test gzip compression deletes source when requested."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "test.csv")
            with open(source_path, "wb") as f:
                f.write(b"test content")

            output_path = os.path.join(tmpdir, "test.csv.gz")
            svc = CompressionService("gzip")
            svc.compress_file(source_path, output_path, delete_source=True)

            assert os.path.exists(output_path)
            assert not os.path.exists(source_path)  # Deleted

    def test_compress_file_gzip_default_level(self):
        """Test gzip compression uses level 9 by default."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "test.csv")
            with open(source_path, "wb") as f:
                f.write(b"x" * 1000)  # Compressible content

            output_path = os.path.join(tmpdir, "test.csv.gz")
            svc = CompressionService("gzip")  # No level specified
            result = svc.compress_file(source_path, output_path, delete_source=False)

            # File should be significantly compressed
            assert result.bytes_written < 100


class TestZipCompression:
    """Tests for zip/zipdeflate compression."""

    def test_compress_file_zip(self):
        """Test zipdeflate compression of a single file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "test.csv")
            content = b"col1,col2\nval1,val2\n"
            with open(source_path, "wb") as f:
                f.write(content)

            output_path = os.path.join(tmpdir, "test.zip")
            svc = CompressionService("zipdeflate")
            result = svc.compress_file(source_path, output_path, delete_source=False)

            assert result.output_path == output_path
            assert result.bytes_written > 0
            assert os.path.exists(output_path)

            # Verify content
            with zipfile.ZipFile(output_path, "r") as zf:
                assert "test.csv" in zf.namelist()
                assert zf.read("test.csv") == content

    def test_compress_file_zip_delete_source(self):
        """Test zip compression deletes source when requested."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "test.csv")
            with open(source_path, "wb") as f:
                f.write(b"test content")

            output_path = os.path.join(tmpdir, "test.zip")
            svc = CompressionService("zip")
            svc.compress_file(source_path, output_path, delete_source=True)

            assert os.path.exists(output_path)
            assert not os.path.exists(source_path)


class TestBundleFiles:
    """Tests for bundling multiple files."""

    def test_bundle_zip(self):
        """Test bundling multiple files into ZIP archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source files
            files = []
            for i in range(3):
                path = os.path.join(tmpdir, f"file{i}.csv")
                with open(path, "wb") as f:
                    f.write(f"content{i}".encode())
                files.append(path)

            output_path = os.path.join(tmpdir, "bundle.zip")
            svc = CompressionService("zipdeflate")
            result = svc.bundle_files(files, output_path, delete_sources=False)

            assert result.bytes_written > 0
            assert os.path.exists(output_path)

            # Verify contents
            with zipfile.ZipFile(output_path, "r") as zf:
                names = zf.namelist()
                assert len(names) == 3
                assert "file0.csv" in names
                assert zf.read("file1.csv") == b"content1"

    def test_bundle_tar_gz(self):
        """Test bundling multiple files into tar.gz archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source files
            files = []
            for i in range(3):
                path = os.path.join(tmpdir, f"file{i}.csv")
                with open(path, "wb") as f:
                    f.write(f"content{i}".encode())
                files.append(path)

            output_path = os.path.join(tmpdir, "bundle.tar.gz")
            svc = CompressionService("gzip")
            result = svc.bundle_files(files, output_path, delete_sources=False)

            assert result.bytes_written > 0
            assert os.path.exists(output_path)

            # Verify contents
            with tarfile.open(output_path, "r:gz") as tf:
                names = tf.getnames()
                assert len(names) == 3
                assert "file0.csv" in names

    def test_bundle_deletes_sources(self):
        """Test bundling deletes source files when requested."""
        with tempfile.TemporaryDirectory() as tmpdir:
            files = []
            for i in range(2):
                path = os.path.join(tmpdir, f"file{i}.csv")
                with open(path, "wb") as f:
                    f.write(b"content")
                files.append(path)

            output_path = os.path.join(tmpdir, "bundle.zip")
            svc = CompressionService("zip")
            svc.bundle_files(files, output_path, delete_sources=True)

            # Sources should be deleted
            for path in files:
                assert not os.path.exists(path)


class TestVerifyArchive:
    """Tests for archive verification."""

    def test_verify_missing_archive(self):
        """Test verification fails for missing archive."""
        svc = CompressionService("gzip")
        with pytest.raises(IOError, match="not found"):
            svc._verify_archive("/nonexistent/path.gz")

    def test_verify_empty_archive(self):
        """Test verification fails for empty archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.gz")
            with open(path, "wb") as f:
                pass  # Create empty file

            svc = CompressionService("gzip")
            with pytest.raises(IOError, match="empty"):
                svc._verify_archive(path)


class TestUnsupportedCompression:
    """Tests for unsupported compression types."""

    def test_compress_unsupported_type(self):
        """Test compress_file raises for unsupported type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "test.csv")
            with open(source_path, "wb") as f:
                f.write(b"content")

            svc = CompressionService("snappy")
            with pytest.raises(ValueError, match="Unsupported"):
                svc.compress_file(source_path, "out.snappy")

    def test_bundle_unsupported_type(self):
        """Test bundle_files raises for unsupported type."""
        svc = CompressionService("snappy")
        with pytest.raises(ValueError, match="not supported"):
            svc.bundle_files(["file1.csv"], "bundle.snappy")

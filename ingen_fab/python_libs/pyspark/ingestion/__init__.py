# Data Integration Framework (ISOLATED - ALL NEW CODE)
# Everything in this folder is new and separate from existing flat_file_ingestion code

# Shared configs (used by both extraction and file loading)
from ingen_fab.python_libs.pyspark.ingestion.config import (
    APIExtractionParams,
    DatabaseExtractionParams,
    FileSystemExtractionParams,
    ResourceConfig,
    SourceConfig,
)

# Shared constants
from ingen_fab.python_libs.pyspark.ingestion.constants import (
    DatastoreType,
    DuplicateHandling,
    ExecutionStatus,
    ImportPattern,
    WriteMode,
)

# Shared results
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchExtractionResult,
    BatchInfo,
    ProcessingMetrics,
    ResourceExtractionResult,
    ResourceExecutionResult,
)

# Extraction Framework
from ingen_fab.python_libs.pyspark.ingestion.extractors.filesystem_extractor import (
    FileSystemExtractor,
)

# File Loading Framework (builds on shared configs)
from ingen_fab.python_libs.pyspark.ingestion.config_manager import ConfigManager
from ingen_fab.python_libs.pyspark.ingestion.loader import FileLoader
from ingen_fab.python_libs.pyspark.ingestion.loading_logger import LoadingLogger

__all__ = [
    # Configuration
    "SourceConfig",
    "ResourceConfig",
    "FileSystemExtractionParams",
    "APIExtractionParams",
    "DatabaseExtractionParams",
    # Constants
    "ExecutionStatus",
    "ImportPattern",
    "DuplicateHandling",
    "WriteMode",
    "DatastoreType",
    # Results
    "BatchExtractionResult",
    "BatchInfo",
    "ProcessingMetrics",
    "ResourceExtractionResult",
    "ResourceExecutionResult",
    # Extraction
    "FileSystemExtractor",
    # File Loading
    "ConfigManager",
    "FileLoader",
    "LoadingLogger",
]

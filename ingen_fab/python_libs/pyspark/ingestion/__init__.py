# Data Integration Framework (ISOLATED - ALL NEW CODE)
# Everything in this folder is new and separate from existing flat_file_ingestion code

# Shared configs (used by both extraction and file loading)
from ingen_fab.python_libs.pyspark.ingestion.config import (
    APIExtractionParams,
    DatabaseExtractionParams,
    FileSystemLoadingParams,
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
    BatchInfo,
    ProcessingMetrics,
    ResourceExecutionResult,
)

# File Loading Framework (builds on shared configs)
from ingen_fab.python_libs.pyspark.ingestion.batch_reader import BatchReader
from ingen_fab.python_libs.pyspark.ingestion.config_manager import ConfigManager
from ingen_fab.python_libs.pyspark.ingestion.file_discovery import FileDiscovery
from ingen_fab.python_libs.pyspark.ingestion.loader import FileLoader
from ingen_fab.python_libs.pyspark.ingestion.logging import FileLoadingLogger
from ingen_fab.python_libs.pyspark.ingestion.orchestrator import FileLoadingOrchestrator

__all__ = [
    # Configuration
    "SourceConfig",
    "ResourceConfig",
    "FileSystemLoadingParams",
    "APIExtractionParams",
    "DatabaseExtractionParams",
    # Constants
    "ExecutionStatus",
    "ImportPattern",
    "DuplicateHandling",
    "WriteMode",
    "DatastoreType",
    # Results
    "BatchInfo",
    "ProcessingMetrics",
    "ResourceExecutionResult",
    # File Loading
    "ConfigManager",
    "FileLoader",
    "FileDiscovery",
    "BatchReader",
    "FileLoadingOrchestrator",
    "FileLoadingLogger",
]

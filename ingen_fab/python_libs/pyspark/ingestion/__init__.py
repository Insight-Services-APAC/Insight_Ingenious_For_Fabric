# Data Integration Framework
# Organized into common, extraction, and loading subpackages

# Shared configs (used by both extraction and file loading)
from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    APIExtractionParams,
    DatabaseExtractionParams,
    FileSystemExtractionParams,
    ResourceConfig,
    SourceConfig,
)

# Shared constants
from ingen_fab.python_libs.pyspark.ingestion.common.constants import (
    DatastoreType,
    DuplicateHandling,
    ExecutionStatus,
    ImportPattern,
    WriteMode,
)

# Shared results
from ingen_fab.python_libs.pyspark.ingestion.common.results import (
    BatchExtractionResult,
    BatchInfo,
    ProcessingMetrics,
    ResourceExtractionResult,
    ResourceExecutionResult,
)

# Config Manager
from ingen_fab.python_libs.pyspark.ingestion.common.config_manager import ConfigIngestionManager

# Extraction Framework
from ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor import (
    FileSystemExtractor,
)
from ingen_fab.python_libs.pyspark.ingestion.extraction.extraction_logger import ExtractionLogger
from ingen_fab.python_libs.pyspark.ingestion.extraction.extraction_orchestrator import ExtractionOrchestrator

# Loading Framework
from ingen_fab.python_libs.pyspark.ingestion.loading.loader import FileLoader
from ingen_fab.python_libs.pyspark.ingestion.loading.loading_logger import LoadingLogger
from ingen_fab.python_libs.pyspark.ingestion.loading.loading_orchestrator import LoadingOrchestrator

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
    # Config Manager
    "ConfigIngestionManager",
    # Extraction
    "FileSystemExtractor",
    "ExtractionLogger",
    "ExtractionOrchestrator",
    # Loading
    "FileLoader",
    "LoadingLogger",
    "LoadingOrchestrator",
]

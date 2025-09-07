"""Utility modules for data profiling operations.

This package provides common utilities for data profiling, including:
- Data type detection and categorization
- Statistical calculations and metrics
- Naming pattern analysis and standardization  
- Report formatting and output generation
- Table discovery and management
- Profile exploration and analysis

Usage:
    from ingen_fab.packages.data_profiling.runtime.utils import (
        DataTypeUtils, StatisticalCalculations, TableNamingPatterns,
        ProfileReportFormatter, TableDiscoveryService, ProfileExplorer
    )
"""

# Data type utilities
from .data_types import (
    DataTypeCategory,
    DataTypeUtils,
    SparkDataTypeAnalyzer
)

# Statistical calculation utilities
from .calculations import (
    StatisticalCalculations,
    ProfilerCalculations,
    AggregationUtils
)

# Naming pattern utilities
from .naming_patterns import (
    NamingConvention,
    TableNamingPatterns,
    ColumnNamingPatterns,
    ProfileNamingUtils
)

# Report formatting utilities
from .formatters import (
    OutputFormat,
    ProfileReportFormatter,
    DataQualityFormatter,
    ComparisonFormatter
)

# Table discovery utilities
from .table_discovery import TableDiscoveryService

# Profile exploration utilities
from .profile_explorer import ProfileExplorer, ProfileSummary

__all__ = [
    # Data type utilities
    "DataTypeCategory",
    "DataTypeUtils", 
    "SparkDataTypeAnalyzer",
    
    # Statistical calculation utilities
    "StatisticalCalculations",
    "ProfilerCalculations",
    "AggregationUtils",
    
    # Naming pattern utilities
    "NamingConvention",
    "TableNamingPatterns",
    "ColumnNamingPatterns", 
    "ProfileNamingUtils",
    
    # Report formatting utilities
    "OutputFormat",
    "ProfileReportFormatter",
    "DataQualityFormatter",
    "ComparisonFormatter",
    
    # Table discovery utilities
    "TableDiscoveryService",
    
    # Profile exploration utilities
    "ProfileExplorer",
    "ProfileSummary",
]

# Version info
__version__ = "1.0.0"
__author__ = "Data Profiling Team"
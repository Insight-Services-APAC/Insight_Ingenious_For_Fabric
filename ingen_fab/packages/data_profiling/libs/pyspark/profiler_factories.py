"""
Profiler Factory Implementation for PySpark

This module provides the factory implementation for creating TieredProfiler instances
that can be registered with the ProfilerRegistry.
"""

from typing import Optional

from pyspark.sql import SparkSession

from ingen_fab.packages.data_profiling.libs.interfaces.data_profiling_interface import (
    DataProfilingInterface,
)
from ingen_fab.packages.data_profiling.libs.interfaces.profiler_registry import (
    ProfilerCapability,
    ProfilerFactory,
    ProfilerMetadata,
)
from ingen_fab.packages.data_profiling.libs.pyspark.tiered_profiler import (
    TieredProfiler,
)


class TieredProfilerFactory(ProfilerFactory):
    """Factory for creating tiered progressive profilers."""
    
    def create_profiler(self, **kwargs) -> DataProfilingInterface:
        """Create a tiered profiler instance."""
        spark = kwargs.get("spark")
        lakehouse = kwargs.get("lakehouse")
        table_prefix = kwargs.get("table_prefix", "tiered_profile")
        exclude_views = kwargs.get("exclude_views", True)  # Default to excluding views
        
        if not spark and not lakehouse:
            # Try to get or create a Spark session
            spark = SparkSession.builder.getOrCreate()
        
        return TieredProfiler(
            lakehouse=lakehouse,
            table_prefix=table_prefix,
            spark=spark,
            exclude_views=exclude_views
        )
    
    def get_metadata(self) -> ProfilerMetadata:
        """Get metadata for the tiered profiler."""
        return ProfilerMetadata(
            name="tiered",
            description="Progressive tiered profiler with 4 scan levels for incremental analysis",
            version="2.0.0",
            capabilities=[
                ProfilerCapability.BASIC_STATS,
                ProfilerCapability.STATISTICAL_ANALYSIS,
                ProfilerCapability.DATA_QUALITY,
                ProfilerCapability.RELATIONSHIP_DISCOVERY,
                ProfilerCapability.PERFORMANCE_OPTIMIZED,
                ProfilerCapability.INCREMENTAL,
            ],
            supported_datastores=["lakehouse", "warehouse", "dataframe"],
            performance_characteristics={
                "max_rows": 1_000_000_000,
                "memory_efficient": True,
                "single_pass": False,  # Multi-level scanning
                "staged_processing": True,
                "incremental": True,  # Can resume from any level
                "persistent_state": True,  # Saves progress between runs
            },
            priority=250,  # Highest priority - preferred profiler
        )
    
    def validate_config(self, **kwargs) -> bool:
        """Validate configuration for tiered profiler."""
        # Very flexible - works with spark, lakehouse, or both
        return True
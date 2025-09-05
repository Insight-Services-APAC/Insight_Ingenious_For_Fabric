"""
Profiler Factory Implementations for PySpark

This module provides factory implementations for creating different types of
PySpark-based profilers that can be registered with the ProfilerRegistry.
"""

from typing import Any, Dict, Optional
from pyspark.sql import SparkSession

from ingen_fab.python_libs.interfaces.profiler_registry import (
    ProfilerFactory,
    ProfilerMetadata,
    ProfilerCapability,
)
from ingen_fab.python_libs.interfaces.data_profiling_interface import DataProfilingInterface
from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark
from ingen_fab.python_libs.pyspark.ultra_fast_profiler import UltraFastProfiler


class StandardProfilerFactory(ProfilerFactory):
    """Factory for creating standard PySpark profilers."""
    
    def create_profiler(self, **kwargs) -> DataProfilingInterface:
        """Create a standard PySpark profiler instance."""
        spark = kwargs.get("spark")
        lakehouse = kwargs.get("lakehouse")
        
        if not spark and not lakehouse:
            # Try to get or create a Spark session
            spark = SparkSession.builder.getOrCreate()
        
        return DataProfilingPySpark(spark=spark, lakehouse=lakehouse)
    
    def get_metadata(self) -> ProfilerMetadata:
        """Get metadata for the standard profiler."""
        return ProfilerMetadata(
            name="standard",
            description="Standard PySpark data profiler with full feature set",
            version="1.0.0",
            capabilities=[
                ProfilerCapability.BASIC_STATS,
                ProfilerCapability.STATISTICAL_ANALYSIS,
                ProfilerCapability.DATA_QUALITY,
                ProfilerCapability.RELATIONSHIP_DISCOVERY,
            ],
            supported_datastores=["lakehouse", "warehouse", "dataframe"],
            performance_characteristics={
                "max_rows": 10_000_000,
                "memory_efficient": False,
                "single_pass": False,
            },
            priority=100,
        )
    
    def validate_config(self, **kwargs) -> bool:
        """Validate configuration for standard profiler."""
        # Standard profiler accepts spark session or lakehouse utils
        return True  # Very flexible configuration


class UltraFastProfilerFactory(ProfilerFactory):
    """Factory for creating ultra-fast single-pass profilers."""
    
    def create_profiler(self, **kwargs) -> DataProfilingInterface:
        """Create an ultra-fast profiler instance."""
        spark = kwargs.get("spark")
        
        if not spark:
            # Try to get or create a Spark session
            spark = SparkSession.builder.getOrCreate()
        
        return UltraFastProfiler(spark=spark)
    
    def get_metadata(self) -> ProfilerMetadata:
        """Get metadata for the ultra-fast profiler."""
        return ProfilerMetadata(
            name="ultra_fast",
            description="Ultra-fast single-pass profiler optimized for large datasets",
            version="1.0.0",
            capabilities=[
                ProfilerCapability.BASIC_STATS,
                ProfilerCapability.DATA_QUALITY,
                ProfilerCapability.PERFORMANCE_OPTIMIZED,
                ProfilerCapability.RELATIONSHIP_DISCOVERY,
            ],
            supported_datastores=["lakehouse", "dataframe"],
            performance_characteristics={
                "max_rows": 100_000_000,
                "memory_efficient": True,
                "single_pass": True,
                "staged_processing": True,
            },
            priority=200,  # Higher priority for performance scenarios
        )
    
    def validate_config(self, **kwargs) -> bool:
        """Validate configuration for ultra-fast profiler."""
        # Requires spark session
        return "spark" in kwargs or SparkSession._instantiatedSession is not None


class OptimizedProfilerFactory(ProfilerFactory):
    """Factory for creating optimized profilers with configurable performance settings."""
    
    def create_profiler(self, **kwargs) -> DataProfilingInterface:
        """Create an optimized profiler instance."""
        spark = kwargs.get("spark")
        
        if not spark:
            spark = SparkSession.builder.getOrCreate()
        
        # Try to import optimized profiler
        try:
            from ingen_fab.python_libs.pyspark.data_profiling_pyspark_optimized import (
                OptimizedDataProfilingPySpark,
                ProfileConfig,
            )
            
            # Create performance config from kwargs
            config = ProfileConfig(
                max_top_values=kwargs.get("max_top_values", 100),
                max_correlation_columns=kwargs.get("max_correlation_columns", 20),
                auto_sample_threshold=kwargs.get("auto_sample_threshold", 1_000_000),
            )
            
            return OptimizedDataProfilingPySpark(spark=spark, config=config)
        except ImportError:
            # Fall back to standard profiler if optimized not available
            return DataProfilingPySpark(spark=spark)
    
    def get_metadata(self) -> ProfilerMetadata:
        """Get metadata for the optimized profiler."""
        return ProfilerMetadata(
            name="optimized",
            description="Performance-optimized profiler with configurable settings",
            version="1.0.0",
            capabilities=[
                ProfilerCapability.BASIC_STATS,
                ProfilerCapability.STATISTICAL_ANALYSIS,
                ProfilerCapability.DATA_QUALITY,
                ProfilerCapability.PERFORMANCE_OPTIMIZED,
            ],
            supported_datastores=["lakehouse", "warehouse", "dataframe"],
            performance_characteristics={
                "max_rows": 50_000_000,
                "memory_efficient": True,
                "single_pass": False,
                "configurable": True,
            },
            priority=150,
        )
    
    def validate_config(self, **kwargs) -> bool:
        """Validate configuration for optimized profiler."""
        # Check for valid numeric parameters if provided
        numeric_params = ["max_top_values", "max_correlation_columns", "auto_sample_threshold"]
        for param in numeric_params:
            if param in kwargs:
                value = kwargs[param]
                if not isinstance(value, (int, float)) or value <= 0:
                    return False
        return True


class CustomProfilerFactory(ProfilerFactory):
    """
    Generic factory for creating custom profiler implementations.
    
    This can be used to wrap any DataProfilingInterface implementation
    as a factory for registration with the ProfilerRegistry.
    """
    
    def __init__(
        self,
        profiler_class: type,
        metadata: ProfilerMetadata,
        config_validator: Optional[callable] = None,
    ):
        """
        Initialize custom profiler factory.
        
        Args:
            profiler_class: Class implementing DataProfilingInterface
            metadata: Metadata describing the profiler
            config_validator: Optional function to validate configuration
        """
        self.profiler_class = profiler_class
        self.metadata = metadata
        self.config_validator = config_validator or (lambda **kwargs: True)
    
    def create_profiler(self, **kwargs) -> DataProfilingInterface:
        """Create a custom profiler instance."""
        return self.profiler_class(**kwargs)
    
    def get_metadata(self) -> ProfilerMetadata:
        """Get metadata for the custom profiler."""
        return self.metadata
    
    def validate_config(self, **kwargs) -> bool:
        """Validate configuration for custom profiler."""
        return self.config_validator(**kwargs)


# Convenience functions for creating profiler factories

def create_profiler_factory(
    name: str,
    profiler_class: type,
    description: str,
    capabilities: list,
    supported_datastores: list,
    **performance_characteristics
) -> ProfilerFactory:
    """
    Create a profiler factory for a custom profiler class.
    
    Args:
        name: Name for the profiler
        profiler_class: Class implementing DataProfilingInterface
        description: Description of the profiler
        capabilities: List of ProfilerCapability values
        supported_datastores: List of supported datastore types
        **performance_characteristics: Additional performance characteristics
        
    Returns:
        ProfilerFactory instance for the custom profiler
    """
    metadata = ProfilerMetadata(
        name=name,
        description=description,
        version="1.0.0",
        capabilities=capabilities,
        supported_datastores=supported_datastores,
        performance_characteristics=performance_characteristics,
    )
    
    return CustomProfilerFactory(profiler_class, metadata)
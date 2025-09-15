"""Configuration module for the data profiling runtime system.

This module provides a comprehensive configuration system for the tiered data profiler,
including configuration for profiling execution, persistence, data quality assessment,
and runtime orchestration.

Key Components:
- BaseConfig: Abstract base class with common configuration functionality
- TieredProfilerConfig: Main profiler execution configuration 
- PersistenceConfig: Configuration for data persistence backends
- QualityConfig: Data quality assessment and threshold configuration
- RuntimeConfig: Top-level runtime configuration orchestrating all components

Usage:
    from ingen_fab.packages.data_profiling.runtime.config import (
        RuntimeConfig, TieredProfilerConfig, PersistenceConfig, QualityConfig
    )
    
    # Create default configuration for production environment
    config = RuntimeConfig.create_for_environment("production")
    
    # Load configuration from files
    config = RuntimeConfig.load_from_files(Path("config/"))
    
    # Configure for specific workspace
    config.apply_workspace_settings("workspace_id", "lakehouse_id")
"""

# Base configuration system
from .base_config import (
    BaseConfig,
    ConfigSource,
    ConfigFormat,
    ConfigMetadata,
    ConfigurationLoader,
    ConfigValidator
)

# Profiler configuration
from .profiler_config import (
    TieredProfilerConfig,
    ProfilingMode,
    SamplingStrategy,
    ScanLevelConfig,
    PerformanceConfig,
    FilterConfig
)

# Persistence configuration
from .persistence_config import (
    PersistenceConfig,
    PersistenceBackend,
    TableFormat,
    CompressionType,
    TableConfig,
    ConnectionConfig
)

# Quality configuration
from .quality_config import (
    QualityConfig,
    QualityDimension,
    ThresholdLevel,
    AlertSeverity,
    QualityThreshold,
    OutlierDetectionConfig,
    ValidationRule,
    ColumnQualityConfig
)

# Runtime configuration
from .runtime_config import RuntimeConfig

__all__ = [
    # Base configuration system
    "BaseConfig",
    "ConfigSource",
    "ConfigFormat", 
    "ConfigMetadata",
    "ConfigurationLoader",
    "ConfigValidator",
    
    # Profiler configuration
    "TieredProfilerConfig",
    "ProfilingMode",
    "SamplingStrategy", 
    "ScanLevelConfig",
    "PerformanceConfig",
    "FilterConfig",
    
    # Persistence configuration
    "PersistenceConfig",
    "PersistenceBackend",
    "TableFormat",
    "CompressionType",
    "TableConfig",
    "ConnectionConfig",
    
    # Quality configuration
    "QualityConfig",
    "QualityDimension",
    "ThresholdLevel",
    "AlertSeverity",
    "QualityThreshold",
    "OutlierDetectionConfig",
    "ValidationRule",
    "ColumnQualityConfig",
    
    # Runtime configuration
    "RuntimeConfig",
]

# Configuration presets for common scenarios
def create_development_config(workspace_id: str = None, lakehouse_id: str = None) -> RuntimeConfig:
    """Create optimized configuration for development environment."""
    return RuntimeConfig.create_for_environment(
        environment="development",
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id
    )

def create_testing_config(workspace_id: str = None, lakehouse_id: str = None) -> RuntimeConfig:
    """Create optimized configuration for testing environment."""  
    return RuntimeConfig.create_for_environment(
        environment="testing",
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id
    )

def create_production_config(workspace_id: str, lakehouse_id: str) -> RuntimeConfig:
    """Create optimized configuration for production environment."""
    if not workspace_id or not lakehouse_id:
        raise ValueError("workspace_id and lakehouse_id are required for production config")
    
    return RuntimeConfig.create_for_environment(
        environment="production",
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id
    )

def load_config_from_env(env_prefix: str = "DATA_PROFILING_") -> RuntimeConfig:
    """Load configuration from environment variables."""
    config = RuntimeConfig()
    
    # Load component configs from environment
    config.profiler = ConfigurationLoader.load_config(
        TieredProfilerConfig,
        env_prefix=f"{env_prefix}PROFILER_",
        validate=False
    )
    
    config.persistence = ConfigurationLoader.load_config(
        PersistenceConfig,
        env_prefix=f"{env_prefix}PERSISTENCE_",
        validate=False
    )
    
    config.quality = ConfigurationLoader.load_config(
        QualityConfig,
        env_prefix=f"{env_prefix}QUALITY_",
        validate=False
    )
    
    # Load runtime config from environment
    runtime_config_data = {}
    import os
    
    for key, value in os.environ.items():
        if key.startswith(env_prefix):
            config_key = key[len(env_prefix):].lower()
            runtime_config_data[config_key] = BaseConfig._parse_env_value(value)
    
    # Apply environment settings
    for key, value in runtime_config_data.items():
        if hasattr(config, key):
            setattr(config, key, value)
    
    return config

# Version and metadata
__version__ = "1.0.0"
__author__ = "Data Profiling Team"
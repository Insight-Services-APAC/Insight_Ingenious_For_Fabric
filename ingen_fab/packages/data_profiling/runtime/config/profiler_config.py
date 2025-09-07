"""Configuration classes for the tiered data profiler."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from enum import Enum

from .base_config import BaseConfig, ConfigValidator
from ..core.enums.profile_types import ScanLevel, ProfileType


class ProfilingMode(Enum):
    """Profiling execution modes."""
    DEVELOPMENT = "development"      # Fast sampling for development
    TESTING = "testing"             # Minimal sampling for testing
    PRODUCTION = "production"       # Full profiling for production
    EXPLORATION = "exploration"     # Deep profiling for data exploration


class SamplingStrategy(Enum):
    """Data sampling strategies."""
    NONE = "none"                   # No sampling
    RANDOM = "random"               # Random sampling
    SYSTEMATIC = "systematic"       # Systematic sampling (every nth row)
    STRATIFIED = "stratified"       # Stratified sampling by key column
    AUTO = "auto"                   # Automatic sampling based on data size


@dataclass
class ScanLevelConfig:
    """Configuration for individual scan levels."""
    enabled: bool = True
    timeout_seconds: int = 3600
    max_tables: Optional[int] = None
    max_columns_per_table: Optional[int] = None
    sample_size: Optional[float] = None
    parallel_execution: bool = True
    custom_params: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> List[str]:
        """Validate scan level configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_range(
            self.timeout_seconds, min_val=1, field_name="timeout_seconds"
        ))
        
        if self.max_tables is not None:
            errors.extend(ConfigValidator.validate_range(
                self.max_tables, min_val=1, field_name="max_tables"
            ))
        
        if self.max_columns_per_table is not None:
            errors.extend(ConfigValidator.validate_range(
                self.max_columns_per_table, min_val=1, field_name="max_columns_per_table"
            ))
        
        if self.sample_size is not None:
            errors.extend(ConfigValidator.validate_range(
                self.sample_size, min_val=0.0, max_val=1.0, field_name="sample_size"
            ))
        
        return errors


@dataclass 
class PerformanceConfig:
    """Performance and resource configuration."""
    # Memory settings
    max_memory_gb: float = 8.0
    memory_per_executor_gb: float = 2.0
    
    # Parallelism settings
    max_parallel_tables: int = 4
    max_parallel_columns: int = 10
    spark_partitions: Optional[int] = None
    
    # Optimization settings
    enable_caching: bool = True
    cache_level: str = "MEMORY_AND_DISK"
    checkpoint_interval: int = 100
    
    # Sampling settings
    auto_sample_threshold_rows: int = 1_000_000
    auto_sample_fraction: float = 0.1
    max_sample_rows: int = 100_000
    
    # Statistics settings
    max_distinct_values: int = 1000
    max_histogram_bins: int = 50
    correlation_threshold: int = 20  # Skip correlation if more numeric columns
    
    def validate(self) -> List[str]:
        """Validate performance configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_range(
            self.max_memory_gb, min_val=0.5, field_name="max_memory_gb"
        ))
        
        errors.extend(ConfigValidator.validate_range(
            self.memory_per_executor_gb, min_val=0.1, field_name="memory_per_executor_gb"
        ))
        
        errors.extend(ConfigValidator.validate_range(
            self.max_parallel_tables, min_val=1, field_name="max_parallel_tables"
        ))
        
        errors.extend(ConfigValidator.validate_range(
            self.max_parallel_columns, min_val=1, field_name="max_parallel_columns"
        ))
        
        errors.extend(ConfigValidator.validate_range(
            self.auto_sample_fraction, min_val=0.001, max_val=1.0, field_name="auto_sample_fraction"
        ))
        
        errors.extend(ConfigValidator.validate_choices(
            self.cache_level,
            ["NONE", "DISK_ONLY", "MEMORY_ONLY", "MEMORY_AND_DISK", "MEMORY_ONLY_SER", "MEMORY_AND_DISK_SER"],
            "cache_level"
        ))
        
        return errors


@dataclass
class FilterConfig:
    """Configuration for table and column filtering."""
    # Table filtering
    include_tables: List[str] = field(default_factory=list)
    exclude_tables: List[str] = field(default_factory=list)
    table_name_patterns: List[str] = field(default_factory=list)
    exclude_views: bool = True
    exclude_temp_tables: bool = True
    
    # Column filtering
    include_columns: List[str] = field(default_factory=list)
    exclude_columns: List[str] = field(default_factory=list)
    column_name_patterns: List[str] = field(default_factory=list)
    max_string_length: int = 10000  # Skip very long text columns
    
    # Data type filtering
    include_data_types: Set[str] = field(default_factory=set)
    exclude_data_types: Set[str] = field(default_factory=lambda: {"binary", "array", "map", "struct"})
    
    def validate(self) -> List[str]:
        """Validate filter configuration."""
        errors = []
        
        errors.extend(ConfigValidator.validate_range(
            self.max_string_length, min_val=1, field_name="max_string_length"
        ))
        
        # Check for conflicting table filters
        if self.include_tables and self.exclude_tables:
            overlap = set(self.include_tables) & set(self.exclude_tables)
            if overlap:
                errors.append(f"Tables cannot be both included and excluded: {overlap}")
        
        # Check for conflicting column filters
        if self.include_columns and self.exclude_columns:
            overlap = set(self.include_columns) & set(self.exclude_columns)
            if overlap:
                errors.append(f"Columns cannot be both included and excluded: {overlap}")
        
        return errors


class TieredProfilerConfig(BaseConfig):
    """Main configuration for the tiered profiler system."""
    
    def __init__(self):
        """Initialize profiler configuration with defaults."""
        super().__init__()
        
        # Basic settings
        self.profiler_name: str = "TieredProfiler"
        self.version: str = "2.0.0"
        self.description: str = "Tiered Data Profiler"
        
        # Execution settings
        self.mode: ProfilingMode = ProfilingMode.PRODUCTION
        self.enabled_scan_levels: List[ScanLevel] = [
            ScanLevel.LEVEL_1_DISCOVERY,
            ScanLevel.LEVEL_2_SCHEMA
        ]
        self.resume_from_checkpoint: bool = True
        self.fail_on_error: bool = False
        
        # Sampling configuration
        self.sampling_strategy: SamplingStrategy = SamplingStrategy.AUTO
        self.global_sample_size: Optional[float] = None
        
        # Component configurations
        self.performance: PerformanceConfig = PerformanceConfig()
        self.filters: FilterConfig = FilterConfig()
        self.scan_levels: Dict[ScanLevel, ScanLevelConfig] = {
            ScanLevel.LEVEL_1_DISCOVERY: ScanLevelConfig(
                timeout_seconds=1800,
                parallel_execution=True
            ),
            ScanLevel.LEVEL_2_SCHEMA: ScanLevelConfig(
                timeout_seconds=3600,
                max_columns_per_table=500,
                parallel_execution=True
            ),
            ScanLevel.LEVEL_3_PROFILE: ScanLevelConfig(
                timeout_seconds=7200,
                max_columns_per_table=100,
                sample_size=0.1,
                enabled=False  # Not implemented yet
            ),
            ScanLevel.LEVEL_4_ADVANCED: ScanLevelConfig(
                timeout_seconds=14400,
                max_columns_per_table=50,
                sample_size=0.05,
                enabled=False  # Not implemented yet
            )
        }
        
        # Output settings
        self.output_formats: List[str] = ["json"]
        self.generate_reports: bool = True
        self.save_intermediate_results: bool = True
        
        # Advanced settings
        self.debug_mode: bool = False
        self.log_level: str = "INFO"
        self.custom_variables: Dict[str, Any] = {}
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "profiler_name": "TieredProfiler",
            "version": "2.0.0",
            "mode": ProfilingMode.PRODUCTION.value,
            "enabled_scan_levels": [level.value for level in [ScanLevel.LEVEL_1_DISCOVERY, ScanLevel.LEVEL_2_SCHEMA]],
            "sampling_strategy": SamplingStrategy.AUTO.value,
            "resume_from_checkpoint": True,
            "fail_on_error": False,
            "performance": {
                "max_memory_gb": 8.0,
                "max_parallel_tables": 4,
                "enable_caching": True,
                "auto_sample_threshold_rows": 1_000_000
            },
            "output_formats": ["json"],
            "generate_reports": True,
            "debug_mode": False,
            "log_level": "INFO"
        }
    
    def validate(self) -> bool:
        """Validate the profiler configuration."""
        errors = []
        
        # Validate basic settings
        errors.extend(ConfigValidator.validate_required(self.profiler_name, "profiler_name"))
        errors.extend(ConfigValidator.validate_choices(
            self.mode, list(ProfilingMode), "mode"
        ))
        errors.extend(ConfigValidator.validate_choices(
            self.sampling_strategy, list(SamplingStrategy), "sampling_strategy"
        ))
        errors.extend(ConfigValidator.validate_choices(
            self.log_level, ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], "log_level"
        ))
        
        # Validate enabled scan levels
        if not self.enabled_scan_levels:
            errors.append("At least one scan level must be enabled")
        
        for scan_level in self.enabled_scan_levels:
            if not isinstance(scan_level, ScanLevel):
                errors.append(f"Invalid scan level: {scan_level}")
        
        # Validate global sample size
        if self.global_sample_size is not None:
            errors.extend(ConfigValidator.validate_range(
                self.global_sample_size, min_val=0.001, max_val=1.0, field_name="global_sample_size"
            ))
        
        # Validate component configurations
        errors.extend(self.performance.validate())
        errors.extend(self.filters.validate())
        
        # Validate scan level configurations
        for scan_level, config in self.scan_levels.items():
            level_errors = config.validate()
            errors.extend([f"scan_levels.{scan_level.value}.{error}" for error in level_errors])
        
        # Store validation results
        self._metadata.validation_errors = errors
        
        return len(errors) == 0
    
    def get_scan_level_config(self, scan_level: ScanLevel) -> ScanLevelConfig:
        """Get configuration for a specific scan level."""
        return self.scan_levels.get(scan_level, ScanLevelConfig())
    
    def enable_scan_level(self, scan_level: ScanLevel, config: Optional[ScanLevelConfig] = None):
        """Enable a scan level with optional custom configuration."""
        if scan_level not in self.enabled_scan_levels:
            self.enabled_scan_levels.append(scan_level)
        
        if config:
            self.scan_levels[scan_level] = config
        elif scan_level not in self.scan_levels:
            self.scan_levels[scan_level] = ScanLevelConfig()
        
        self.scan_levels[scan_level].enabled = True
    
    def disable_scan_level(self, scan_level: ScanLevel):
        """Disable a scan level."""
        if scan_level in self.enabled_scan_levels:
            self.enabled_scan_levels.remove(scan_level)
        
        if scan_level in self.scan_levels:
            self.scan_levels[scan_level].enabled = False
    
    def configure_for_mode(self, mode: ProfilingMode):
        """Configure profiler for a specific mode."""
        self.mode = mode
        
        if mode == ProfilingMode.DEVELOPMENT:
            # Fast development settings
            self.global_sample_size = 0.1
            self.performance.max_parallel_tables = 2
            self.performance.enable_caching = False
            self.enabled_scan_levels = [ScanLevel.LEVEL_1_DISCOVERY]
            
        elif mode == ProfilingMode.TESTING:
            # Minimal testing settings
            self.global_sample_size = 0.01
            self.performance.max_parallel_tables = 1
            self.enabled_scan_levels = [ScanLevel.LEVEL_1_DISCOVERY]
            
        elif mode == ProfilingMode.EXPLORATION:
            # Deep exploration settings
            self.global_sample_size = 0.5
            self.enabled_scan_levels = list(ScanLevel)
            self.performance.max_parallel_tables = 2
            
        elif mode == ProfilingMode.PRODUCTION:
            # Full production settings
            self.global_sample_size = None  # No sampling by default
            self.enabled_scan_levels = [ScanLevel.LEVEL_1_DISCOVERY, ScanLevel.LEVEL_2_SCHEMA]
            self.performance.enable_caching = True
    
    def apply_preset(self, preset_name: str):
        """Apply a named configuration preset."""
        presets = {
            "minimal": {
                "enabled_scan_levels": [ScanLevel.LEVEL_1_DISCOVERY],
                "global_sample_size": 0.01,
                "performance": {"max_parallel_tables": 1}
            },
            "standard": {
                "enabled_scan_levels": [ScanLevel.LEVEL_1_DISCOVERY, ScanLevel.LEVEL_2_SCHEMA],
                "global_sample_size": 0.1,
                "performance": {"max_parallel_tables": 4}
            },
            "comprehensive": {
                "enabled_scan_levels": list(ScanLevel),
                "global_sample_size": None,
                "performance": {"max_parallel_tables": 8, "max_memory_gb": 16.0}
            }
        }
        
        if preset_name not in presets:
            raise ValueError(f"Unknown preset: {preset_name}. Available: {list(presets.keys())}")
        
        preset_data = presets[preset_name]
        
        # Apply preset settings
        for key, value in preset_data.items():
            if key == "performance":
                for perf_key, perf_value in value.items():
                    setattr(self.performance, perf_key, perf_value)
            else:
                setattr(self, key, value)
    
    def get_effective_sample_size(self, table_name: str) -> Optional[float]:
        """Get the effective sample size for a table."""
        # Check for table-specific sample size in custom variables
        table_sample = self.custom_variables.get(f"sample_size_{table_name}")
        if table_sample is not None:
            return table_sample
        
        # Return global sample size
        return self.global_sample_size
    
    def should_profile_table(self, table_name: str) -> bool:
        """Check if a table should be profiled based on filters."""
        # Check include list (if specified, table must be in it)
        if self.filters.include_tables:
            if table_name not in self.filters.include_tables:
                return False
        
        # Check exclude list
        if table_name in self.filters.exclude_tables:
            return False
        
        # Check patterns
        import re
        for pattern in self.filters.table_name_patterns:
            if re.match(pattern, table_name):
                return True
        
        # If no patterns specified, include by default
        if not self.filters.table_name_patterns:
            return True
        
        return False
    
    def should_profile_column(self, column_name: str, data_type: str) -> bool:
        """Check if a column should be profiled based on filters."""
        # Check data type filters
        if self.filters.exclude_data_types and data_type.lower() in self.filters.exclude_data_types:
            return False
        
        if self.filters.include_data_types and data_type.lower() not in self.filters.include_data_types:
            return False
        
        # Check include list
        if self.filters.include_columns:
            if column_name not in self.filters.include_columns:
                return False
        
        # Check exclude list
        if column_name in self.filters.exclude_columns:
            return False
        
        return True
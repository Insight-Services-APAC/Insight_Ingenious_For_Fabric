"""
Configuration Builder for Data Profiling

This module provides a builder pattern for constructing profiling configurations
with validation and default value management.
"""

from typing import Any, Dict, Optional, List, Set
from dataclasses import dataclass, field
from pathlib import Path

from ingen_fab.python_libs.interfaces.profiler_registry import (
    get_registry,
    ProfilerCapability,
)


@dataclass
class ProfilingConfiguration:
    """Configuration for data profiling operations."""
    
    # Basic settings
    profile_type: str = "full"
    save_to_catalog: bool = True
    generate_report: bool = True
    output_format: str = "yaml"
    sample_size: Optional[float] = None
    target_tables: List[str] = field(default_factory=list)
    
    # Performance settings
    auto_sample_large_tables: bool = True
    max_correlation_columns: int = 20
    top_values_limit: int = 100
    enable_performance_mode: bool = False
    enable_ultra_fast_mode: bool = True
    
    # Quality thresholds
    quality_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "completeness": 0.95,
        "uniqueness": 0.99,
        "validity": 0.98,
    })
    
    # Configuration metadata
    auto_discover_tables: bool = True
    profile_frequency: str = "daily"
    
    # Profiler selection
    preferred_profiler: Optional[str] = None
    required_capabilities: List[str] = field(default_factory=list)
    
    # Template variables
    custom_template_vars: Dict[str, Any] = field(default_factory=dict)


class ConfigurationBuilder:
    """
    Builder for creating and validating profiling configurations.
    
    This class provides a fluent API for building configurations with
    validation and intelligent defaults.
    """
    
    def __init__(self):
        """Initialize the configuration builder."""
        self._config = ProfilingConfiguration()
        self._errors: List[str] = []
        self._warnings: List[str] = []
    
    def profile_type(self, profile_type: str) -> "ConfigurationBuilder":
        """
        Set the profile type.
        
        Args:
            profile_type: Type of profiling (basic, statistical, data_quality, relationship, full)
            
        Returns:
            Self for method chaining
        """
        valid_types = ["basic", "statistical", "data_quality", "relationship", "full"]
        if profile_type not in valid_types:
            self._errors.append(f"Invalid profile type: {profile_type}. Must be one of {valid_types}")
        else:
            self._config.profile_type = profile_type
        return self
    
    def output_settings(
        self,
        save_to_catalog: bool = True,
        generate_report: bool = True,
        output_format: str = "yaml",
    ) -> "ConfigurationBuilder":
        """
        Configure output settings.
        
        Args:
            save_to_catalog: Whether to save results to catalog tables
            generate_report: Whether to generate formatted reports
            output_format: Format for reports (yaml, html, markdown, json)
            
        Returns:
            Self for method chaining
        """
        valid_formats = ["yaml", "html", "markdown", "json"]
        if output_format not in valid_formats:
            self._errors.append(f"Invalid output format: {output_format}. Must be one of {valid_formats}")
        else:
            self._config.save_to_catalog = save_to_catalog
            self._config.generate_report = generate_report
            self._config.output_format = output_format
        return self
    
    def sampling(self, sample_size: Optional[float] = None) -> "ConfigurationBuilder":
        """
        Configure data sampling.
        
        Args:
            sample_size: Fraction of data to sample (0-1), None for full dataset
            
        Returns:
            Self for method chaining
        """
        if sample_size is not None:
            if not 0 < sample_size <= 1:
                self._errors.append(f"Sample size must be between 0 and 1, got {sample_size}")
            else:
                self._config.sample_size = sample_size
        return self
    
    def target_tables(self, tables: List[str]) -> "ConfigurationBuilder":
        """
        Set specific tables to profile.
        
        Args:
            tables: List of table names to profile
            
        Returns:
            Self for method chaining
        """
        if not isinstance(tables, list):
            self._errors.append("Target tables must be a list")
        else:
            self._config.target_tables = tables
        return self
    
    def performance_tuning(
        self,
        auto_sample_large_tables: bool = True,
        max_correlation_columns: int = 20,
        top_values_limit: int = 100,
        enable_performance_mode: bool = False,
        enable_ultra_fast_mode: bool = True,
    ) -> "ConfigurationBuilder":
        """
        Configure performance settings.
        
        Args:
            auto_sample_large_tables: Auto-sample tables > 1M rows
            max_correlation_columns: Skip correlation if more numeric columns
            top_values_limit: Max distinct values to collect per column
            enable_performance_mode: Use optimized profiling
            enable_ultra_fast_mode: Use single-pass ultra-fast profiling
            
        Returns:
            Self for method chaining
        """
        if max_correlation_columns <= 0:
            self._errors.append("max_correlation_columns must be positive")
        if top_values_limit <= 0:
            self._errors.append("top_values_limit must be positive")
        
        if enable_performance_mode and enable_ultra_fast_mode:
            self._warnings.append(
                "Both performance_mode and ultra_fast_mode enabled. ultra_fast_mode will take precedence."
            )
        
        self._config.auto_sample_large_tables = auto_sample_large_tables
        self._config.max_correlation_columns = max_correlation_columns
        self._config.top_values_limit = top_values_limit
        self._config.enable_performance_mode = enable_performance_mode
        self._config.enable_ultra_fast_mode = enable_ultra_fast_mode
        
        return self
    
    def quality_thresholds(
        self,
        completeness: float = 0.95,
        uniqueness: float = 0.99,
        validity: float = 0.98,
    ) -> "ConfigurationBuilder":
        """
        Set data quality thresholds.
        
        Args:
            completeness: Minimum acceptable completeness ratio
            uniqueness: Minimum acceptable uniqueness ratio
            validity: Minimum acceptable validity ratio
            
        Returns:
            Self for method chaining
        """
        thresholds = {"completeness": completeness, "uniqueness": uniqueness, "validity": validity}
        
        for name, value in thresholds.items():
            if not 0 <= value <= 1:
                self._errors.append(f"{name} threshold must be between 0 and 1, got {value}")
        
        if not self._errors:
            self._config.quality_thresholds = thresholds
        
        return self
    
    def automation_settings(
        self,
        auto_discover_tables: bool = True,
        profile_frequency: str = "daily",
    ) -> "ConfigurationBuilder":
        """
        Configure automation settings.
        
        Args:
            auto_discover_tables: Automatically discover tables to profile
            profile_frequency: How often to run profiling (daily, weekly, monthly)
            
        Returns:
            Self for method chaining
        """
        valid_frequencies = ["daily", "weekly", "monthly", "on_demand"]
        if profile_frequency not in valid_frequencies:
            self._errors.append(f"Invalid profile frequency: {profile_frequency}. Must be one of {valid_frequencies}")
        else:
            self._config.auto_discover_tables = auto_discover_tables
            self._config.profile_frequency = profile_frequency
        
        return self
    
    def profiler_selection(
        self,
        preferred_profiler: Optional[str] = None,
        required_capabilities: Optional[List[str]] = None,
    ) -> "ConfigurationBuilder":
        """
        Configure profiler selection criteria.
        
        Args:
            preferred_profiler: Name of preferred profiler to use
            required_capabilities: List of required profiler capabilities
            
        Returns:
            Self for method chaining
        """
        registry = get_registry()
        
        if preferred_profiler:
            available_profilers = registry.list_profilers()
            if preferred_profiler not in available_profilers:
                self._warnings.append(
                    f"Preferred profiler '{preferred_profiler}' not available. "
                    f"Available: {available_profilers}"
                )
            else:
                self._config.preferred_profiler = preferred_profiler
        
        if required_capabilities:
            # Validate capability names
            valid_capabilities = [cap.value for cap in ProfilerCapability]
            invalid_caps = [cap for cap in required_capabilities if cap not in valid_capabilities]
            if invalid_caps:
                self._errors.append(f"Invalid capabilities: {invalid_caps}. Valid: {valid_capabilities}")
            else:
                self._config.required_capabilities = required_capabilities
        
        return self
    
    def custom_variables(self, **kwargs) -> "ConfigurationBuilder":
        """
        Add custom template variables.
        
        Args:
            **kwargs: Custom variables to pass to templates
            
        Returns:
            Self for method chaining
        """
        self._config.custom_template_vars.update(kwargs)
        return self
    
    def for_datastore(self, datastore: str) -> "ConfigurationBuilder":
        """
        Apply datastore-specific optimizations.
        
        Args:
            datastore: Target datastore (lakehouse, warehouse)
            
        Returns:
            Self for method chaining
        """
        if datastore == "lakehouse":
            # Lakehouse optimizations
            self._config.enable_ultra_fast_mode = True
            self._config.auto_sample_large_tables = True
        elif datastore == "warehouse":
            # Warehouse optimizations
            self._config.enable_performance_mode = True
            self._config.enable_ultra_fast_mode = False
        else:
            self._warnings.append(f"Unknown datastore '{datastore}', no optimizations applied")
        
        return self
    
    def for_scenario(self, scenario: str) -> "ConfigurationBuilder":
        """
        Apply scenario-specific configurations.
        
        Args:
            scenario: Scenario name (development, testing, production, exploration)
            
        Returns:
            Self for method chaining
        """
        if scenario == "development":
            self.sampling(0.1).performance_tuning(enable_ultra_fast_mode=True)
        elif scenario == "testing":
            self.sampling(0.01).performance_tuning(enable_ultra_fast_mode=True)
        elif scenario == "production":
            self.performance_tuning(enable_performance_mode=True)
        elif scenario == "exploration":
            self.profile_type("full").performance_tuning(enable_ultra_fast_mode=True)
        else:
            self._warnings.append(f"Unknown scenario '{scenario}', no preset applied")
        
        return self
    
    def validate(self) -> bool:
        """
        Validate the current configuration.
        
        Returns:
            True if configuration is valid
        """
        # Additional cross-field validation
        if self._config.sample_size and self._config.auto_sample_large_tables:
            self._warnings.append(
                "Both explicit sampling and auto-sampling enabled. Explicit sampling takes precedence."
            )
        
        # Check profiler availability for current settings
        if self._config.preferred_profiler:
            try:
                registry = get_registry()
                metadata = registry.get_profiler_metadata(self._config.preferred_profiler)
                
                # Check if required capabilities are supported
                if self._config.required_capabilities:
                    supported_caps = [cap.value for cap in metadata.capabilities]
                    missing_caps = [cap for cap in self._config.required_capabilities if cap not in supported_caps]
                    if missing_caps:
                        self._errors.append(
                            f"Preferred profiler '{self._config.preferred_profiler}' "
                            f"does not support required capabilities: {missing_caps}"
                        )
            except KeyError:
                # Already handled in profiler_selection
                pass
        
        return len(self._errors) == 0
    
    def build(self) -> ProfilingConfiguration:
        """
        Build the final configuration.
        
        Returns:
            Complete ProfilingConfiguration instance
            
        Raises:
            ValueError: If configuration is invalid
        """
        if not self.validate():
            raise ValueError(f"Invalid configuration: {self._errors}")
        
        return self._config
    
    def to_template_vars(self) -> Dict[str, Any]:
        """
        Convert configuration to template variables.
        
        Returns:
            Dictionary suitable for template rendering
        """
        config = self.build()
        
        # Convert configuration to template variables
        template_vars = {
            "profile_type": config.profile_type,
            "save_to_catalog": config.save_to_catalog,
            "generate_report": config.generate_report,
            "output_format": config.output_format,
            "sample_size": config.sample_size,
            "auto_sample_large_tables": config.auto_sample_large_tables,
            "max_correlation_columns": config.max_correlation_columns,
            "top_values_limit": config.top_values_limit,
            "enable_performance_mode": config.enable_performance_mode,
            "enable_ultra_fast_mode": config.enable_ultra_fast_mode,
            "auto_discover_tables": config.auto_discover_tables,
            "profile_frequency": config.profile_frequency,
            "quality_thresholds": config.quality_thresholds,
        }
        
        # Add custom variables
        template_vars.update(config.custom_template_vars)
        
        return template_vars
    
    def get_errors(self) -> List[str]:
        """Get validation errors."""
        return self._errors.copy()
    
    def get_warnings(self) -> List[str]:
        """Get validation warnings."""
        return self._warnings.copy()


def create_config() -> ConfigurationBuilder:
    """
    Create a new configuration builder.
    
    Returns:
        New ConfigurationBuilder instance
    """
    return ConfigurationBuilder()


def create_default_config(target_datastore: str) -> ProfilingConfiguration:
    """
    Create a default configuration for a target datastore.
    
    Args:
        target_datastore: Target datastore type
        
    Returns:
        Default ProfilingConfiguration
    """
    return (
        create_config()
        .for_datastore(target_datastore)
        .build()
    )
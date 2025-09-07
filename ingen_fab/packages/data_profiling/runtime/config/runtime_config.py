"""Main runtime configuration that combines all configuration components."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type
from pathlib import Path

from .base_config import BaseConfig, ConfigurationLoader, ConfigValidator
from .profiler_config import TieredProfilerConfig, ProfilingMode
from .persistence_config import PersistenceConfig, PersistenceBackend
from .quality_config import QualityConfig


class RuntimeConfig(BaseConfig):
    """
    Main runtime configuration that combines all configuration components.
    
    This is the top-level configuration class that orchestrates all other
    configuration components for the data profiling runtime system.
    """
    
    def __init__(self):
        """Initialize runtime configuration with all components."""
        super().__init__()
        
        # Runtime metadata
        self.runtime_version: str = "2.0.0"
        self.config_version: str = "1.0.0"
        self.environment: str = "production"
        self.workspace_name: str = ""
        
        # Component configurations
        self.profiler: TieredProfilerConfig = TieredProfilerConfig()
        self.persistence: PersistenceConfig = PersistenceConfig()
        self.quality: QualityConfig = QualityConfig()
        
        # Global settings
        self.logging_config: Dict[str, Any] = {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "handlers": ["console"],
            "log_to_file": False,
            "log_file_path": None,
            "max_log_file_size_mb": 100,
            "log_retention_days": 30
        }
        
        # Resource limits
        self.resource_limits: Dict[str, Any] = {
            "max_memory_gb": 16.0,
            "max_cpu_cores": 8,
            "max_execution_time_hours": 24,
            "max_tables_per_run": 1000,
            "max_columns_per_table": 1000
        }
        
        # Monitoring and observability
        self.monitoring: Dict[str, Any] = {
            "enable_metrics": True,
            "metrics_endpoint": None,
            "enable_tracing": False,
            "trace_sampling_rate": 0.1,
            "health_check_interval": 300  # seconds
        }
        
        # Security settings
        self.security: Dict[str, Any] = {
            "enable_encryption": True,
            "mask_sensitive_data": True,
            "sensitive_column_patterns": [
                r".*password.*", r".*secret.*", r".*key.*", 
                r".*ssn.*", r".*credit.*card.*", r".*phone.*"
            ],
            "audit_logging": True
        }
        
        # Feature flags
        self.feature_flags: Dict[str, bool] = {
            "enable_experimental_features": False,
            "enable_advanced_analytics": True,
            "enable_ml_predictions": False,
            "enable_real_time_monitoring": False
        }
        
        # Custom settings
        self.custom_settings: Dict[str, Any] = {}
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "runtime_version": "2.0.0",
            "config_version": "1.0.0", 
            "environment": "production",
            "logging_config": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "handlers": ["console"]
            },
            "resource_limits": {
                "max_memory_gb": 16.0,
                "max_cpu_cores": 8,
                "max_execution_time_hours": 24
            },
            "monitoring": {
                "enable_metrics": True,
                "enable_tracing": False
            },
            "security": {
                "enable_encryption": True,
                "mask_sensitive_data": True,
                "audit_logging": True
            }
        }
    
    def validate(self) -> bool:
        """Validate the complete runtime configuration."""
        errors = []
        
        # Validate basic settings
        errors.extend(ConfigValidator.validate_required(self.runtime_version, "runtime_version"))
        errors.extend(ConfigValidator.validate_required(self.config_version, "config_version"))
        errors.extend(ConfigValidator.validate_choices(
            self.environment, ["development", "testing", "staging", "production"], "environment"
        ))
        
        # Validate resource limits
        errors.extend(ConfigValidator.validate_range(
            self.resource_limits["max_memory_gb"], min_val=1.0, field_name="resource_limits.max_memory_gb"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.resource_limits["max_cpu_cores"], min_val=1, field_name="resource_limits.max_cpu_cores"
        ))
        errors.extend(ConfigValidator.validate_range(
            self.resource_limits["max_execution_time_hours"], min_val=1, field_name="resource_limits.max_execution_time_hours"
        ))
        
        # Validate logging config
        errors.extend(ConfigValidator.validate_choices(
            self.logging_config["level"],
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "logging_config.level"
        ))
        
        # Validate monitoring config
        if "trace_sampling_rate" in self.monitoring:
            errors.extend(ConfigValidator.validate_range(
                self.monitoring["trace_sampling_rate"], min_val=0.0, max_val=1.0,
                field_name="monitoring.trace_sampling_rate"
            ))
        
        # Validate component configurations
        if not self.profiler.validate():
            component_errors = self.profiler.metadata.validation_errors
            errors.extend([f"profiler.{error}" for error in component_errors])
        
        if not self.persistence.validate():
            component_errors = self.persistence.metadata.validation_errors
            errors.extend([f"persistence.{error}" for error in component_errors])
        
        if not self.quality.validate():
            component_errors = self.quality.metadata.validation_errors
            errors.extend([f"quality.{error}" for error in component_errors])
        
        # Cross-component validation
        errors.extend(self._validate_cross_component_dependencies())
        
        # Store validation results
        self._metadata.validation_errors = errors
        
        return len(errors) == 0
    
    def _validate_cross_component_dependencies(self) -> List[str]:
        """Validate dependencies between configuration components."""
        errors = []
        
        # Check persistence backend compatibility with profiler settings
        if (self.persistence.backend == PersistenceBackend.MEMORY and 
            len(self.profiler.enabled_scan_levels) > 2):
            errors.append(
                "Memory persistence backend not recommended for more than 2 scan levels. "
                "Consider using lakehouse or database backend."
            )
        
        # Check resource allocation consistency
        if (self.profiler.performance.max_memory_gb > 
            self.resource_limits["max_memory_gb"]):
            errors.append(
                "Profiler max_memory_gb exceeds global resource limit"
            )
        
        # Check quality sample size compatibility with profiler sampling
        if (self.profiler.global_sample_size is not None and
            self.quality.quality_check_sample_size > self.profiler.global_sample_size):
            errors.append(
                "Quality check sample size cannot exceed profiler sample size"
            )
        
        return errors
    
    def configure_for_environment(self, environment: str):
        """Configure runtime for specific environment."""
        self.environment = environment
        
        if environment == "development":
            # Development optimizations
            self.profiler.configure_for_mode(ProfilingMode.DEVELOPMENT)
            self.persistence.configure_for_backend(PersistenceBackend.MEMORY)
            self.quality.configure_for_mode("basic")
            self.logging_config["level"] = "DEBUG"
            self.resource_limits["max_memory_gb"] = 4.0
            self.feature_flags["enable_experimental_features"] = True
            
        elif environment == "testing":
            # Testing optimizations  
            self.profiler.configure_for_mode(ProfilingMode.TESTING)
            self.persistence.configure_for_backend(PersistenceBackend.MEMORY)
            self.quality.configure_for_mode("standard")
            self.logging_config["level"] = "INFO"
            self.resource_limits["max_memory_gb"] = 2.0
            
        elif environment == "staging":
            # Staging - similar to production but with monitoring
            self.profiler.configure_for_mode(ProfilingMode.PRODUCTION)
            self.persistence.configure_for_backend(PersistenceBackend.LAKEHOUSE)
            self.quality.configure_for_mode("comprehensive")
            self.monitoring["enable_tracing"] = True
            self.monitoring["trace_sampling_rate"] = 0.5
            
        elif environment == "production":
            # Full production settings
            self.profiler.configure_for_mode(ProfilingMode.PRODUCTION) 
            self.persistence.configure_for_backend(PersistenceBackend.LAKEHOUSE)
            self.quality.configure_for_mode("comprehensive")
            self.logging_config["level"] = "INFO"
            self.security["audit_logging"] = True
    
    def apply_workspace_settings(self, workspace_id: str, lakehouse_id: str):
        """Apply workspace-specific settings."""
        self.workspace_name = f"{workspace_id}/{lakehouse_id}"
        self.persistence.workspace_id = workspace_id
        self.persistence.lakehouse_id = lakehouse_id
    
    def enable_feature(self, feature_name: str, enabled: bool = True):
        """Enable or disable a feature flag."""
        if feature_name in self.feature_flags:
            self.feature_flags[feature_name] = enabled
        else:
            raise ValueError(f"Unknown feature flag: {feature_name}")
    
    def get_effective_log_level(self) -> str:
        """Get the effective logging level considering environment."""
        if self.environment == "development":
            return "DEBUG"
        elif self.environment in ["testing", "staging"]:
            return "INFO"
        else:
            return self.logging_config.get("level", "INFO")
    
    def get_resource_allocation(self) -> Dict[str, Any]:
        """Get effective resource allocation across all components."""
        return {
            "total_memory_gb": min(
                self.resource_limits["max_memory_gb"],
                self.profiler.performance.max_memory_gb
            ),
            "cpu_cores": min(
                self.resource_limits["max_cpu_cores"],
                self.profiler.performance.max_parallel_tables
            ),
            "max_execution_time": self.resource_limits["max_execution_time_hours"] * 3600
        }
    
    def create_execution_context(self) -> Dict[str, Any]:
        """Create execution context dictionary for runtime use."""
        return {
            "config_version": self.config_version,
            "environment": self.environment,
            "workspace": self.workspace_name,
            "profiler_config": self.profiler.to_dict(),
            "persistence_config": self.persistence.to_dict(),
            "quality_config": self.quality.to_dict(),
            "resource_allocation": self.get_resource_allocation(),
            "feature_flags": self.feature_flags.copy(),
            "security_settings": {
                "encryption_enabled": self.security["enable_encryption"],
                "masking_enabled": self.security["mask_sensitive_data"],
                "audit_enabled": self.security["audit_logging"]
            }
        }
    
    @classmethod
    def create_for_environment(
        cls,
        environment: str,
        workspace_id: Optional[str] = None,
        lakehouse_id: Optional[str] = None,
        config_overrides: Optional[Dict[str, Any]] = None
    ) -> 'RuntimeConfig':
        """
        Create a runtime configuration for a specific environment.
        
        Args:
            environment: Target environment
            workspace_id: Optional workspace ID
            lakehouse_id: Optional lakehouse ID  
            config_overrides: Optional configuration overrides
            
        Returns:
            Configured RuntimeConfig instance
        """
        config = cls()
        config.configure_for_environment(environment)
        
        if workspace_id and lakehouse_id:
            config.apply_workspace_settings(workspace_id, lakehouse_id)
        
        if config_overrides:
            # Apply overrides
            for key, value in config_overrides.items():
                if hasattr(config, key):
                    setattr(config, key, value)
        
        return config
    
    @classmethod
    def load_from_files(
        cls,
        config_dir: Path,
        environment: Optional[str] = None
    ) -> 'RuntimeConfig':
        """
        Load configuration from multiple files in a directory.
        
        Args:
            config_dir: Directory containing configuration files
            environment: Optional environment override
            
        Returns:
            Loaded RuntimeConfig instance
        """
        config = cls()
        
        # Load component configurations if they exist
        profiler_config_file = config_dir / "profiler.yaml"
        if profiler_config_file.exists():
            config.profiler = TieredProfilerConfig.from_file(profiler_config_file)
        
        persistence_config_file = config_dir / "persistence.yaml"
        if persistence_config_file.exists():
            config.persistence = PersistenceConfig.from_file(persistence_config_file)
        
        quality_config_file = config_dir / "quality.yaml"
        if quality_config_file.exists():
            config.quality = QualityConfig.from_file(quality_config_file)
        
        # Load main runtime config
        runtime_config_file = config_dir / "runtime.yaml"
        if runtime_config_file.exists():
            runtime_data = ConfigurationLoader.load_config(
                dict, file_path=runtime_config_file, validate=False
            )
            
            # Apply runtime-specific settings
            for key, value in runtime_data.items():
                if key not in ["profiler", "persistence", "quality"] and hasattr(config, key):
                    setattr(config, key, value)
        
        # Override environment if specified
        if environment:
            config.configure_for_environment(environment)
        
        return config
    
    def save_to_directory(self, config_dir: Path):
        """
        Save configuration to multiple files in a directory.
        
        Args:
            config_dir: Directory to save configuration files
        """
        config_dir.mkdir(parents=True, exist_ok=True)
        
        # Save component configurations
        self.profiler.save(config_dir / "profiler.yaml")
        self.persistence.save(config_dir / "persistence.yaml") 
        self.quality.save(config_dir / "quality.yaml")
        
        # Save main runtime config (excluding components)
        runtime_data = {
            "runtime_version": self.runtime_version,
            "config_version": self.config_version,
            "environment": self.environment,
            "workspace_name": self.workspace_name,
            "logging_config": self.logging_config,
            "resource_limits": self.resource_limits,
            "monitoring": self.monitoring,
            "security": self.security,
            "feature_flags": self.feature_flags,
            "custom_settings": self.custom_settings
        }
        
        runtime_config = type(self)()
        for key, value in runtime_data.items():
            setattr(runtime_config, key, value)
        
        runtime_config.save(config_dir / "runtime.yaml")
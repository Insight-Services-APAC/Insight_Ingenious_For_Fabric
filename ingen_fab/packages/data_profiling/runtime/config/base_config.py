"""Base configuration classes for the data profiling runtime system."""

import os
import json
import yaml
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Type, TypeVar
from enum import Enum

# Type variable for configuration classes
T = TypeVar('T', bound='BaseConfig')


class ConfigSource(Enum):
    """Sources for configuration loading."""
    FILE = "file"
    ENVIRONMENT = "environment"
    DEFAULTS = "defaults"
    OVERRIDE = "override"


class ConfigFormat(Enum):
    """Supported configuration file formats."""
    YAML = "yaml"
    JSON = "json"
    TOML = "toml"


@dataclass
class ConfigMetadata:
    """Metadata about configuration source and validation."""
    source: ConfigSource
    file_path: Optional[str] = None
    loaded_at: Optional[str] = None
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)


class BaseConfig(ABC):
    """
    Abstract base class for all runtime configurations.
    
    Provides common functionality for configuration loading, validation,
    and serialization across all configuration types.
    """
    
    def __init__(self):
        """Initialize base configuration."""
        self._metadata = ConfigMetadata(source=ConfigSource.DEFAULTS)
    
    @property
    def metadata(self) -> ConfigMetadata:
        """Get configuration metadata."""
        return self._metadata
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the configuration.
        
        Returns:
            True if configuration is valid
        """
        pass
    
    @abstractmethod
    def get_defaults(self) -> Dict[str, Any]:
        """
        Get default configuration values.
        
        Returns:
            Dictionary of default values
        """
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)
    
    def to_json(self, indent: Optional[int] = 2) -> str:
        """Convert configuration to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)
    
    def to_yaml(self) -> str:
        """Convert configuration to YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False)
    
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Create configuration from dictionary.
        
        Args:
            data: Configuration data
            
        Returns:
            Configuration instance
        """
        instance = cls()
        
        # Update instance with data
        for key, value in data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        
        return instance
    
    @classmethod
    def from_file(cls: Type[T], file_path: Union[str, Path]) -> T:
        """
        Load configuration from file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Configuration instance
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is unsupported or invalid
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
        
        # Determine format from extension
        suffix = file_path.suffix.lower()
        if suffix in ['.yaml', '.yml']:
            format_type = ConfigFormat.YAML
        elif suffix == '.json':
            format_type = ConfigFormat.JSON
        else:
            raise ValueError(f"Unsupported configuration file format: {suffix}")
        
        # Load data
        with open(file_path, 'r') as f:
            if format_type == ConfigFormat.YAML:
                data = yaml.safe_load(f)
            elif format_type == ConfigFormat.JSON:
                data = json.load(f)
        
        # Create instance
        instance = cls.from_dict(data)
        instance._metadata.source = ConfigSource.FILE
        instance._metadata.file_path = str(file_path)
        
        return instance
    
    @classmethod
    def from_env(cls: Type[T], prefix: str = "") -> T:
        """
        Load configuration from environment variables.
        
        Args:
            prefix: Prefix for environment variables
            
        Returns:
            Configuration instance
        """
        instance = cls()
        env_data = {}
        
        # Get all environment variables with the prefix
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and convert to lowercase
                config_key = key[len(prefix):].lower()
                
                # Try to convert to appropriate type
                env_data[config_key] = cls._parse_env_value(value)
        
        # Update instance
        for key, value in env_data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        
        instance._metadata.source = ConfigSource.ENVIRONMENT
        return instance
    
    @staticmethod
    def _parse_env_value(value: str) -> Any:
        """Parse environment variable value to appropriate type."""
        # Try boolean
        if value.lower() in ['true', 'false']:
            return value.lower() == 'true'
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Try JSON
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            pass
        
        # Return as string
        return value
    
    def merge(self, other: 'BaseConfig') -> 'BaseConfig':
        """
        Merge this configuration with another configuration.
        
        Args:
            other: Configuration to merge with
            
        Returns:
            New merged configuration instance
        """
        # Start with this configuration
        merged_data = self.to_dict()
        
        # Update with other configuration
        other_data = other.to_dict()
        merged_data.update(other_data)
        
        # Create new instance
        merged_instance = self.__class__.from_dict(merged_data)
        merged_instance._metadata.source = ConfigSource.OVERRIDE
        
        return merged_instance
    
    def save(self, file_path: Union[str, Path], format_type: Optional[ConfigFormat] = None):
        """
        Save configuration to file.
        
        Args:
            file_path: Path to save file
            format_type: Format to save in (auto-detected if None)
        """
        file_path = Path(file_path)
        
        # Auto-detect format from extension if not provided
        if format_type is None:
            suffix = file_path.suffix.lower()
            if suffix in ['.yaml', '.yml']:
                format_type = ConfigFormat.YAML
            elif suffix == '.json':
                format_type = ConfigFormat.JSON
            else:
                format_type = ConfigFormat.YAML  # Default
        
        # Create parent directories
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save file
        with open(file_path, 'w') as f:
            if format_type == ConfigFormat.YAML:
                f.write(self.to_yaml())
            elif format_type == ConfigFormat.JSON:
                f.write(self.to_json())


class ConfigurationLoader:
    """Factory class for loading configurations from various sources."""
    
    @staticmethod
    def load_config(
        config_class: Type[T],
        file_path: Optional[Union[str, Path]] = None,
        env_prefix: Optional[str] = None,
        defaults: Optional[Dict[str, Any]] = None,
        validate: bool = True
    ) -> T:
        """
        Load configuration with cascading priority:
        1. File (if provided)
        2. Environment variables (if prefix provided)  
        3. Defaults (if provided)
        4. Class defaults
        
        Args:
            config_class: Configuration class to instantiate
            file_path: Optional path to configuration file
            env_prefix: Optional environment variable prefix
            defaults: Optional default values
            validate: Whether to validate the final configuration
            
        Returns:
            Loaded and validated configuration
            
        Raises:
            ValueError: If configuration is invalid and validate=True
        """
        # Start with class defaults
        config = config_class()
        
        # Apply provided defaults
        if defaults:
            default_config = config_class.from_dict(defaults)
            config = config.merge(default_config)
        
        # Apply environment variables
        if env_prefix:
            env_config = config_class.from_env(env_prefix)
            config = config.merge(env_config)
        
        # Apply file configuration (highest priority)
        if file_path:
            file_config = config_class.from_file(file_path)
            config = config.merge(file_config)
        
        # Validate if requested
        if validate and not config.validate():
            errors = config.metadata.validation_errors
            raise ValueError(f"Configuration validation failed: {errors}")
        
        return config
    
    @staticmethod
    def discover_config_file(
        base_name: str,
        search_paths: Optional[List[Union[str, Path]]] = None,
        extensions: Optional[List[str]] = None
    ) -> Optional[Path]:
        """
        Discover configuration file by searching in common locations.
        
        Args:
            base_name: Base name of configuration file (without extension)
            search_paths: Optional list of paths to search
            extensions: Optional list of extensions to try
            
        Returns:
            Path to found configuration file, or None if not found
        """
        if search_paths is None:
            search_paths = [
                Path.cwd(),
                Path.cwd() / "config",
                Path.home() / ".config",
                Path("/etc")
            ]
        
        if extensions is None:
            extensions = [".yaml", ".yml", ".json"]
        
        for search_path in search_paths:
            search_path = Path(search_path)
            if not search_path.exists():
                continue
                
            for extension in extensions:
                config_file = search_path / f"{base_name}{extension}"
                if config_file.exists():
                    return config_file
        
        return None


class ConfigValidator:
    """Utility class for configuration validation."""
    
    @staticmethod
    def validate_range(
        value: Union[int, float], 
        min_val: Optional[Union[int, float]] = None,
        max_val: Optional[Union[int, float]] = None,
        field_name: str = "value"
    ) -> List[str]:
        """Validate that a value is within a specified range."""
        errors = []
        
        if min_val is not None and value < min_val:
            errors.append(f"{field_name} must be >= {min_val}, got {value}")
        
        if max_val is not None and value > max_val:
            errors.append(f"{field_name} must be <= {max_val}, got {value}")
        
        return errors
    
    @staticmethod
    def validate_choices(
        value: Any,
        choices: List[Any],
        field_name: str = "value"
    ) -> List[str]:
        """Validate that a value is one of the allowed choices."""
        errors = []
        
        if value not in choices:
            errors.append(f"{field_name} must be one of {choices}, got {value}")
        
        return errors
    
    @staticmethod
    def validate_type(
        value: Any,
        expected_type: Type,
        field_name: str = "value"
    ) -> List[str]:
        """Validate that a value is of the expected type."""
        errors = []
        
        if not isinstance(value, expected_type):
            errors.append(f"{field_name} must be of type {expected_type.__name__}, got {type(value).__name__}")
        
        return errors
    
    @staticmethod
    def validate_required(
        value: Any,
        field_name: str = "value"
    ) -> List[str]:
        """Validate that a required value is not None or empty."""
        errors = []
        
        if value is None:
            errors.append(f"{field_name} is required")
        elif isinstance(value, (str, list, dict)) and len(value) == 0:
            errors.append(f"{field_name} cannot be empty")
        
        return errors
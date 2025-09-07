"""Factory for creating persistence instances."""

from typing import Any, Dict, Optional
from pyspark.sql import SparkSession

from ..core.interfaces.persistence_interface import PersistenceInterface
from .lakehouse_persistence import LakehousePersistence
from .enhanced_lakehouse_persistence import EnhancedLakehousePersistence
from .memory_persistence import MemoryPersistence


class PersistenceFactory:
    """Factory for creating persistence implementations."""
    
    @staticmethod
    def create_persistence(
        persistence_type: str,
        table_prefix: str = "tiered_profile",
        **kwargs
    ) -> PersistenceInterface:
        """
        Create a persistence instance based on type.
        
        Args:
            persistence_type: Type of persistence ("lakehouse", "memory", "warehouse", "file")
            table_prefix: Prefix for profile result tables
            **kwargs: Additional arguments for specific persistence types
            
        Returns:
            PersistenceInterface implementation
            
        Raises:
            ValueError: If persistence_type is not supported
            TypeError: If required arguments are missing for the persistence type
        """
        persistence_type = persistence_type.lower()
        
        if persistence_type == "lakehouse":
            return PersistenceFactory._create_lakehouse_persistence(
                table_prefix=table_prefix, **kwargs
            )
        elif persistence_type == "enhanced_lakehouse":
            return PersistenceFactory._create_enhanced_lakehouse_persistence(
                table_prefix=table_prefix, **kwargs
            )
        elif persistence_type == "memory":
            return PersistenceFactory._create_memory_persistence(
                table_prefix=table_prefix, **kwargs
            )
        elif persistence_type == "warehouse":
            # Future implementation
            raise NotImplementedError("Warehouse persistence not yet implemented")
        elif persistence_type == "file":
            # Future implementation
            raise NotImplementedError("File persistence not yet implemented")
        else:
            raise ValueError(
                f"Unsupported persistence type: {persistence_type}. "
                f"Supported types: lakehouse, memory, warehouse, file"
            )
    
    @staticmethod
    def _create_lakehouse_persistence(table_prefix: str, **kwargs) -> LakehousePersistence:
        """Create lakehouse persistence instance."""
        # Required arguments for lakehouse persistence
        required_args = ["lakehouse", "spark"]
        missing_args = [arg for arg in required_args if arg not in kwargs]
        
        if missing_args:
            raise TypeError(
                f"LakehousePersistence requires the following arguments: {missing_args}"
            )
        
        lakehouse = kwargs["lakehouse"]
        spark = kwargs["spark"]
        
        # Validate types
        if not hasattr(lakehouse, "check_if_table_exists"):
            raise TypeError("lakehouse must be a lakehouse_utils instance")
        
        if not isinstance(spark, SparkSession):
            raise TypeError("spark must be a SparkSession instance")
        
        return LakehousePersistence(
            lakehouse=lakehouse,
            spark=spark,
            table_prefix=table_prefix
        )
    
    @staticmethod
    def _create_enhanced_lakehouse_persistence(table_prefix: str, **kwargs) -> EnhancedLakehousePersistence:
        """Create enhanced lakehouse persistence instance."""
        # Required arguments for enhanced lakehouse persistence
        required_args = ["lakehouse", "spark"]
        missing_args = [arg for arg in required_args if arg not in kwargs]
        
        if missing_args:
            raise TypeError(
                f"EnhancedLakehousePersistence requires the following arguments: {missing_args}"
            )
        
        lakehouse = kwargs["lakehouse"]
        spark = kwargs["spark"]
        profile_grain = kwargs.get("profile_grain", "daily")  # Default to daily
        
        # Validate types
        if not hasattr(lakehouse, "check_if_table_exists"):
            raise TypeError("lakehouse must be a lakehouse_utils instance")
        
        if not isinstance(spark, SparkSession):
            raise TypeError("spark must be a SparkSession instance")
        
        # Import ProfileGrain enum
        from ..core.enums.profile_types import ProfileGrain
        
        # Convert string to enum if needed
        if isinstance(profile_grain, str):
            profile_grain = ProfileGrain(profile_grain.lower())
        
        return EnhancedLakehousePersistence(
            lakehouse=lakehouse,
            spark=spark,
            table_prefix=table_prefix,
            profile_grain=profile_grain
        )
    
    @staticmethod
    def _create_memory_persistence(table_prefix: str, **kwargs) -> MemoryPersistence:
        """Create memory persistence instance."""
        return MemoryPersistence(table_prefix=table_prefix)
    
    @staticmethod
    def get_supported_types() -> list[str]:
        """Get list of supported persistence types."""
        return ["lakehouse", "memory", "warehouse", "file"]
    
    @staticmethod
    def create_persistence_from_config(
        config: Dict[str, Any],
        **additional_kwargs
    ) -> PersistenceInterface:
        """
        Create persistence from configuration dictionary.
        
        Args:
            config: Configuration dictionary with 'type' and other settings
            **additional_kwargs: Additional arguments that override config
            
        Returns:
            PersistenceInterface implementation
            
        Example config:
            {
                "type": "lakehouse",
                "table_prefix": "my_profile",
                "lakehouse": lakehouse_instance,
                "spark": spark_session
            }
        """
        if "type" not in config:
            raise ValueError("Configuration must contain 'type' field")
        
        # Merge config with additional kwargs (kwargs take precedence)
        merged_kwargs = {**config, **additional_kwargs}
        persistence_type = merged_kwargs.pop("type")
        
        return PersistenceFactory.create_persistence(
            persistence_type=persistence_type,
            **merged_kwargs
        )


class PersistenceRegistry:
    """Registry for managing persistence instances."""
    
    def __init__(self):
        self._instances: Dict[str, PersistenceInterface] = {}
    
    def register(self, name: str, persistence: PersistenceInterface) -> None:
        """Register a persistence instance with a name."""
        self._instances[name] = persistence
    
    def get(self, name: str) -> Optional[PersistenceInterface]:
        """Get a registered persistence instance by name."""
        return self._instances.get(name)
    
    def remove(self, name: str) -> bool:
        """Remove a registered persistence instance."""
        if name in self._instances:
            del self._instances[name]
            return True
        return False
    
    def list_names(self) -> list[str]:
        """List all registered persistence instance names."""
        return list(self._instances.keys())
    
    def clear(self) -> None:
        """Clear all registered instances."""
        self._instances.clear()


# Global registry instance for convenience
persistence_registry = PersistenceRegistry()
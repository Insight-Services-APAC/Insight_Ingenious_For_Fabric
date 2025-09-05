"""
Profiler Registry System

This module provides a registry pattern for managing different profiler implementations,
allowing runtime registration and discovery of profiling strategies.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Type, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from ingen_fab.python_libs.interfaces.data_profiling_interface import (
    DataProfilingInterface,
    ProfileType,
    DatasetProfile,
)


class ProfilerCapability(Enum):
    """Capabilities that a profiler can support."""
    BASIC_STATS = "basic_stats"
    STATISTICAL_ANALYSIS = "statistical_analysis"
    DATA_QUALITY = "data_quality"
    RELATIONSHIP_DISCOVERY = "relationship_discovery"
    PERFORMANCE_OPTIMIZED = "performance_optimized"
    STREAMING = "streaming"
    INCREMENTAL = "incremental"
    DISTRIBUTED = "distributed"


@dataclass
class ProfilerMetadata:
    """Metadata about a registered profiler."""
    name: str
    description: str
    version: str
    capabilities: List[ProfilerCapability]
    supported_datastores: List[str]  # e.g., ["lakehouse", "warehouse", "dataframe"]
    performance_characteristics: Dict[str, Any]  # e.g., {"max_rows": 1000000, "memory_efficient": True}
    priority: int = 100  # Higher priority profilers are preferred


class ProfilerFactory(ABC):
    """Abstract factory for creating profiler instances."""
    
    @abstractmethod
    def create_profiler(self, **kwargs) -> DataProfilingInterface:
        """
        Create a profiler instance with the given configuration.
        
        Args:
            **kwargs: Configuration parameters for the profiler
            
        Returns:
            Configured profiler instance
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> ProfilerMetadata:
        """
        Get metadata about this profiler.
        
        Returns:
            ProfilerMetadata describing the profiler
        """
        pass
    
    @abstractmethod
    def validate_config(self, **kwargs) -> bool:
        """
        Validate configuration parameters for this profiler.
        
        Args:
            **kwargs: Configuration parameters to validate
            
        Returns:
            True if configuration is valid
        """
        pass


class ProfilerRegistry:
    """
    Registry for managing and discovering profiler implementations.
    
    This class implements a singleton pattern to maintain a global registry
    of available profilers that can be dynamically registered and discovered.
    """
    
    _instance = None
    _profilers: Dict[str, ProfilerFactory] = {}
    
    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super(ProfilerRegistry, cls).__new__(cls)
            cls._instance._profilers = {}
            cls._instance._initialize_default_profilers()
        return cls._instance
    
    def _initialize_default_profilers(self):
        """Initialize registry with default profiler implementations."""
        # Register built-in profilers
        try:
            from ingen_fab.python_libs.pyspark.profiler_factories import (
                StandardProfilerFactory,
                UltraFastProfilerFactory,
                OptimizedProfilerFactory,
            )
            
            self.register_profiler("standard", StandardProfilerFactory())
            self.register_profiler("ultra_fast", UltraFastProfilerFactory())
            self.register_profiler("optimized", OptimizedProfilerFactory())
        except ImportError:
            # Profiler factories not yet implemented
            pass
    
    def register_profiler(self, name: str, factory: ProfilerFactory) -> None:
        """
        Register a new profiler factory.
        
        Args:
            name: Unique name for the profiler
            factory: Factory instance for creating the profiler
            
        Raises:
            ValueError: If a profiler with the same name already exists
        """
        if name in self._profilers:
            raise ValueError(f"Profiler '{name}' is already registered")
        self._profilers[name] = factory
    
    def unregister_profiler(self, name: str) -> None:
        """
        Remove a profiler from the registry.
        
        Args:
            name: Name of the profiler to remove
            
        Raises:
            KeyError: If the profiler doesn't exist
        """
        if name not in self._profilers:
            raise KeyError(f"Profiler '{name}' not found in registry")
        del self._profilers[name]
    
    def get_profiler(self, name: str, **kwargs) -> DataProfilingInterface:
        """
        Get a configured profiler instance by name.
        
        Args:
            name: Name of the profiler to create
            **kwargs: Configuration parameters for the profiler
            
        Returns:
            Configured profiler instance
            
        Raises:
            KeyError: If the profiler doesn't exist
            ValueError: If the configuration is invalid
        """
        if name not in self._profilers:
            raise KeyError(f"Profiler '{name}' not found in registry")
        
        factory = self._profilers[name]
        
        # Validate configuration
        if not factory.validate_config(**kwargs):
            raise ValueError(f"Invalid configuration for profiler '{name}'")
        
        return factory.create_profiler(**kwargs)
    
    def list_profilers(self) -> List[str]:
        """
        List all registered profiler names.
        
        Returns:
            List of profiler names
        """
        return list(self._profilers.keys())
    
    def get_profiler_metadata(self, name: str) -> ProfilerMetadata:
        """
        Get metadata for a specific profiler.
        
        Args:
            name: Name of the profiler
            
        Returns:
            ProfilerMetadata for the profiler
            
        Raises:
            KeyError: If the profiler doesn't exist
        """
        if name not in self._profilers:
            raise KeyError(f"Profiler '{name}' not found in registry")
        return self._profilers[name].get_metadata()
    
    def find_profilers_by_capability(
        self, capability: ProfilerCapability
    ) -> List[str]:
        """
        Find all profilers that support a specific capability.
        
        Args:
            capability: Capability to search for
            
        Returns:
            List of profiler names that support the capability
        """
        matching_profilers = []
        for name, factory in self._profilers.items():
            metadata = factory.get_metadata()
            if capability in metadata.capabilities:
                matching_profilers.append(name)
        return matching_profilers
    
    def find_profilers_for_datastore(self, datastore: str) -> List[str]:
        """
        Find all profilers that support a specific datastore.
        
        Args:
            datastore: Datastore type (e.g., "lakehouse", "warehouse")
            
        Returns:
            List of profiler names that support the datastore
        """
        matching_profilers = []
        for name, factory in self._profilers.items():
            metadata = factory.get_metadata()
            if datastore in metadata.supported_datastores:
                matching_profilers.append(name)
        return matching_profilers
    
    def get_best_profiler_for_scenario(
        self,
        datastore: str,
        row_count: Optional[int] = None,
        required_capabilities: Optional[List[ProfilerCapability]] = None,
        prefer_performance: bool = False,
    ) -> str:
        """
        Get the best profiler for a given scenario.
        
        Args:
            datastore: Target datastore type
            row_count: Estimated number of rows to profile
            required_capabilities: List of required capabilities
            prefer_performance: Whether to prioritize performance
            
        Returns:
            Name of the best matching profiler
            
        Raises:
            ValueError: If no suitable profiler is found
        """
        candidates = []
        
        for name, factory in self._profilers.items():
            metadata = factory.get_metadata()
            
            # Check datastore compatibility
            if datastore not in metadata.supported_datastores:
                continue
            
            # Check required capabilities
            if required_capabilities:
                if not all(cap in metadata.capabilities for cap in required_capabilities):
                    continue
            
            # Check performance characteristics
            if row_count and "max_rows" in metadata.performance_characteristics:
                if row_count > metadata.performance_characteristics["max_rows"]:
                    continue
            
            candidates.append((name, metadata))
        
        if not candidates:
            raise ValueError(f"No suitable profiler found for scenario")
        
        # Sort by priority and performance preference
        if prefer_performance:
            # Prioritize performance-optimized profilers
            candidates.sort(
                key=lambda x: (
                    ProfilerCapability.PERFORMANCE_OPTIMIZED not in x[1].capabilities,
                    -x[1].priority,
                )
            )
        else:
            # Sort by priority only
            candidates.sort(key=lambda x: -x[1].priority)
        
        return candidates[0][0]
    
    def clear(self) -> None:
        """Clear all registered profilers (useful for testing)."""
        self._profilers.clear()
    
    def reset(self) -> None:
        """Reset registry to default state with built-in profilers."""
        self.clear()
        self._initialize_default_profilers()


# Global registry instance
_registry = ProfilerRegistry()


def get_registry() -> ProfilerRegistry:
    """
    Get the global profiler registry instance.
    
    Returns:
        Global ProfilerRegistry instance
    """
    return _registry


def register_profiler(name: str, factory: ProfilerFactory) -> None:
    """
    Convenience function to register a profiler with the global registry.
    
    Args:
        name: Unique name for the profiler
        factory: Factory instance for creating the profiler
    """
    get_registry().register_profiler(name, factory)


def get_profiler(name: str, **kwargs) -> DataProfilingInterface:
    """
    Convenience function to get a profiler from the global registry.
    
    Args:
        name: Name of the profiler to create
        **kwargs: Configuration parameters for the profiler
        
    Returns:
        Configured profiler instance
    """
    return get_registry().get_profiler(name, **kwargs)
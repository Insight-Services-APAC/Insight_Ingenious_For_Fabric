"""Persistence layer for data profiling runtime.

This module provides various persistence implementations for storing profiling results,
metadata, and progress tracking.

Available implementations:
- LakehousePersistence: Delta table-based persistence using lakehouse_utils
- MemoryPersistence: In-memory persistence for testing and development

Usage:
    from .factory import PersistenceFactory
    
    # Create lakehouse persistence
    persistence = PersistenceFactory.create_persistence(
        "lakehouse",
        lakehouse=lakehouse_instance,
        spark=spark_session,
        table_prefix="my_profile"
    )
    
    # Create memory persistence for testing
    test_persistence = PersistenceFactory.create_persistence("memory")
"""

from .base_persistence import BasePersistence
from .lakehouse_persistence import LakehousePersistence
from .memory_persistence import MemoryPersistence
from .factory import PersistenceFactory, PersistenceRegistry, persistence_registry

__all__ = [
    # Base classes
    "BasePersistence",
    
    # Concrete implementations
    "LakehousePersistence", 
    "MemoryPersistence",
    
    # Factory and registry
    "PersistenceFactory",
    "PersistenceRegistry", 
    "persistence_registry",
]
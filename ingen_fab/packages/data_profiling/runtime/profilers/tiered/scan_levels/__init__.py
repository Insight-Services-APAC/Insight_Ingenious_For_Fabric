"""Modular scan levels for tiered profiling.

This module provides a modular architecture for the tiered profiling system,
where each scan level is implemented as a separate class that can be composed
and orchestrated by the TieredProfiler.

Scan Levels:
- Level 1 (Discovery): Fast discovery of Delta tables and basic metadata
- Level 2 (Schema): Column metadata extraction and key identification  
- Level 3 (Profile): Statistical profiling and data quality metrics (TODO)
- Level 4 (Advanced): Advanced analysis and relationship discovery (TODO)

Usage:
    from .scan_coordinator import ScanCoordinator
    
    # Create coordinator
    coordinator = ScanCoordinator(lakehouse, spark, persistence)
    
    # Execute individual scan levels
    results = coordinator.execute_scan_level(ScanLevel.LEVEL_1_DISCOVERY)
    
    # Execute sequence of scan levels
    results = coordinator.execute_scan_sequence([
        ScanLevel.LEVEL_1_DISCOVERY,
        ScanLevel.LEVEL_2_SCHEMA
    ])
"""

from .base_scan_level import BaseScanLevel
from .level_1_discovery import Level1DiscoveryScanner
from .level_2_schema import Level2SchemaScanner
from .scan_coordinator import ScanCoordinator

__all__ = [
    # Base classes
    "BaseScanLevel",
    
    # Scan level implementations
    "Level1DiscoveryScanner",
    "Level2SchemaScanner",
    
    # Coordinator
    "ScanCoordinator",
]
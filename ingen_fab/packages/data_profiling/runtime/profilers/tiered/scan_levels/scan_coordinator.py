"""Scan coordinator for orchestrating tiered profiling scan levels."""

import logging
from typing import Any, Dict, List, Optional, Type

from pyspark.sql import SparkSession

from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

from ....core.enums.profile_types import ScanLevel
from ....core.interfaces.persistence_interface import PersistenceInterface
from .base_scan_level import BaseScanLevel
from .level_1_discovery import Level1DiscoveryScanner
from .level_2_schema import Level2SchemaScanner
from .level_3_profile import Level3ProfileScanner
from .level_4_advanced import Level4AdvancedScanner


class ScanCoordinator:
    """
    Coordinates the execution of different scan levels in the tiered profiling system.
    
    This class manages the instantiation and orchestration of scan level implementations,
    providing a clean interface for the TieredProfiler to execute scans.
    """
    
    def __init__(
        self,
        lakehouse: lakehouse_utils,
        spark: SparkSession,
        persistence: PersistenceInterface,
        exclude_views: bool = True
    ):
        """
        Initialize the scan coordinator.
        
        Args:
            lakehouse: lakehouse_utils instance for data access
            spark: SparkSession for DataFrame operations
            persistence: Persistence interface for storing results
            exclude_views: Whether to exclude views from scanning
        """
        self.lakehouse = lakehouse
        self.spark = spark
        self.persistence = persistence
        self.exclude_views = exclude_views
        self.logger = logging.getLogger(__name__)
        
        # Registry of available scan level implementations
        self._scan_level_registry: Dict[ScanLevel, Type[BaseScanLevel]] = {
            ScanLevel.LEVEL_1_DISCOVERY: Level1DiscoveryScanner,
            ScanLevel.LEVEL_2_SCHEMA: Level2SchemaScanner,
            ScanLevel.LEVEL_3_PROFILE: Level3ProfileScanner,
            ScanLevel.LEVEL_4_ADVANCED: Level4AdvancedScanner,
        }
        
        # Cache of instantiated scan level objects
        self._scan_instances: Dict[ScanLevel, BaseScanLevel] = {}
    
    def get_scanner(self, scan_level: ScanLevel) -> BaseScanLevel:
        """
        Get a scanner instance for the specified scan level.
        
        Args:
            scan_level: The scan level to get scanner for
            
        Returns:
            BaseScanLevel implementation for the scan level
            
        Raises:
            NotImplementedError: If scan level is not implemented
        """
        if scan_level not in self._scan_level_registry:
            raise NotImplementedError(f"Scan level {scan_level} is not implemented")
        
        # Return cached instance if available
        if scan_level in self._scan_instances:
            return self._scan_instances[scan_level]
        
        # Create new instance
        scanner_class = self._scan_level_registry[scan_level]
        scanner = scanner_class(
            lakehouse=self.lakehouse,
            spark=self.spark,
            persistence=self.persistence,
            exclude_views=self.exclude_views
        )
        
        # Cache the instance
        self._scan_instances[scan_level] = scanner
        return scanner
    
    def execute_scan_level(
        self,
        scan_level: ScanLevel,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        **kwargs
    ) -> List[Any]:
        """
        Execute a specific scan level.
        
        Args:
            scan_level: The scan level to execute
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables
            **kwargs: Additional scan-level specific parameters
            
        Returns:
            List of scan results (type varies by scan level)
        """
        scanner = self.get_scanner(scan_level)
        return scanner.execute(
            table_names=table_names,
            resume=resume,
            **kwargs
        )
    
    def execute_scan_sequence(
        self,
        scan_levels: List[ScanLevel],
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        **kwargs
    ) -> Dict[ScanLevel, List[Any]]:
        """
        Execute a sequence of scan levels in order.
        
        Args:
            scan_levels: List of scan levels to execute in order
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables
            **kwargs: Additional scan-level specific parameters
            
        Returns:
            Dictionary mapping scan levels to their results
        """
        results = {}
        
        for scan_level in scan_levels:
            try:
                self.logger.info(f"Executing {scan_level.value}")
                scan_results = self.execute_scan_level(
                    scan_level,
                    table_names=table_names,
                    resume=resume,
                    **kwargs
                )
                results[scan_level] = scan_results
                
            except Exception as e:
                self.logger.error(f"Failed to execute {scan_level.value}: {e}")
                # Store empty results to indicate failure
                results[scan_level] = []
                # Optionally, you could break here or continue with other levels
                
        return results
    
    def get_available_scan_levels(self) -> List[ScanLevel]:
        """Get list of available (implemented) scan levels."""
        return list(self._scan_level_registry.keys())
    
    def get_scan_statistics(self) -> Dict[ScanLevel, Dict[str, Any]]:
        """Get statistics for all available scan levels."""
        stats = {}
        
        for scan_level in self.get_available_scan_levels():
            try:
                scanner = self.get_scanner(scan_level)
                stats[scan_level] = scanner.get_scan_statistics()
            except Exception as e:
                self.logger.warning(f"Could not get statistics for {scan_level}: {e}")
                stats[scan_level] = {"error": str(e)}
        
        return stats
    
    def register_scan_level(
        self,
        scan_level: ScanLevel,
        scanner_class: Type[BaseScanLevel]
    ) -> None:
        """
        Register a new scan level implementation.
        
        Args:
            scan_level: The scan level enum
            scanner_class: Class implementing BaseScanLevel
        """
        if not issubclass(scanner_class, BaseScanLevel):
            raise ValueError("scanner_class must inherit from BaseScanLevel")
        
        self._scan_level_registry[scan_level] = scanner_class
        # Clear cached instance if it exists
        if scan_level in self._scan_instances:
            del self._scan_instances[scan_level]
        
        self.logger.info(f"Registered scan level {scan_level.value}")
    
    def is_scan_level_available(self, scan_level: ScanLevel) -> bool:
        """Check if a scan level is available (implemented)."""
        return scan_level in self._scan_level_registry
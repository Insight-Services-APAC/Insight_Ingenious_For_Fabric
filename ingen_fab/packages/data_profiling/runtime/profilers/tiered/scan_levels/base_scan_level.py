"""Base class for all scan levels in the tiered profiling system."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
import logging

from pyspark.sql import SparkSession
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

from ....core.interfaces.persistence_interface import PersistenceInterface
from ....core.enums.profile_types import ScanLevel
from ....core.models.metadata import ScanProgress


class BaseScanLevel(ABC):
    """Abstract base class for all scan levels."""
    
    def __init__(
        self,
        lakehouse: lakehouse_utils,
        spark: SparkSession,
        persistence: PersistenceInterface,
        exclude_views: bool = True
    ):
        """
        Initialize base scan level.
        
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
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @property
    @abstractmethod
    def scan_level(self) -> ScanLevel:
        """Return the ScanLevel enum for this scan level."""
        pass
    
    @property
    @abstractmethod
    def scan_name(self) -> str:
        """Return human-readable name for this scan level."""
        pass
    
    @property
    @abstractmethod
    def scan_description(self) -> str:
        """Return description of what this scan level does."""
        pass
    
    @abstractmethod
    def execute(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        **kwargs
    ) -> List[Any]:
        """
        Execute the scan level.
        
        Args:
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables
            **kwargs: Additional scan-level specific parameters
            
        Returns:
            List of scan results (type varies by scan level)
        """
        pass
    
    def _print_scan_header(self) -> None:
        """Print formatted header for the scan level."""
        print("\n" + "="*60)
        print(f"🔍 {self.scan_name.upper()}")
        print(f"   {self.scan_description}")
        print("="*60)
    
    def _update_progress(
        self,
        table_name: str,
        start_time: datetime,
        success: bool = True,
        error: Optional[str] = None
    ) -> None:
        """Update scan progress for a table."""
        end_time = datetime.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Load existing progress or create new
        progress = self.persistence.load_progress(table_name) or ScanProgress(table_name=table_name)
        
        # Update the appropriate level completion time and duration
        if self.scan_level == ScanLevel.LEVEL_1_DISCOVERY:
            progress.level_1_completed = end_time if success else None
            progress.level_1_duration_ms = duration_ms if success else None
        elif self.scan_level == ScanLevel.LEVEL_2_SCHEMA:
            progress.level_2_completed = end_time if success else None
            progress.level_2_duration_ms = duration_ms if success else None
        elif self.scan_level == ScanLevel.LEVEL_3_PROFILE:
            progress.level_3_completed = end_time if success else None
            progress.level_3_duration_ms = duration_ms if success else None
        elif self.scan_level == ScanLevel.LEVEL_4_ADVANCED:
            progress.level_4_completed = end_time if success else None
            progress.level_4_duration_ms = duration_ms if success else None
        
        # Update error information if applicable
        if not success and error:
            progress.last_error = error
            progress.last_error_time = end_time
        elif success:
            # Clear error if scan was successful
            progress.last_error = None
            progress.last_error_time = None
        
        progress.last_updated = end_time
        self.persistence.save_progress(progress)
    
    def _should_skip_table(self, table_name: str, resume: bool) -> bool:
        """Check if a table should be skipped based on resume logic."""
        if not resume:
            return False
            
        # Check if table has already been scanned at this level
        completed_tables = self.persistence.list_completed_tables(self.scan_level)
        return table_name in completed_tables
    
    def _is_view(self, table_name: str) -> bool:
        """Check if a table is a view."""
        try:
            # Check if table exists in lakehouse metadata
            tables_df = self.spark.sql("SHOW TABLES")
            table_info = tables_df.filter(f"tableName = '{table_name}'").collect()
            
            if table_info:
                row = table_info[0]
                # Spark Row -> convert to dict safely
                row_dict = row.asDict(recursive=False)
                table_type = str(row_dict.get("tableType", "")).lower()
                return 'view' in table_type
            return False
        except Exception as e:
            self.logger.warning(f"Could not determine if {table_name} is a view: {e}")
            return False
    
    def _filter_tables(self, table_names: List[str], resume: bool) -> List[str]:
        """Filter table list based on exclusion rules and resume logic."""
        filtered_tables = []
        
        for table_name in table_names:
            # Skip views if configured
            if self.exclude_views and self._is_view(table_name):
                self.logger.debug(f"Skipping view: {table_name}")
                continue
            
            # Skip already scanned tables if resuming
            if self._should_skip_table(table_name, resume):
                self.logger.debug(f"Skipping already scanned table: {table_name}")
                continue
                
            filtered_tables.append(table_name)
        
        return filtered_tables
    
    def _validate_tables_exist(self, table_names: List[str]) -> List[str]:
        """Validate that tables exist and are accessible."""
        valid_tables = []
        
        for table_name in table_names:
            try:
                # Try to get basic info about the table
                self.lakehouse.get_table_info(table_name)
                valid_tables.append(table_name)
            except Exception as e:
                self.logger.warning(f"Table {table_name} not accessible: {e}")
                continue
        
        return valid_tables
    
    def get_scan_statistics(self) -> Dict[str, Any]:
        """Get statistics about completed scans for this level."""
        completed_tables = self.persistence.list_completed_tables(self.scan_level)
        return {
            "scan_level": self.scan_level.value,
            "scan_name": self.scan_name,
            "completed_tables_count": len(completed_tables),
            "completed_tables": completed_tables
        }
"""Abstract interface for profile persistence."""

from abc import ABC, abstractmethod
from typing import List, Optional

from ..enums.profile_types import ScanLevel
from ..models.metadata import TableMetadata, SchemaMetadata, ScanProgress
from ..models.profile_models import DatasetProfile


class PersistenceInterface(ABC):
    """Abstract interface for persisting profiling results."""
    
    @abstractmethod
    def save_table_metadata(self, metadata: TableMetadata) -> None:
        """Save table metadata from Level 1 scan."""
        pass
    
    @abstractmethod
    def load_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Load table metadata for a specific table."""
        pass
    
    @abstractmethod
    def save_schema_metadata(self, metadata: SchemaMetadata) -> None:
        """Save schema metadata from Level 2 scan."""
        pass
    
    @abstractmethod
    def load_schema_metadata(self, table_name: str) -> Optional[SchemaMetadata]:
        """Load schema metadata for a specific table."""
        pass
    
    @abstractmethod
    def save_profile(self, profile: DatasetProfile) -> None:
        """Save profile results from Level 3/4 scans."""
        pass
    
    @abstractmethod
    def load_profile(self, table_name: str, level: Optional[ScanLevel] = None) -> Optional[DatasetProfile]:
        """Load profile for a specific table and optional scan level."""
        pass
    
    @abstractmethod
    def save_progress(self, progress: ScanProgress) -> None:
        """Save scan progress for a table."""
        pass
    
    @abstractmethod
    def load_progress(self, table_name: str) -> Optional[ScanProgress]:
        """Load scan progress for a table."""
        pass
    
    @abstractmethod
    def list_completed_tables(self, level: ScanLevel) -> List[str]:
        """List all tables that have completed a specific scan level."""
        pass
    
    @abstractmethod
    def clear_table_data(self, table_name: str) -> None:
        """Clear all persisted data for a specific table."""
        pass
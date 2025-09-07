"""In-memory persistence implementation for testing and development."""

from datetime import datetime
from typing import List, Optional, Dict, Any

from .base_persistence import BasePersistence
from ..core.enums.profile_types import ScanLevel
from ..core.models.metadata import TableMetadata, SchemaMetadata, ScanProgress
from ..core.models.profile_models import DatasetProfile


class MemoryPersistence(BasePersistence):
    """In-memory persistence implementation for testing and development."""
    
    def __init__(self, table_prefix: str = "tiered_profile"):
        """
        Initialize in-memory persistence.
        
        Args:
            table_prefix: Prefix for profile result tables (used for namespacing)
        """
        super().__init__(table_prefix)
        
        # In-memory storage
        self._metadata_store: Dict[str, TableMetadata] = {}
        self._schema_store: Dict[str, SchemaMetadata] = {}
        self._profile_store: Dict[str, DatasetProfile] = {}
        self._progress_store: Dict[str, ScanProgress] = {}
        
        self.logger.info(f"Initialized MemoryPersistence with prefix: {table_prefix}")
    
    def save_table_metadata(self, metadata: TableMetadata) -> None:
        """Save table metadata to memory."""
        # Update scan timestamp
        metadata.scan_timestamp = datetime.now()
        self._metadata_store[metadata.table_name] = metadata
        self.logger.debug(f"Saved table metadata for: {metadata.table_name}")
    
    def load_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Load table metadata for a specific table."""
        metadata = self._metadata_store.get(table_name)
        if metadata:
            self.logger.debug(f"Loaded table metadata for: {table_name}")
        return metadata
    
    def save_schema_metadata(self, metadata: SchemaMetadata) -> None:
        """Save schema metadata to memory."""
        # Update scan timestamp
        metadata.scan_timestamp = datetime.now()
        self._schema_store[metadata.table_name] = metadata
        self.logger.debug(f"Saved schema metadata for: {metadata.table_name}")
    
    def load_schema_metadata(self, table_name: str) -> Optional[SchemaMetadata]:
        """Load schema metadata for a specific table."""
        metadata = self._schema_store.get(table_name)
        if metadata:
            self.logger.debug(f"Loaded schema metadata for: {table_name}")
        return metadata
    
    def save_profile(self, profile: DatasetProfile) -> None:
        """Save profile results from Level 3/4 scans."""
        # Update profile timestamp
        profile.profile_timestamp = datetime.now().isoformat()
        self._profile_store[profile.dataset_name] = profile
        self.logger.debug(f"Saved profile for: {profile.dataset_name}")
    
    def load_profile(self, table_name: str, level: Optional[ScanLevel] = None) -> Optional[DatasetProfile]:
        """Load profile for a specific table and optional scan level."""
        # Note: In memory implementation doesn't differentiate by scan level
        # since profiles contain all available information
        profile = self._profile_store.get(table_name)
        if profile:
            self.logger.debug(f"Loaded profile for: {table_name}")
        return profile
    
    def save_progress(self, progress: ScanProgress) -> None:
        """Save scan progress for a table."""
        # Update last_updated timestamp
        progress.last_updated = datetime.now()
        self._progress_store[progress.table_name] = progress
        self.logger.debug(f"Saved progress for: {progress.table_name}")
    
    def load_progress(self, table_name: str) -> Optional[ScanProgress]:
        """Load scan progress for a table."""
        progress = self._progress_store.get(table_name)
        if progress:
            self.logger.debug(f"Loaded progress for: {table_name}")
        return progress
    
    def list_completed_tables(self, level: ScanLevel) -> List[str]:
        """List all tables that have completed a specific scan level."""
        completed_tables = []
        
        for table_name, progress in self._progress_store.items():
            # Check if the specified scan level is completed
            if level == ScanLevel.LEVEL_1_DISCOVERY and progress.level_1_completed:
                completed_tables.append(table_name)
            elif level == ScanLevel.LEVEL_2_SCHEMA and progress.level_2_completed:
                completed_tables.append(table_name)
            elif level == ScanLevel.LEVEL_3_PROFILE and progress.level_3_completed:
                completed_tables.append(table_name)
            elif level == ScanLevel.LEVEL_4_ADVANCED and progress.level_4_completed:
                completed_tables.append(table_name)
        
        self.logger.debug(f"Found {len(completed_tables)} tables completed at {level}")
        return completed_tables
    
    def clear_table_data(self, table_name: str) -> None:
        """Clear all persisted data for a specific table."""
        stores_cleared = 0
        
        if table_name in self._metadata_store:
            del self._metadata_store[table_name]
            stores_cleared += 1
        
        if table_name in self._schema_store:
            del self._schema_store[table_name]
            stores_cleared += 1
        
        if table_name in self._profile_store:
            del self._profile_store[table_name]
            stores_cleared += 1
        
        if table_name in self._progress_store:
            del self._progress_store[table_name]
            stores_cleared += 1
        
        self.logger.info(f"Cleared data for {table_name} from {stores_cleared} stores")
    
    def clear_all_data(self) -> None:
        """Clear all stored data (useful for testing)."""
        self._metadata_store.clear()
        self._schema_store.clear()
        self._profile_store.clear()
        self._progress_store.clear()
        self.logger.info("Cleared all stored data")
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about stored data (useful for debugging)."""
        stats = {
            "metadata_count": len(self._metadata_store),
            "schema_count": len(self._schema_store),
            "profile_count": len(self._profile_store),
            "progress_count": len(self._progress_store),
        }
        return stats
    
    def list_all_tables(self) -> List[str]:
        """List all tables that have any stored data."""
        all_tables = set()
        all_tables.update(self._metadata_store.keys())
        all_tables.update(self._schema_store.keys())
        all_tables.update(self._profile_store.keys())
        all_tables.update(self._progress_store.keys())
        return sorted(list(all_tables))
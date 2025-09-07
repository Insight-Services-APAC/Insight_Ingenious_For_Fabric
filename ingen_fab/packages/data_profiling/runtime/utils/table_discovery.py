"""Table discovery utilities for data profiling."""

from typing import List, Optional

from ..core.enums.profile_types import ScanLevel
from ..core.interfaces.persistence_interface import PersistenceInterface
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


class TableDiscoveryService:
    """Service for discovering and filtering tables for profiling."""
    
    def __init__(
        self,
        lakehouse: lakehouse_utils,
        persistence: PersistenceInterface,
        config_lakehouse: Optional[lakehouse_utils] = None
    ):
        """
        Initialize table discovery service.
        
        Args:
            lakehouse: Main lakehouse utils instance
            persistence: Persistence interface for checking existing tables
            config_lakehouse: Optional separate lakehouse for configuration tables
        """
        self.lakehouse = lakehouse
        self.persistence = persistence
        self.config_lakehouse = config_lakehouse or lakehouse
    
    def discover_tables_to_profile(
        self,
        target_tables: Optional[List[str]] = None,
        only_scan_new_tables: bool = False,
        force_rescan: bool = False,
        config_table_name: str = "config_data_profiling"
    ) -> List[str]:
        """
        Discover tables to profile based on various strategies.
        
        Args:
            target_tables: Optional explicit list of tables to profile
            only_scan_new_tables: Only profile tables not yet scanned
            force_rescan: Force rescan of all tables regardless of existing scans
            config_table_name: Name of the configuration table
            
        Returns:
            List of table names to profile
        """
        if target_tables:
            print(f"✅ Using provided target tables: {len(target_tables)} tables")
            return target_tables
        
        # Try to get tables from configuration table
        tables_from_config = self._get_tables_from_config(config_table_name)
        if tables_from_config:
            return tables_from_config
        
        # Fallback to discovering all tables in lakehouse
        return self._discover_all_tables(only_scan_new_tables, force_rescan)
    
    def _get_tables_from_config(self, config_table_name: str) -> Optional[List[str]]:
        """Get active tables from configuration table."""
        try:
            config_df = self.config_lakehouse.read_table(config_table_name)
            active_configs = config_df.filter("active_yn = 'Y'").collect()
            tables_to_profile = [row.table_name for row in active_configs]
            print(f"✅ Found {len(tables_to_profile)} active tables from {config_table_name}")
            return tables_to_profile
        except Exception as e:
            print(f"⚠️  No configuration table found ({config_table_name}): {e}")
            return None
    
    def _discover_all_tables(
        self, 
        only_scan_new_tables: bool, 
        force_rescan: bool
    ) -> List[str]:
        """Discover all tables in the lakehouse with optional filtering."""
        print("⚠️  No configuration table found, discovering all tables")
        
        # Get all tables in the lakehouse
        all_tables = self.lakehouse.list_tables()
        
        if only_scan_new_tables and not force_rescan:
            # Filter to only new tables not in metadata
            try:
                existing_tables = self.persistence.list_completed_tables(ScanLevel.LEVEL_1_DISCOVERY)
                tables_to_profile = [t for t in all_tables if t not in existing_tables]
                print(f"✅ Found {len(tables_to_profile)} new tables to profile (out of {len(all_tables)} total)")
                return tables_to_profile
            except Exception as e:
                print(f"⚠️  Could not check existing tables: {e}")
                # Fallback to all tables
                pass
        
        print(f"✅ Found {len(all_tables)} tables in lakehouse")
        return all_tables
    
    def filter_tables_by_scan_level(
        self, 
        tables: List[str], 
        min_scan_level: ScanLevel
    ) -> List[str]:
        """
        Filter tables that have completed at least the specified scan level.
        
        Args:
            tables: List of table names to filter
            min_scan_level: Minimum scan level required
            
        Returns:
            Filtered list of table names
        """
        try:
            completed_tables = self.persistence.list_completed_tables(min_scan_level)
            filtered_tables = [t for t in tables if t in completed_tables]
            print(f"✅ Found {len(filtered_tables)} tables with {min_scan_level.value} completed")
            return filtered_tables
        except Exception as e:
            print(f"⚠️  Could not filter by scan level: {e}")
            return tables
    
    def get_table_discovery_stats(self) -> dict:
        """Get statistics about table discovery and scan progress."""
        try:
            all_tables = self.lakehouse.list_tables()
            stats = {
                "total_tables": len(all_tables),
                "discovered_tables": len(self.persistence.list_completed_tables(ScanLevel.LEVEL_1_DISCOVERY)),
                "schema_scanned": len(self.persistence.list_completed_tables(ScanLevel.LEVEL_2_SCHEMA)),
                "profile_scanned": len(self.persistence.list_completed_tables(ScanLevel.LEVEL_3_PROFILE)),
                "advanced_scanned": len(self.persistence.list_completed_tables(ScanLevel.LEVEL_4_ADVANCED))
            }
            return stats
        except Exception as e:
            return {"error": str(e)}
    
    def validate_tables_exist(self, table_names: List[str]) -> List[str]:
        """
        Validate that tables exist and are accessible.
        
        Args:
            table_names: List of table names to validate
            
        Returns:
            List of validated table names that exist
        """
        valid_tables = []
        invalid_tables = []
        
        for table_name in table_names:
            try:
                # Try to read the table to validate it exists and is accessible
                df = self.lakehouse.read_table(table_name)
                df.count()  # Force evaluation to check accessibility
                valid_tables.append(table_name)
            except Exception as e:
                invalid_tables.append((table_name, str(e)))
        
        if invalid_tables:
            print(f"⚠️  Found {len(invalid_tables)} invalid/inaccessible tables:")
            for table_name, error in invalid_tables[:3]:  # Show first 3
                print(f"   - {table_name}: {error}")
            if len(invalid_tables) > 3:
                print(f"   ... and {len(invalid_tables) - 3} more")
        
        print(f"✅ Validated {len(valid_tables)} accessible tables")
        return valid_tables
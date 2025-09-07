"""Level 1 Discovery - Fast discovery of Delta tables (metadata only)."""

import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from delta.tables import DeltaTable

from .base_scan_level import BaseScanLevel
from ....core.enums.profile_types import ScanLevel
from ....core.models.metadata import TableMetadata


class Level1DiscoveryScanner(BaseScanLevel):
    """
    Level 1 Scan: Fast discovery of Delta tables (metadata only).
    
    This scan discovers tables and captures basic metadata without
    reading any data. It's the fastest scan level and provides:
    - Table paths and names
    - Basic Delta table metadata (file count, size, creation time)
    - Partition information
    - Row count estimates (when available from metadata)
    """
    
    @property
    def scan_level(self) -> ScanLevel:
        return ScanLevel.LEVEL_1_DISCOVERY
    
    @property 
    def scan_name(self) -> str:
        return "LEVEL 1 SCAN: Table Discovery (Metadata Only)"
    
    @property
    def scan_description(self) -> str:
        return "Fast discovery of Delta tables and basic metadata collection"
    
    def execute(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        **kwargs
    ) -> List[TableMetadata]:
        """
        Execute Level 1 discovery scan.
        
        Args:
            table_names: Optional list of specific table paths to scan
            resume: Whether to skip already scanned tables
            **kwargs: Additional parameters (unused in Level 1)
            
        Returns:
            List of TableMetadata objects for discovered tables
        """
        self._print_scan_header()
        
        discovered_tables = []
        
        # Discover tables
        if table_names:
            tables_to_scan = table_names
        else:
            tables_to_scan = self._discover_delta_tables()
        
        # Filter out already scanned tables if resuming
        tables_to_scan = self._filter_tables([self._extract_table_name(path) for path in tables_to_scan], resume)
        
        if not tables_to_scan:
            print("ℹ️  No tables to scan (all tables already scanned or no tables found)")
            return discovered_tables
        
        print(f"📊 Found {len(tables_to_scan)} tables to scan")
        
        # Process each table
        for i, table_path in enumerate(tables_to_scan, 1):
            table_name = self._extract_table_name(table_path)
            start_time = datetime.now()
            
            print(f"\n[{i}/{len(tables_to_scan)}] 🔍 Scanning: {table_name}")
            
            try:
                # Extract table metadata
                metadata = self._extract_table_metadata(table_path, table_name)
                
                # Save to persistence
                self.persistence.save_table_metadata(metadata)
                discovered_tables.append(metadata)
                
                # Update progress
                self._update_progress(table_name, start_time, success=True)
                
                row_count_str = f"{metadata.row_count:,}" if metadata.row_count is not None else "unknown"
                print(f"    ✅ Success: {row_count_str} rows, {metadata.size_bytes or 0:,} bytes")
                
            except Exception as e:
                error_details = f"Level 1 scan failed for {table_name}: {str(e)}"
                print(f"    ❌ Error: {error_details}")
                print(f"    📍 Location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
                print(f"    🔍 Full traceback: {traceback.format_exc()}")
                
                # Update progress with error
                self._update_progress(table_name, start_time, success=False, error=error_details)
        
        print(f"\n✨ Level 1 scan complete. Discovered {len(discovered_tables)} tables")
        return discovered_tables
    
    def _discover_delta_tables(self) -> List[str]:
        """Discover all Delta tables in the lakehouse using lakehouse_utils."""
        try:
            # Use lakehouse_utils list_tables method
            table_names = self.lakehouse.list_tables()
            
            # Filter out views if exclude_views is True
            if self.exclude_views:
                filtered_tables = []
                for table_name in table_names:
                    if not self._is_view(table_name):
                        filtered_tables.append(table_name)
                
                excluded_count = len(table_names) - len(filtered_tables)
                if excluded_count > 0:
                    print(f"ℹ️  Excluded {excluded_count} views from profiling")
                    
                table_names = filtered_tables
            
            # Convert table names to full paths for consistency
            tables_uri = self.lakehouse.lakehouse_tables_uri()
            tables = [f"{tables_uri}{table_name}" for table_name in table_names]
            
            return tables
        except Exception as e:
            print(f"  ⚠️  Could not discover tables using lakehouse_utils: {e}")
            return []
    
    def _extract_table_name(self, table_path: str) -> str:
        """Extract table name from path."""
        if "/" in table_path:
            return table_path.rstrip("/").split("/")[-1]
        return table_path
    
    def _extract_table_metadata(self, table_path: str, table_name: str) -> TableMetadata:
        """Extract metadata for a Delta table without reading data."""
        metadata = TableMetadata(
            table_name=table_name,
            table_path=table_path
        )
        
        try:
            # Check if table exists using lakehouse_utils
            if not self.lakehouse.check_if_table_exists(table_name):
                print(f"    ⚠️  Table {table_name} does not exist in lakehouse")
                return metadata
            
            # Get Delta table details directly
            if "file://" in table_path or "/" in table_path:
                delta_table = DeltaTable.forPath(self.spark, self.lakehouse.lakehouse_tables_uri() + table_name)
            else:
                delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Get table details using Delta API
            details = delta_table.detail().collect()[0]
            
            # Helper for tolerant attribute access with warning
            def attr_with_warning(obj, attr, default=None):
                if not hasattr(obj, attr) or getattr(obj, attr) is None:
                    print(f"    ⚠️  Warning: attribute '{attr}' missing in Delta table details for {table_name}")
                    return default
                return getattr(obj, attr)

            metadata.num_files = attr_with_warning(details, 'numFiles')
            metadata.size_bytes = attr_with_warning(details, 'sizeInBytes')
            metadata.created_time = str(attr_with_warning(details, 'createdAt', ''))
            metadata.modified_time = str(attr_with_warning(details, 'lastModified', ''))

            # Get partition columns if any
            partition_cols = attr_with_warning(details, 'partitionColumns', [])
            if partition_cols:
                metadata.partition_columns = partition_cols
            
            # Try to get row count from table metadata (faster than querying data)
            try:
                # Use DESCRIBE DETAIL which doesn't read all data
                describe_df = self.spark.sql(f"DESCRIBE DETAIL {table_name}")
                describe_row = describe_df.collect()[0]
                
                # Try to get numRecords from statistics
                if hasattr(describe_row, 'statistics') and describe_row.statistics:
                    import json
                    stats = json.loads(describe_row.statistics)
                    if 'numRecords' in stats:
                        metadata.row_count = int(stats['numRecords'])
                        print(f"    📊 Got row count from statistics: {metadata.row_count:,}")
                    
            except Exception as stats_e:
                print(f"    ⚠️  Could not get row count from statistics: {stats_e}")
                
                # Fallback: Try a simple COUNT query with LIMIT for performance
                try:
                    count_df = self.spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
                    count_result = count_df.collect()[0]
                    metadata.row_count = int(count_result['row_count'])
                    print(f"    📊 Got row count from COUNT query: {metadata.row_count:,}")
                except Exception as count_e:
                    print(f"    ⚠️  Could not get row count from COUNT query: {count_e}")
                    metadata.row_count = None
            
            # Try to get additional properties
            try:
                table_properties = delta_table.history(1).collect()[0]
                if hasattr(table_properties, 'operationParameters'):
                    metadata.properties = dict(table_properties.operationParameters)
            except Exception as props_e:
                print(f"    ⚠️  Could not get table properties: {props_e}")
                
        except Exception as e:
            print(f"    ⚠️  Could not extract full metadata for {table_name}: {e}")
            # Return basic metadata even if detailed extraction fails
            
        return metadata
    
    def get_discovered_table_names(self) -> List[str]:
        """Get list of table names that have been discovered."""
        return self.persistence.list_completed_tables(self.scan_level)
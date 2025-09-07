"""Level 2 Schema - Column metadata extraction."""

import traceback
import time
from datetime import datetime
from typing import Dict, List, Optional

from .base_scan_level import BaseScanLevel
from ....core.enums.profile_types import ScanLevel
from ....core.models.metadata import SchemaMetadata, ScanProgress, ColumnMetadata


class Level2SchemaScanner(BaseScanLevel):
    """
    Level 2 Scan: Column metadata extraction.
    
    This scan captures column names, types, and counts without reading data.
    It provides:
    - Column names and data types
    - Column count
    - Primary and foreign key candidates based on naming patterns
    - Column nullability information
    """
    
    @property
    def scan_level(self) -> ScanLevel:
        return ScanLevel.LEVEL_2_SCHEMA
    
    @property 
    def scan_name(self) -> str:
        return "LEVEL 2 SCAN: Schema Discovery (Column Metadata)"
    
    @property
    def scan_description(self) -> str:
        return "Column metadata extraction and key identification"
    
    def execute(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        **kwargs
    ) -> List[SchemaMetadata]:
        """
        Execute Level 2 schema scan.
        
        Args:
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables
            **kwargs: Additional parameters (unused in Level 2)
            
        Returns:
            List of SchemaMetadata objects
        """
        self._print_scan_header()
        
        schemas = []
        
        # Get tables to scan
        if table_names:
            tables_to_scan = table_names
        else:
            # Get all tables that completed Level 1
            level_1_completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_1_DISCOVERY)
            tables_to_scan = level_1_completed
        
        # Filter out already scanned tables if resuming
        tables_to_scan = self._filter_tables(tables_to_scan, resume)
        
        if not tables_to_scan:
            print("ℹ️  No tables to scan (all tables already scanned or no Level 1 completed)")
            return schemas
            
        print(f"📊 Processing {len(tables_to_scan)} tables")
        
        for i, table_name in enumerate(tables_to_scan, 1):
            print(f"\n[{i}/{len(tables_to_scan)}] Processing schema for: {table_name}")
            
            start_time = datetime.now()
            
            try:
                # Load table metadata from Level 1
                table_metadata = self.persistence.load_table_metadata(table_name)
                if not table_metadata:
                    print(f"    ⚠️  No Level 1 metadata found for {table_name}, skipping")
                    continue
                
                # Extract schema metadata
                schema = self._extract_schema_metadata(table_metadata.table_path, table_name)
                
                # Save to persistence
                self.persistence.save_schema_metadata(schema)
                schemas.append(schema)
                
                # Update progress
                self._update_progress(table_name, start_time, success=True)
                
                print(f"    ✅ Success: {schema.column_count} columns identified")
                
            except Exception as e:
                error_details = f"Level 2 scan failed for {table_name}: {str(e)}"
                print(f"    ❌ Error: {error_details}")
                print(f"    📍 Location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
                print(f"    🔍 Full traceback: {traceback.format_exc()}")
                
                # Update progress with error
                self._update_progress(table_name, start_time, success=False, error=error_details)
        
        print(f"\n✨ Level 2 scan complete. Processed {len(schemas)} schemas")
        return schemas
    
    def _extract_schema_metadata(self, table_path: str, table_name: str) -> SchemaMetadata:
        """Extract schema metadata using lakehouse_utils."""
        try:
            # Use lakehouse_utils get_table_schema method
            schema_dict = self.lakehouse.get_table_schema(table_name)
            
            # Create column metadata objects
            columns = []
            primary_key_candidates = []
            foreign_key_candidates = []
            
            for col_name, col_type in schema_dict.items():
                # Create column metadata
                column_metadata = ColumnMetadata(
                    name=col_name,
                    data_type=col_type,
                    nullable=True,  # lakehouse_utils doesn't provide nullability info
                    is_primary_key=False,  # Will be updated based on analysis
                    is_foreign_key=False   # Will be updated based on analysis
                )
                columns.append(column_metadata)
                
                # Identify potential keys based on naming patterns
                col_lower = col_name.lower()
                if any(pattern in col_lower for pattern in ['_id', 'id_', '_key', 'key_', '_pk']):
                    if col_lower == 'id' or col_lower.endswith('_id'):
                        primary_key_candidates.append(col_name)
                    else:
                        foreign_key_candidates.append(col_name)
            
            # Create backward-compatible columns list (dicts)
            columns_dict = [
                {"name": col.name, "type": col.data_type, "nullable": str(col.nullable)}
                for col in columns
            ]
            
            schema = SchemaMetadata(
                table_name=table_name,
                column_count=len(columns),
                columns=columns_dict,
                column_metadata=columns,
                primary_key_candidates=primary_key_candidates,
                foreign_key_candidates=foreign_key_candidates
            )
            
            return schema
            
        except Exception as e:
            print(f"    ⚠️  Could not extract schema using lakehouse_utils: {e}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(e.__traceback__)[-1].lineno}")
            print(f"    🔍 Traceback: {traceback.format_exc()}")
            # Fallback to direct DataFrame approach
            return self._extract_schema_metadata_fallback(table_path, table_name)
    
    def _extract_schema_metadata_fallback(self, table_path: str, table_name: str) -> SchemaMetadata:
        """Fallback method for schema extraction using direct DataFrame access."""
        print(f"    🔄 Using fallback method for {table_name}")
        
        try:
            # Get DataFrame schema directly
            df = self.lakehouse.read_table(table_name)
            spark_schema = df.schema
            
            columns = []
            primary_key_candidates = []
            foreign_key_candidates = []
            
            for field in spark_schema.fields:
                # Create column metadata
                column_metadata = ColumnMetadata(
                    name=field.name,
                    data_type=str(field.dataType),
                    nullable=field.nullable,
                    is_primary_key=False,
                    is_foreign_key=False
                )
                columns.append(column_metadata)
                
                # Identify potential keys based on naming patterns
                col_lower = field.name.lower()
                if any(pattern in col_lower for pattern in ['_id', 'id_', '_key', 'key_', '_pk']):
                    if col_lower == 'id' or col_lower.endswith('_id'):
                        primary_key_candidates.append(field.name)
                    else:
                        foreign_key_candidates.append(field.name)
            
            # Create backward-compatible columns list (dicts)
            columns_dict = [
                {"name": col.name, "type": col.data_type, "nullable": str(col.nullable)}
                for col in columns
            ]
            
            schema = SchemaMetadata(
                table_name=table_name,
                column_count=len(columns),
                columns=columns_dict,
                column_metadata=columns,
                primary_key_candidates=primary_key_candidates,
                foreign_key_candidates=foreign_key_candidates
            )
            
            print(f"    ✅ Fallback successful: {len(columns)} columns extracted")
            return schema
            
        except Exception as fallback_e:
            print(f"    ❌ Fallback also failed: {fallback_e}")
            # Return minimal schema
            return SchemaMetadata(
                table_name=table_name,
                column_count=0,
                columns=[],
                primary_key_candidates=[],
                foreign_key_candidates=[],
                scan_timestamp=datetime.now()
            )
    
    def _identify_key_columns(self, column_name: str) -> tuple[bool, bool]:
        """
        Identify if a column is likely a primary or foreign key.
        
        Args:
            column_name: Name of the column to analyze
            
        Returns:
            Tuple of (is_primary_key_candidate, is_foreign_key_candidate)
        """
        col_lower = column_name.lower()
        
        # Primary key patterns
        primary_patterns = ['id', 'pk', '_id', 'key']
        is_primary = any(pattern in col_lower for pattern in primary_patterns)
        
        # Foreign key patterns 
        foreign_patterns = ['_id', 'id_', '_key', 'key_', 'fk_', '_fk']
        is_foreign = any(pattern in col_lower for pattern in foreign_patterns)
        
        # If column is exactly 'id', it's likely a primary key
        if col_lower == 'id':
            return True, False
        
        # If it ends with '_id', it's likely a foreign key
        if col_lower.endswith('_id'):
            return False, True
            
        return is_primary, is_foreign
    
    def get_table_schemas(self, table_names: Optional[List[str]] = None) -> Dict[str, SchemaMetadata]:
        """
        Get schema metadata for completed tables.
        
        Args:
            table_names: Optional list of specific table names
            
        Returns:
            Dictionary mapping table names to their SchemaMetadata
        """
        if table_names is None:
            table_names = self.persistence.list_completed_tables(self.scan_level)
        
        schemas = {}
        for table_name in table_names:
            schema = self.persistence.load_schema_metadata(table_name)
            if schema:
                schemas[table_name] = schema
                
        return schemas
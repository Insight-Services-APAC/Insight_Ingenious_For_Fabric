"""Base persistence implementation with common functionality."""

from abc import ABC
from typing import List, Optional, Dict, Any
import json
import logging

from ..core.interfaces.persistence_interface import PersistenceInterface
from ..core.enums.profile_types import ScanLevel
from ..core.models.metadata import TableMetadata, SchemaMetadata, ScanProgress
from ..core.models.profile_models import DatasetProfile


class BasePersistence(PersistenceInterface, ABC):
    """Base class for persistence implementations with common functionality."""
    
    def __init__(self, table_prefix: str = "tiered_profile"):
        """
        Initialize base persistence.
        
        Args:
            table_prefix: Prefix for profile result tables
        """
        self.table_prefix = table_prefix
        self.logger = logging.getLogger(__name__)
        
        # Table names for different scan levels
        self.metadata_table = f"{table_prefix}_metadata"
        self.schema_table = f"{table_prefix}_schemas"  
        self.profile_table = f"{table_prefix}_profiles"
        self.progress_table = f"{table_prefix}_progress"
    
    def _serialize_metadata(self, metadata: TableMetadata) -> Dict[str, Any]:
        """Convert TableMetadata to serializable dictionary."""
        return {
            "table_name": metadata.table_name,
            "table_path": metadata.table_path,
            "table_format": metadata.table_format,
            "row_count": metadata.row_count,
            "size_bytes": metadata.size_bytes,
            "num_files": metadata.num_files,
            "created_time": metadata.created_time,
            "modified_time": metadata.modified_time,
            "partition_columns": json.dumps(metadata.partition_columns) if metadata.partition_columns else None,
            "properties": json.dumps(metadata.properties) if metadata.properties else None,
            "scan_timestamp": metadata.scan_timestamp
        }
    
    def _deserialize_metadata(self, data: Dict[str, Any]) -> TableMetadata:
        """Convert dictionary back to TableMetadata."""
        return TableMetadata(
            table_name=data["table_name"],
            table_path=data["table_path"],
            table_format=data.get("table_format", "delta"),
            row_count=data.get("row_count"),
            size_bytes=data.get("size_bytes"),
            num_files=data.get("num_files"),
            created_time=data.get("created_time"),
            modified_time=data.get("modified_time"),
            partition_columns=json.loads(data["partition_columns"]) if data.get("partition_columns") else None,
            properties=json.loads(data["properties"]) if data.get("properties") else None,
            scan_timestamp=data.get("scan_timestamp")
        )
    
    def _serialize_schema_metadata(self, metadata: SchemaMetadata) -> Dict[str, Any]:
        """Convert SchemaMetadata to serializable dictionary."""
        return {
            "table_name": metadata.table_name,
            "column_count": metadata.column_count,
            "columns": json.dumps([col.to_dict() for col in metadata.columns]),
            "primary_key_candidates": json.dumps(metadata.primary_key_candidates) if metadata.primary_key_candidates else None,
            "foreign_key_candidates": json.dumps(metadata.foreign_key_candidates) if metadata.foreign_key_candidates else None,
            "scan_timestamp": metadata.scan_timestamp
        }
    
    def _deserialize_schema_metadata(self, data: Dict[str, Any]) -> SchemaMetadata:
        """Convert dictionary back to SchemaMetadata."""
        from ..core.models.metadata import ColumnMetadata
        
        columns_data = json.loads(data["columns"])
        columns = [ColumnMetadata.from_dict(col_data) for col_data in columns_data]
        
        return SchemaMetadata(
            table_name=data["table_name"],
            column_count=data["column_count"],
            columns=columns,
            primary_key_candidates=json.loads(data["primary_key_candidates"]) if data.get("primary_key_candidates") else None,
            foreign_key_candidates=json.loads(data["foreign_key_candidates"]) if data.get("foreign_key_candidates") else None,
            scan_timestamp=data.get("scan_timestamp")
        )
    
    def _serialize_profile(self, profile: DatasetProfile) -> Dict[str, Any]:
        """Convert DatasetProfile to serializable dictionary."""
        return profile.to_dict()
    
    def _deserialize_profile(self, data: Dict[str, Any]) -> DatasetProfile:
        """Convert dictionary back to DatasetProfile."""
        return DatasetProfile.from_dict(data)
    
    def _serialize_progress(self, progress: ScanProgress) -> Dict[str, Any]:
        """Convert ScanProgress to serializable dictionary."""
        return {
            "table_name": progress.table_name,
            "level_1_completed": progress.level_1_completed,
            "level_1_duration_ms": progress.level_1_duration_ms,
            "level_2_completed": progress.level_2_completed,
            "level_2_duration_ms": progress.level_2_duration_ms,
            "level_3_completed": progress.level_3_completed,
            "level_3_duration_ms": progress.level_3_duration_ms,
            "level_4_completed": progress.level_4_completed,
            "level_4_duration_ms": progress.level_4_duration_ms,
            "last_error": progress.last_error,
            "last_error_time": progress.last_error_time,
            "last_updated": progress.last_updated
        }
    
    def _deserialize_progress(self, data: Dict[str, Any]) -> ScanProgress:
        """Convert dictionary back to ScanProgress."""
        return ScanProgress(
            table_name=data["table_name"],
            level_1_completed=data.get("level_1_completed"),
            level_1_duration_ms=data.get("level_1_duration_ms"),
            level_2_completed=data.get("level_2_completed"),
            level_2_duration_ms=data.get("level_2_duration_ms"),
            level_3_completed=data.get("level_3_completed"),
            level_3_duration_ms=data.get("level_3_duration_ms"),
            level_4_completed=data.get("level_4_completed"),
            level_4_duration_ms=data.get("level_4_duration_ms"),
            last_error=data.get("last_error"),
            last_error_time=data.get("last_error_time"),
            last_updated=data.get("last_updated")
        )
    
    def _get_scan_level_column(self, level: ScanLevel) -> str:
        """Get the column name for a specific scan level in progress table."""
        level_mapping = {
            ScanLevel.LEVEL_1_DISCOVERY: "level_1_completed",
            ScanLevel.LEVEL_2_SCHEMA: "level_2_completed", 
            ScanLevel.LEVEL_3_PROFILE: "level_3_completed",
            ScanLevel.LEVEL_4_ADVANCED: "level_4_completed"
        }
        return level_mapping.get(level, "level_1_completed")
"""Metadata models for tiered profiling."""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..enums.profile_types import ScanLevel


@dataclass
class TableMetadata:
    """Metadata for a discovered table."""
    table_name: str
    table_path: str
    table_format: str = "delta"
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    num_files: Optional[int] = None
    created_time: Optional[str] = None
    modified_time: Optional[str] = None
    partition_columns: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    scan_timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return asdict(self)


@dataclass
class ColumnMetadata:
    """Metadata for a table column."""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    is_partition: bool = False
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: Optional[str] = None  # Table.Column reference
    default_value: Optional[str] = None
    check_constraints: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnMetadata':
        """Create ColumnMetadata from dictionary."""
        # Handle backward compatibility: "type" -> "data_type"
        if "type" in data and "data_type" not in data:
            data = data.copy()
            data["data_type"] = data.pop("type")
        
        # Handle string boolean values
        if "nullable" in data and isinstance(data["nullable"], str):
            data["nullable"] = data["nullable"].lower() == "true"
            
        return cls(**data)


@dataclass
class SchemaMetadata:
    """Schema metadata for a table."""
    table_name: str
    column_count: int
    columns: List[Dict[str, str]] = field(default_factory=list)  # [{name, type, nullable}] - kept as dict for backward compatibility
    column_metadata: List[ColumnMetadata] = field(default_factory=list)  # New field using ColumnMetadata objects
    primary_key_candidates: List[str] = field(default_factory=list)
    foreign_key_candidates: List[str] = field(default_factory=list)
    scan_timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        result = asdict(self)
        # Ensure column_metadata is serialized properly
        if self.column_metadata:
            result['column_metadata'] = [col.to_dict() for col in self.column_metadata]
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaMetadata':
        """Create SchemaMetadata from dictionary."""
        # Convert column_metadata dicts back to ColumnMetadata objects
        if 'column_metadata' in data and data['column_metadata']:
            data['column_metadata'] = [
                ColumnMetadata(**col_dict) if isinstance(col_dict, dict) else col_dict 
                for col_dict in data['column_metadata']
            ]
        return cls(**data)


@dataclass
class ScanProgress:
    """Track scanning progress for a table."""
    table_name: str
    level_1_completed: Optional[datetime] = None
    level_2_completed: Optional[datetime] = None
    level_3_completed: Optional[datetime] = None
    level_4_completed: Optional[datetime] = None
    level_1_duration_ms: Optional[int] = None
    level_2_duration_ms: Optional[int] = None
    level_3_duration_ms: Optional[int] = None
    level_4_duration_ms: Optional[int] = None
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    
    def get_last_completed_level(self) -> Optional[ScanLevel]:
        """Get the last successfully completed scan level."""
        if self.level_4_completed:
            return ScanLevel.LEVEL_4_ADVANCED
        elif self.level_3_completed:
            return ScanLevel.LEVEL_3_PROFILE
        elif self.level_2_completed:
            return ScanLevel.LEVEL_2_SCHEMA
        elif self.level_1_completed:
            return ScanLevel.LEVEL_1_DISCOVERY
        return None
"""Relationship models for data profiling."""

from dataclasses import dataclass, field
from typing import Any, Dict, List

from ..enums.profile_types import RelationshipType
from .statistics import BusinessRule


@dataclass
class ColumnRelationship:
    """Relationship between columns across tables."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: RelationshipType
    confidence_score: float
    overlap_percentage: float
    referential_integrity_score: float
    suggested_join_condition: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "source_table": self.source_table,
            "source_column": self.source_column,
            "target_table": self.target_table,
            "target_column": self.target_column,
            "relationship_type": self.relationship_type.value,
            "confidence_score": self.confidence_score,
            "overlap_percentage": self.overlap_percentage,
            "referential_integrity_score": self.referential_integrity_score,
            "suggested_join_condition": self.suggested_join_condition
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnRelationship":
        """Create from dictionary for JSON deserialization."""
        return cls(
            source_table=data.get("source_table", ""),
            source_column=data.get("source_column", ""),
            target_table=data.get("target_table", ""),
            target_column=data.get("target_column", ""),
            relationship_type=RelationshipType(data.get("relationship_type", "one_to_one")),
            confidence_score=data.get("confidence_score", 0.0),
            overlap_percentage=data.get("overlap_percentage", 0.0),
            referential_integrity_score=data.get("referential_integrity_score", 0.0),
            suggested_join_condition=data.get("suggested_join_condition", "")
        )


@dataclass
class TableRelationship:
    """Relationship analysis between two tables."""
    table1: str
    table2: str
    relationship_type: RelationshipType
    confidence_score: float
    suggested_join_conditions: List[str] = field(default_factory=list)
    common_columns: List[str] = field(default_factory=list)
    referential_integrity_issues: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "table1": self.table1,
            "table2": self.table2,
            "relationship_type": self.relationship_type.value,
            "confidence_score": self.confidence_score,
            "suggested_join_conditions": self.suggested_join_conditions,
            "common_columns": self.common_columns,
            "referential_integrity_issues": self.referential_integrity_issues
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableRelationship":
        """Create from dictionary for JSON deserialization."""
        return cls(
            table1=data.get("table1", ""),
            table2=data.get("table2", ""),
            relationship_type=RelationshipType(data.get("relationship_type", "one_to_one")),
            confidence_score=data.get("confidence_score", 0.0),
            suggested_join_conditions=data.get("suggested_join_conditions", []),
            common_columns=data.get("common_columns", []),
            referential_integrity_issues=data.get("referential_integrity_issues", [])
        )


@dataclass
class EntityRelationshipGraph:
    """Complete entity relationship graph for a dataset."""
    entities: List[str] = field(default_factory=list)  # Table names
    relationships: List[TableRelationship] = field(default_factory=list)
    suggested_primary_keys: Dict[str, List[str]] = field(default_factory=dict)
    suggested_foreign_keys: Dict[str, List[ColumnRelationship]] = field(default_factory=dict)
    business_rules: Dict[str, List[BusinessRule]] = field(default_factory=dict)
    join_recommendations: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "entities": self.entities,
            "relationships": [rel.to_dict() for rel in self.relationships],
            "suggested_primary_keys": self.suggested_primary_keys,
            "suggested_foreign_keys": {
                k: [rel.to_dict() for rel in v] for k, v in self.suggested_foreign_keys.items()
            },
            "business_rules": {
                k: [rule.to_dict() for rule in v] for k, v in self.business_rules.items()
            },
            "join_recommendations": self.join_recommendations
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EntityRelationshipGraph":
        """Create from dictionary for JSON deserialization."""
        return cls(
            entities=data.get("entities", []),
            relationships=[TableRelationship.from_dict(rel) for rel in data.get("relationships", [])],
            suggested_primary_keys=data.get("suggested_primary_keys", {}),
            suggested_foreign_keys={
                k: [ColumnRelationship.from_dict(rel) for rel in v] 
                for k, v in data.get("suggested_foreign_keys", {}).items()
            },
            business_rules={
                k: [BusinessRule.from_dict(rule) for rule in v]
                for k, v in data.get("business_rules", {}).items()
            },
            join_recommendations=data.get("join_recommendations", [])
        )
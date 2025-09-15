"""Core profile models for data profiling."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..enums.profile_types import SemanticType
from .statistics import NamingPattern, ValuePattern, ValueStatistics, BusinessRule
from .relationships import ColumnRelationship, EntityRelationshipGraph


@dataclass
class ColumnProfile:
    """Profile information for a single column."""
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    completeness: Optional[float] = None
    uniqueness: Optional[float] = None
    entropy: Optional[float] = None
    value_distribution: Optional[Dict[Any, int]] = None
    top_distinct_values: Optional[List[Any]] = None
    # Relationship discovery fields
    semantic_type: Optional[SemanticType] = None
    naming_pattern: Optional[NamingPattern] = None
    value_pattern: Optional[ValuePattern] = None
    business_rules: List[BusinessRule] = field(default_factory=list)
    relationships: List[ColumnRelationship] = field(default_factory=list)
    # Enhanced value statistics for deeper analysis
    value_statistics: Optional[ValueStatistics] = None
    # Additional L4 fields
    percentiles: Optional[Dict[int, float]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "null_count": self.null_count,
            "null_percentage": self.null_percentage,
            "distinct_count": self.distinct_count,
            "distinct_percentage": self.distinct_percentage,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": self.mean_value,
            "median_value": self.median_value,
            "std_dev": self.std_dev,
            "completeness": self.completeness,
            "uniqueness": self.uniqueness,
            "entropy": self.entropy,
            "value_distribution": self.value_distribution,
            "top_distinct_values": self.top_distinct_values,
            "semantic_type": self.semantic_type.value if self.semantic_type else None,
            "naming_pattern": self.naming_pattern.to_dict() if self.naming_pattern else None,
            "value_pattern": self.value_pattern.to_dict() if self.value_pattern else None,
            "business_rules": [rule.to_dict() for rule in self.business_rules],
            "relationships": [rel.to_dict() for rel in self.relationships],
            "value_statistics": self.value_statistics.to_dict() if self.value_statistics else None,
            "percentiles": self.percentiles
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnProfile":
        """Create from dictionary for JSON deserialization."""
        return cls(
            column_name=data.get("column_name", ""),
            data_type=data.get("data_type", ""),
            null_count=data.get("null_count", 0),
            null_percentage=data.get("null_percentage", 0.0),
            distinct_count=data.get("distinct_count", 0),
            distinct_percentage=data.get("distinct_percentage", 0.0),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            mean_value=data.get("mean_value"),
            median_value=data.get("median_value"),
            std_dev=data.get("std_dev"),
            completeness=data.get("completeness"),
            uniqueness=data.get("uniqueness"),
            entropy=data.get("entropy"),
            value_distribution=data.get("value_distribution"),
            top_distinct_values=data.get("top_distinct_values"),
            semantic_type=SemanticType(data["semantic_type"]) if data.get("semantic_type") else None,
            naming_pattern=NamingPattern.from_dict(data["naming_pattern"]) if data.get("naming_pattern") else None,
            value_pattern=ValuePattern.from_dict(data["value_pattern"]) if data.get("value_pattern") else None,
            business_rules=[BusinessRule.from_dict(rule) for rule in data.get("business_rules", [])],
            relationships=[ColumnRelationship.from_dict(rel) for rel in data.get("relationships", [])],
            value_statistics=ValueStatistics.from_dict(data["value_statistics"]) if data.get("value_statistics") else None,
            percentiles=data.get("percentiles")
        )


@dataclass
class DatasetProfile:
    """Complete profile information for a dataset."""
    dataset_name: str
    row_count: int
    column_count: int
    profile_timestamp: str
    column_profiles: List[ColumnProfile]
    data_quality_score: Optional[float] = None
    correlations: Optional[Dict[str, Dict[str, float]]] = None
    anomalies: Optional[List[Dict[str, Any]]] = None
    recommendations: Optional[List[str]] = None
    null_count: Optional[int] = None
    duplicate_count: Optional[int] = None
    statistics: Optional[Dict[str, Any]] = None
    data_quality_issues: Optional[List[Dict[str, Any]]] = None
    # Relationship discovery fields
    entity_relationships: Optional[EntityRelationshipGraph] = None
    semantic_summary: Dict[str, Any] = field(default_factory=dict)
    business_glossary: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "dataset_name": self.dataset_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "profile_timestamp": self.profile_timestamp,
            "column_profiles": [col.to_dict() for col in self.column_profiles],
            "data_quality_score": self.data_quality_score,
            "correlations": self.correlations,
            "anomalies": self.anomalies,
            "recommendations": self.recommendations,
            "null_count": self.null_count,
            "duplicate_count": self.duplicate_count,
            "statistics": self.statistics,
            "data_quality_issues": self.data_quality_issues,
            "entity_relationships": self.entity_relationships if isinstance(self.entity_relationships, list) else (self.entity_relationships.to_dict() if self.entity_relationships else None),
            "semantic_summary": self.semantic_summary,
            "business_glossary": self.business_glossary
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetProfile":
        """Create from dictionary for JSON deserialization."""
        return cls(
            dataset_name=data.get("dataset_name", ""),
            row_count=data.get("row_count", 0),
            column_count=data.get("column_count", 0),
            profile_timestamp=data.get("profile_timestamp", ""),
            column_profiles=[ColumnProfile.from_dict(col) for col in data.get("column_profiles", [])],
            data_quality_score=data.get("data_quality_score"),
            correlations=data.get("correlations"),
            anomalies=data.get("anomalies"),
            recommendations=data.get("recommendations"),
            null_count=data.get("null_count"),
            duplicate_count=data.get("duplicate_count"),
            statistics=data.get("statistics"),
            data_quality_issues=data.get("data_quality_issues"),
            entity_relationships=data.get("entity_relationships") if isinstance(data.get("entity_relationships"), list) else (EntityRelationshipGraph.from_dict(data["entity_relationships"]) if data.get("entity_relationships") else None),
            semantic_summary=data.get("semantic_summary", {}),
            business_glossary=data.get("business_glossary", {})
        )
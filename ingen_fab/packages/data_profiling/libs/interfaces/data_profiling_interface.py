"""Interface for data profiling implementations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class ProfileType(Enum):
    """Types of data profiles that can be generated."""
    BASIC = "basic"  # Row count, column count, basic stats
    STATISTICAL = "statistical"  # Mean, median, std dev, percentiles
    DATA_QUALITY = "data_quality"  # Nulls, duplicates, patterns
    DISTRIBUTION = "distribution"  # Value distributions, histograms
    CORRELATION = "correlation"  # Column correlations
    RELATIONSHIP = "relationship"  # Entity relationships and joins
    FULL = "full"  # All profile types


class SemanticType(Enum):
    """Semantic types for columns based on content analysis."""
    IDENTIFIER = "identifier"  # Primary keys, unique identifiers
    FOREIGN_KEY = "foreign_key"  # References to other tables
    MEASURE = "measure"  # Numeric values for aggregation
    DIMENSION = "dimension"  # Categorical values for grouping
    TIMESTAMP = "timestamp"  # Date/time values
    STATUS = "status"  # State or status indicators
    DESCRIPTION = "description"  # Text descriptions or comments
    HIERARCHY = "hierarchy"  # Parent-child relationships
    UNKNOWN = "unknown"  # Cannot determine semantic type


class ValueFormat(Enum):
    """Detected value formats in columns."""
    UUID = "uuid"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    IP_ADDRESS = "ip_address"
    JSON = "json"
    XML = "xml"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    COUNTRY_CODE = "country_code"
    POSTAL_CODE = "postal_code"
    UNKNOWN = "unknown"


class RelationshipType(Enum):
    """Types of relationships between tables."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    HIERARCHY = "hierarchy"


@dataclass
class NamingPattern:
    """Analysis of column naming patterns."""
    is_id_column: bool = False
    is_foreign_key: bool = False
    is_timestamp: bool = False
    is_status_flag: bool = False
    is_measurement: bool = False
    detected_patterns: List[str] = field(default_factory=list)
    naming_confidence: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "is_id_column": self.is_id_column,
            "is_foreign_key": self.is_foreign_key,
            "is_timestamp": self.is_timestamp,
            "is_status_flag": self.is_status_flag,
            "is_measurement": self.is_measurement,
            "detected_patterns": self.detected_patterns,
            "naming_confidence": self.naming_confidence
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NamingPattern":
        """Create from dictionary for JSON deserialization."""
        return cls(
            is_id_column=data.get("is_id_column", False),
            is_foreign_key=data.get("is_foreign_key", False),
            is_timestamp=data.get("is_timestamp", False),
            is_status_flag=data.get("is_status_flag", False),
            is_measurement=data.get("is_measurement", False),
            detected_patterns=data.get("detected_patterns", []),
            naming_confidence=data.get("naming_confidence", 0.0)
        )


@dataclass
class ValuePattern:
    """Analysis of value patterns and formats."""
    detected_format: ValueFormat = ValueFormat.UNKNOWN
    format_confidence: float = 0.0
    regex_patterns: List[str] = field(default_factory=list)
    sample_values: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "detected_format": self.detected_format.value,
            "format_confidence": self.format_confidence,
            "regex_patterns": self.regex_patterns,
            "sample_values": self.sample_values
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValuePattern":
        """Create from dictionary for JSON deserialization."""
        return cls(
            detected_format=ValueFormat(data.get("detected_format", "unknown")),
            format_confidence=data.get("format_confidence", 0.0),
            regex_patterns=data.get("regex_patterns", []),
            sample_values=data.get("sample_values", [])
        )


@dataclass
class BusinessRule:
    """Detected business rule for a column."""
    rule_type: str
    rule_description: str
    confidence: float
    rule_expression: Optional[str] = None
    violation_count: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "rule_type": self.rule_type,
            "rule_description": self.rule_description,
            "confidence": self.confidence,
            "rule_expression": self.rule_expression,
            "violation_count": self.violation_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BusinessRule":
        """Create from dictionary for JSON deserialization."""
        return cls(
            rule_type=data.get("rule_type", ""),
            rule_description=data.get("rule_description", ""),
            confidence=data.get("confidence", 0.0),
            rule_expression=data.get("rule_expression"),
            violation_count=data.get("violation_count")
        )


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
class ValueStatistics:
    """Comprehensive value statistics for relationship discovery."""
    value_hash_signature: Optional[str] = None  # Hash of sorted distinct values for quick comparison
    value_count_distribution: Optional[Dict[int, int]] = None  # {count: frequency} for cardinality analysis
    selectivity: Optional[float] = None  # distinct_count / row_count
    is_unique_key: bool = False  # True if all values are unique
    is_constant: bool = False  # True if only one distinct value
    dominant_value: Optional[Any] = None  # Most frequent value
    dominant_value_ratio: Optional[float] = None  # Frequency of most common value
    value_length_stats: Optional[Dict[str, float]] = None  # Min/max/avg length for strings
    numeric_distribution: Optional[Dict[str, float]] = None  # Quartiles for numeric columns
    sample_values: Optional[List[Any]] = None  # Random sample of non-null values (for testing overlaps)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "value_hash_signature": self.value_hash_signature,
            "value_count_distribution": self.value_count_distribution,
            "selectivity": self.selectivity,
            "is_unique_key": self.is_unique_key,
            "is_constant": self.is_constant,
            "dominant_value": self.dominant_value,
            "dominant_value_ratio": self.dominant_value_ratio,
            "value_length_stats": self.value_length_stats,
            "numeric_distribution": self.numeric_distribution,
            "sample_values": self.sample_values
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValueStatistics":
        """Create from dictionary for JSON deserialization."""
        return cls(
            value_hash_signature=data.get("value_hash_signature"),
            value_count_distribution=data.get("value_count_distribution"),
            selectivity=data.get("selectivity"),
            is_unique_key=data.get("is_unique_key", False),
            is_constant=data.get("is_constant", False),
            dominant_value=data.get("dominant_value"),
            dominant_value_ratio=data.get("dominant_value_ratio"),
            value_length_stats=data.get("value_length_stats"),
            numeric_distribution=data.get("numeric_distribution"),
            sample_values=data.get("sample_values")
        )


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
    # New relationship discovery fields
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
    # New relationship discovery fields
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
            "entity_relationships": self.entity_relationships.to_dict() if self.entity_relationships else None,
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
            entity_relationships=EntityRelationshipGraph.from_dict(data["entity_relationships"]) if data.get("entity_relationships") else None,
            semantic_summary=data.get("semantic_summary", {}),
            business_glossary=data.get("business_glossary", {})
        )


class DataProfilingInterface(ABC):
    """Abstract interface for data profiling implementations."""

    @abstractmethod
    def profile_dataset(
        self,
        dataset: Any,
        profile_type: ProfileType = ProfileType.BASIC,
        columns: Optional[List[str]] = None,
        sample_size: Optional[float] = None
    ) -> DatasetProfile:
        """
        Profile a dataset to generate statistics and quality metrics.
        
        Args:
            dataset: The dataset to profile (DataFrame, table name, or path)
            profile_type: Type of profiling to perform
            columns: Specific columns to profile (None = all columns)
            sample_size: Fraction of data to sample (None = full dataset)
            
        Returns:
            DatasetProfile containing profiling results
        """
        pass

    @abstractmethod
    def profile_column(
        self,
        dataset: Any,
        column_name: str,
        profile_type: ProfileType = ProfileType.BASIC
    ) -> ColumnProfile:
        """
        Profile a single column in detail.
        
        Args:
            dataset: The dataset containing the column
            column_name: Name of the column to profile
            profile_type: Type of profiling to perform
            
        Returns:
            ColumnProfile containing column-specific profiling results
        """
        pass

    @abstractmethod
    def compare_profiles(
        self,
        profile1: DatasetProfile,
        profile2: DatasetProfile
    ) -> Dict[str, Any]:
        """
        Compare two dataset profiles to identify changes.
        
        Args:
            profile1: First dataset profile
            profile2: Second dataset profile
            
        Returns:
            Dictionary containing comparison results and drift metrics
        """
        pass

    @abstractmethod
    def generate_quality_report(
        self,
        profile: DatasetProfile,
        output_format: str = "html"
    ) -> str:
        """
        Generate a formatted quality report from a profile.
        
        Args:
            profile: Dataset profile to report on
            output_format: Format for the report (html, json, markdown)
            
        Returns:
            Formatted report as string
        """
        pass

    @abstractmethod
    def suggest_data_quality_rules(
        self,
        profile: DatasetProfile
    ) -> List[Dict[str, Any]]:
        """
        Suggest data quality rules based on profiling results.
        
        Args:
            profile: Dataset profile to analyze
            
        Returns:
            List of suggested data quality rules
        """
        pass

    @abstractmethod
    def validate_against_rules(
        self,
        dataset: Any,
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Validate a dataset against a set of data quality rules.
        
        Args:
            dataset: Dataset to validate
            rules: List of data quality rules to apply
            
        Returns:
            Validation results including pass/fail status and violations
        """
        pass
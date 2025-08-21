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


@dataclass
class ValuePattern:
    """Analysis of value patterns and formats."""
    detected_format: ValueFormat = ValueFormat.UNKNOWN
    format_confidence: float = 0.0
    regex_patterns: List[str] = field(default_factory=list)
    sample_values: List[str] = field(default_factory=list)


@dataclass
class BusinessRule:
    """Detected business rule for a column."""
    rule_type: str
    rule_description: str
    confidence: float
    rule_expression: Optional[str] = None
    violation_count: Optional[int] = None


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


@dataclass
class EntityRelationshipGraph:
    """Complete entity relationship graph for a dataset."""
    entities: List[str] = field(default_factory=list)  # Table names
    relationships: List[TableRelationship] = field(default_factory=list)
    suggested_primary_keys: Dict[str, List[str]] = field(default_factory=dict)
    suggested_foreign_keys: Dict[str, List[ColumnRelationship]] = field(default_factory=dict)
    business_rules: Dict[str, List[BusinessRule]] = field(default_factory=dict)
    join_recommendations: List[Dict[str, Any]] = field(default_factory=list)


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
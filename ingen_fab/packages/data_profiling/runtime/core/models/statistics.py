"""Statistical models for data profiling."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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


from ..enums.profile_types import ValueFormat


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
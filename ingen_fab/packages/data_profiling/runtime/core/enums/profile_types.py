"""Enums for data profiling types and classifications."""

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


class ProfileGrain(Enum):
    """Profile versioning granularity control."""
    DAILY = "daily"  # One profile per table per day (overwrites same-day profiles)
    CONTINUOUS = "continuous"  # Keep all profile versions (no overwriting)


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


class ScanLevel(Enum):
    """Scan levels for tiered profiling."""
    LEVEL_1_DISCOVERY = 1
    LEVEL_2_SCHEMA = 2
    LEVEL_3_PROFILE = 3
    LEVEL_4_ADVANCED = 4
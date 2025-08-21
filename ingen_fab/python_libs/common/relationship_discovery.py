"""
Relationship Discovery Engine

This module provides functionality for analyzing column and table relationships,
semantic types, and business rules to help understand data structure and usage.
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import Counter

from ..interfaces.data_profiling_interface import (
    SemanticType, ValueFormat, RelationshipType, NamingPattern, ValuePattern, 
    BusinessRule, ColumnRelationship, TableRelationship, EntityRelationshipGraph
)


class ColumnNamingAnalyzer:
    """Analyzes column names to detect patterns and semantic meaning."""
    
    # Naming pattern definitions
    ID_PATTERNS = [
        r"^id$", r"_id$", r"^.*_id$", r"^key$", r"_key$", r"^.*_key$",
        r"^pk$", r"_pk$", r"^.*_pk$", r"^identifier$"
    ]
    
    FOREIGN_KEY_PATTERNS = [
        r"^customer_id$", r"^user_id$", r"^order_id$", r"^product_id$",
        r"^account_id$", r"^invoice_id$", r"^transaction_id$", r"^payment_id$",
        r"^.*_fk$", r"^fk_.*", r"^ref_.*", r".*_ref$"
    ]
    
    TIMESTAMP_PATTERNS = [
        r".*_date$", r".*_time$", r".*_timestamp$", r"^created.*", r"^updated.*",
        r"^modified.*", r".*_at$", r"^date_.*", r"^time_.*", r"^ts_.*",
        r"^deleted.*", r"^archived.*", r".*_on$"
    ]
    
    STATUS_PATTERNS = [
        r".*status$", r".*state$", r".*flag$", r".*_yn$", r"^is_.*",
        r"^has_.*", r"^can_.*", r".*_ind$", r".*_indicator$", r"active",
        r"enabled", r"disabled", r"valid", r"invalid"
    ]
    
    MEASUREMENT_PATTERNS = [
        r".*amount$", r".*total$", r".*count$", r".*quantity$", r".*_qty$",
        r".*price$", r".*cost$", r".*value$", r".*_val$", r".*rate$",
        r".*_pct$", r".*percentage$", r".*weight$", r".*length$", r".*width$",
        r".*height$", r".*size$", r".*volume$", r".*score$"
    ]
    
    def analyze_column_name(self, column_name: str) -> NamingPattern:
        """Analyze a column name for patterns."""
        name_lower = column_name.lower().strip()
        detected_patterns = []
        
        # Check each pattern type
        is_id = self._matches_patterns(name_lower, self.ID_PATTERNS)
        is_fk = self._matches_patterns(name_lower, self.FOREIGN_KEY_PATTERNS)
        is_timestamp = self._matches_patterns(name_lower, self.TIMESTAMP_PATTERNS)
        is_status = self._matches_patterns(name_lower, self.STATUS_PATTERNS)
        is_measurement = self._matches_patterns(name_lower, self.MEASUREMENT_PATTERNS)
        
        if is_id:
            detected_patterns.append("identifier")
        if is_fk:
            detected_patterns.append("foreign_key")
        if is_timestamp:
            detected_patterns.append("timestamp")
        if is_status:
            detected_patterns.append("status")
        if is_measurement:
            detected_patterns.append("measurement")
        
        # Calculate confidence based on pattern strength
        confidence = self._calculate_naming_confidence(name_lower, detected_patterns)
        
        return NamingPattern(
            is_id_column=is_id,
            is_foreign_key=is_fk,
            is_timestamp=is_timestamp,
            is_status_flag=is_status,
            is_measurement=is_measurement,
            detected_patterns=detected_patterns,
            naming_confidence=confidence
        )
    
    def _matches_patterns(self, name: str, patterns: List[str]) -> bool:
        """Check if name matches any pattern in the list."""
        return any(re.match(pattern, name, re.IGNORECASE) for pattern in patterns)
    
    def _calculate_naming_confidence(self, name: str, patterns: List[str]) -> float:
        """Calculate confidence score for naming pattern detection."""
        if not patterns:
            return 0.0
        
        # Base confidence from pattern matches
        base_confidence = len(patterns) * 0.2
        
        # Boost confidence for exact matches on common patterns
        exact_matches = ["id", "key", "status", "date", "time", "amount", "total"]
        if name in exact_matches:
            base_confidence += 0.4
        
        # Boost for compound names that are very specific
        if "_" in name and len(name.split("_")) >= 2:
            base_confidence += 0.2
        
        return min(base_confidence, 1.0)


class ValueFormatDetector:
    """Detects value formats in column data."""
    
    # Format patterns
    FORMAT_PATTERNS = {
        ValueFormat.UUID: r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        ValueFormat.EMAIL: r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        ValueFormat.PHONE: r"^[\+]?[1-9][\d]{0,15}$|^[\(]?[\d]{3}[\)]?[-\s\.]?[\d]{3}[-\s\.]?[\d]{4}$",
        ValueFormat.URL: r"^https?://[^\s/$.?#].[^\s]*$",
        ValueFormat.IP_ADDRESS: r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
        ValueFormat.JSON: r"^\s*[\{\[].*[\}\]]\s*$",
        ValueFormat.CURRENCY: r"^[\$\£\€\¥]?[\d,]+\.?\d{0,2}$",
        ValueFormat.PERCENTAGE: r"^\d+\.?\d*%$",
        ValueFormat.COUNTRY_CODE: r"^[A-Z]{2,3}$",
        ValueFormat.POSTAL_CODE: r"^[A-Z0-9\s-]{3,12}$"
    }
    
    def detect_format(self, sample_values: List[str], min_match_ratio: float = 0.8) -> ValuePattern:
        """Detect format from sample values."""
        if not sample_values:
            return ValuePattern()
        
        # Clean and filter non-null values
        clean_values = [str(v).strip() for v in sample_values if v is not None and str(v).strip()]
        if not clean_values:
            return ValuePattern()
        
        best_format = ValueFormat.UNKNOWN
        best_confidence = 0.0
        matching_patterns = []
        
        # Test each format pattern
        for value_format, pattern in self.FORMAT_PATTERNS.items():
            matches = sum(1 for value in clean_values if re.match(pattern, value, re.IGNORECASE))
            match_ratio = matches / len(clean_values)
            
            if match_ratio >= min_match_ratio and match_ratio > best_confidence:
                best_format = value_format
                best_confidence = match_ratio
                matching_patterns.append(pattern)
        
        return ValuePattern(
            detected_format=best_format,
            format_confidence=best_confidence,
            regex_patterns=matching_patterns,
            sample_values=clean_values[:10]  # Keep first 10 for reference
        )


class SemanticTypeClassifier:
    """Classifies columns into semantic types based on multiple signals."""
    
    def classify_column(
        self, 
        column_name: str,
        data_type: str,
        null_percentage: float,
        distinct_percentage: float,
        uniqueness: float,
        naming_pattern: NamingPattern,
        value_pattern: ValuePattern,
        sample_values: Optional[List[Any]] = None
    ) -> SemanticType:
        """Classify column semantic type using multiple signals."""
        
        # Primary key identification
        if (naming_pattern.is_id_column and 
            uniqueness > 0.95 and 
            null_percentage < 0.05):
            return SemanticType.IDENTIFIER
        
        # Foreign key identification
        if (naming_pattern.is_foreign_key or 
            (naming_pattern.is_id_column and uniqueness < 0.9)):
            return SemanticType.FOREIGN_KEY
        
        # Timestamp identification
        if (naming_pattern.is_timestamp or 
            data_type.lower() in ['date', 'timestamp', 'datetime']):
            return SemanticType.TIMESTAMP
        
        # Status/flag identification
        if (naming_pattern.is_status_flag or 
            (distinct_percentage < 0.1 and data_type.lower() in ['boolean', 'string'])):
            return SemanticType.STATUS
        
        # Measure identification
        if (naming_pattern.is_measurement and 
            data_type.lower() in ['int', 'bigint', 'float', 'double', 'decimal']):
            return SemanticType.MEASURE
        
        # Dimension identification (categorical with reasonable cardinality)
        if (data_type.lower() in ['string', 'varchar'] and 
            0.01 < distinct_percentage < 0.5):
            return SemanticType.DIMENSION
        
        # Description identification (high-cardinality text)
        if (data_type.lower() in ['string', 'varchar', 'text'] and 
            distinct_percentage > 0.7 and 
            sample_values and 
            any(len(str(v)) > 50 for v in sample_values[:10] if v)):
            return SemanticType.DESCRIPTION
        
        # Hierarchy identification (parent-child patterns)
        if (column_name.lower().endswith('_parent') or 
            column_name.lower().startswith('parent_') or
            'hierarchy' in column_name.lower()):
            return SemanticType.HIERARCHY
        
        return SemanticType.UNKNOWN


class BusinessRuleDetector:
    """Detects business rules from data patterns."""
    
    def detect_rules(
        self,
        column_name: str,
        data_type: str,
        min_value: Any,
        max_value: Any,
        distinct_values: List[Any],
        null_count: int,
        total_count: int,
        semantic_type: SemanticType
    ) -> List[BusinessRule]:
        """Detect business rules for a column."""
        rules = []
        
        # Enum constraint detection
        if len(distinct_values) <= 20 and len(distinct_values) > 1:
            rules.append(BusinessRule(
                rule_type="enum_constraint",
                rule_description=f"Column values should be one of: {', '.join(map(str, distinct_values))}",
                confidence=0.9,
                rule_expression=f"{column_name} IN ({', '.join(repr(v) for v in distinct_values)})"
            ))
        
        # Not null constraint
        if null_count == 0:
            rules.append(BusinessRule(
                rule_type="not_null_constraint",
                rule_description=f"Column {column_name} should not be null",
                confidence=1.0,
                rule_expression=f"{column_name} IS NOT NULL"
            ))
        
        # Range constraints for numeric columns
        if (data_type.lower() in ['int', 'bigint', 'float', 'double', 'decimal'] and 
            min_value is not None and max_value is not None):
            rules.append(BusinessRule(
                rule_type="range_constraint",
                rule_description=f"Column {column_name} should be between {min_value} and {max_value}",
                confidence=0.8,
                rule_expression=f"{column_name} BETWEEN {min_value} AND {max_value}"
            ))
        
        # Uniqueness constraint
        if len(distinct_values) == total_count and null_count == 0:
            rules.append(BusinessRule(
                rule_type="unique_constraint",
                rule_description=f"Column {column_name} should have unique values",
                confidence=0.95,
                rule_expression=f"DISTINCT {column_name}"
            ))
        
        # Positive values constraint for measures
        if (semantic_type == SemanticType.MEASURE and 
            min_value is not None and min_value >= 0):
            rules.append(BusinessRule(
                rule_type="positive_constraint",
                rule_description=f"Column {column_name} should be positive or zero",
                confidence=0.85,
                rule_expression=f"{column_name} >= 0"
            ))
        
        return rules


class ReferentialIntegrityAnalyzer:
    """Analyzes referential integrity between tables."""
    
    def analyze_relationship(
        self,
        source_table: str,
        source_column: str,
        source_values: List[Any],
        target_table: str, 
        target_column: str,
        target_values: List[Any]
    ) -> ColumnRelationship:
        """Analyze relationship between two columns."""
        
        # Convert to sets for analysis
        source_set = set(v for v in source_values if v is not None)
        target_set = set(v for v in target_values if v is not None)
        
        if not source_set or not target_set:
            return ColumnRelationship(
                source_table=source_table,
                source_column=source_column,
                target_table=target_table,
                target_column=target_column,
                relationship_type=RelationshipType.MANY_TO_MANY,
                confidence_score=0.0,
                overlap_percentage=0.0,
                referential_integrity_score=0.0,
                suggested_join_condition=f"{source_table}.{source_column} = {target_table}.{target_column}"
            )
        
        # Calculate overlap
        overlap = source_set & target_set
        overlap_percentage = len(overlap) / len(source_set) if source_set else 0.0
        
        # Calculate referential integrity
        foreign_key_coverage = len(overlap) / len(source_set) if source_set else 0.0
        
        # Determine relationship type
        relationship_type = self._determine_relationship_type(source_values, target_values, overlap)
        
        # Calculate confidence score
        confidence_score = self._calculate_relationship_confidence(
            overlap_percentage, foreign_key_coverage, relationship_type
        )
        
        return ColumnRelationship(
            source_table=source_table,
            source_column=source_column,
            target_table=target_table,
            target_column=target_column,
            relationship_type=relationship_type,
            confidence_score=confidence_score,
            overlap_percentage=overlap_percentage,
            referential_integrity_score=foreign_key_coverage,
            suggested_join_condition=f"{source_table}.{source_column} = {target_table}.{target_column}"
        )
    
    def _determine_relationship_type(
        self,
        source_values: List[Any],
        target_values: List[Any],
        overlap: set
    ) -> RelationshipType:
        """Determine the type of relationship between columns."""
        
        source_unique = len(set(source_values))
        target_unique = len(set(target_values))
        source_total = len(source_values)
        target_total = len(target_values)
        
        # One-to-one: both sides unique and similar counts
        if (source_unique == source_total and 
            target_unique == target_total and
            abs(source_total - target_total) / max(source_total, target_total) < 0.1):
            return RelationshipType.ONE_TO_ONE
        
        # One-to-many: target unique, source has duplicates
        if target_unique == target_total and source_unique < source_total:
            return RelationshipType.ONE_TO_MANY
        
        # Many-to-one: source unique, target has duplicates  
        if source_unique == source_total and target_unique < target_total:
            return RelationshipType.MANY_TO_ONE
        
        # Many-to-many: both have duplicates
        return RelationshipType.MANY_TO_MANY
    
    def _calculate_relationship_confidence(
        self,
        overlap_percentage: float,
        referential_integrity: float,
        relationship_type: RelationshipType
    ) -> float:
        """Calculate confidence score for a relationship."""
        base_score = (overlap_percentage + referential_integrity) / 2
        
        # Boost confidence for cleaner relationship types
        if relationship_type in [RelationshipType.ONE_TO_ONE, RelationshipType.ONE_TO_MANY]:
            base_score *= 1.2
        
        # High overlap boosts confidence
        if overlap_percentage > 0.8:
            base_score *= 1.1
        
        return min(base_score, 1.0)


class JoinRecommendationEngine:
    """Generates join recommendations based on relationship analysis."""
    
    def generate_recommendations(
        self,
        relationships: List[ColumnRelationship],
        min_confidence: float = 0.7
    ) -> List[Dict[str, Any]]:
        """Generate join recommendations from relationships."""
        
        recommendations = []
        
        # Group relationships by table pairs
        table_pairs = {}
        for rel in relationships:
            if rel.confidence_score >= min_confidence:
                key = (rel.source_table, rel.target_table)
                if key not in table_pairs:
                    table_pairs[key] = []
                table_pairs[key].append(rel)
        
        # Generate recommendations for each table pair
        for (source_table, target_table), rels in table_pairs.items():
            # Sort by confidence
            rels.sort(key=lambda x: x.confidence_score, reverse=True)
            best_rel = rels[0]
            
            recommendation = {
                "source_table": source_table,
                "target_table": target_table,
                "relationship_type": best_rel.relationship_type.value,
                "confidence_score": best_rel.confidence_score,
                "primary_join_condition": best_rel.suggested_join_condition,
                "alternative_joins": [r.suggested_join_condition for r in rels[1:3]],  # Top 3
                "referential_integrity": best_rel.referential_integrity_score,
                "data_overlap": best_rel.overlap_percentage,
                "join_sql": self._generate_join_sql(source_table, target_table, best_rel)
            }
            
            recommendations.append(recommendation)
        
        return sorted(recommendations, key=lambda x: x["confidence_score"], reverse=True)
    
    def _generate_join_sql(
        self,
        source_table: str,
        target_table: str,
        relationship: ColumnRelationship
    ) -> str:
        """Generate SQL join statement."""
        
        join_type = "INNER"
        if relationship.referential_integrity_score < 0.9:
            join_type = "LEFT"
        
        return f"""SELECT *
FROM {source_table} s
{join_type} JOIN {target_table} t
  ON {relationship.suggested_join_condition.replace(source_table, 's').replace(target_table, 't')}"""
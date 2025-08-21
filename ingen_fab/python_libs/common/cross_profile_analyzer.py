"""
Cross-Profile Relationship Analyzer

This module analyzes relationships between tables using their profile data,
focusing on value-based discovery rather than just naming conventions.
"""

from typing import List, Dict, Tuple, Optional, Any, Set
from dataclasses import dataclass
import hashlib
import json
from collections import defaultdict

from ingen_fab.python_libs.interfaces.data_profiling_interface import (
    DatasetProfile,
    ColumnProfile,
    ColumnRelationship,
    RelationshipType,
    ValueStatistics,
)


@dataclass
class RelationshipCandidate:
    """A potential relationship between two columns."""
    source_table: str
    source_column: ColumnProfile
    target_table: str
    target_column: ColumnProfile
    value_overlap_score: float = 0.0
    cardinality_score: float = 0.0
    data_type_match: bool = False
    name_similarity_score: float = 0.0
    statistical_correlation: float = 0.0
    confidence: float = 0.0
    relationship_type: Optional[RelationshipType] = None
    evidence: List[str] = None


class CrossProfileAnalyzer:
    """Analyzes relationships across multiple dataset profiles."""
    
    def __init__(self, min_confidence: float = 0.5):
        """
        Initialize the analyzer.
        
        Args:
            min_confidence: Minimum confidence threshold for reporting relationships
        """
        self.min_confidence = min_confidence
        self.candidates: List[RelationshipCandidate] = []
    
    def analyze_profiles(
        self, 
        profiles: Dict[str, DatasetProfile]
    ) -> List[RelationshipCandidate]:
        """
        Analyze relationships across multiple dataset profiles.
        
        Args:
            profiles: Dictionary of table_name -> DatasetProfile
            
        Returns:
            List of discovered relationship candidates
        """
        self.candidates = []
        
        # Build column index for efficient lookups
        column_index = self._build_column_index(profiles)
        
        # Phase 1: Find exact value matches (highest confidence)
        self._find_exact_value_matches(column_index)
        
        # Phase 2: Find statistical relationships
        self._find_statistical_relationships(column_index)
        
        # Phase 3: Find cardinality-based relationships
        self._find_cardinality_relationships(column_index)
        
        # Phase 4: Find pattern-based relationships
        self._find_pattern_relationships(column_index)
        
        # Score and filter candidates
        scored_candidates = self._score_candidates(self.candidates)
        
        return [c for c in scored_candidates if c.confidence >= self.min_confidence]
    
    def _build_column_index(
        self, 
        profiles: Dict[str, DatasetProfile]
    ) -> Dict[str, List[Tuple[str, ColumnProfile]]]:
        """Build an index of columns by various characteristics."""
        
        index = {
            'by_type': defaultdict(list),
            'by_cardinality': defaultdict(list),
            'by_hash': defaultdict(list),
            'by_selectivity': defaultdict(list),
            'all_columns': []
        }
        
        for table_name, profile in profiles.items():
            for col in profile.column_profiles:
                entry = (table_name, col)
                
                # Index by data type
                index['by_type'][col.data_type].append(entry)
                
                # Index by cardinality bucket
                if profile.row_count > 0:
                    selectivity = col.distinct_count / profile.row_count
                    if selectivity == 1.0:
                        index['by_cardinality']['unique'].append(entry)
                    elif selectivity > 0.9:
                        index['by_cardinality']['high'].append(entry)
                    elif selectivity > 0.1:
                        index['by_cardinality']['medium'].append(entry)
                    else:
                        index['by_cardinality']['low'].append(entry)
                
                # Index by value hash (if available)
                if col.value_statistics and col.value_statistics.value_hash_signature:
                    index['by_hash'][col.value_statistics.value_hash_signature].append(entry)
                
                # Store all columns
                index['all_columns'].append(entry)
        
        return index
    
    def _find_exact_value_matches(self, column_index: Dict):
        """Find columns with exact matching values using hash signatures."""
        
        # Check columns with same hash signatures
        for hash_sig, columns in column_index['by_hash'].items():
            if len(columns) < 2:
                continue
            
            # Compare each pair
            for i in range(len(columns)):
                for j in range(i + 1, len(columns)):
                    table1, col1 = columns[i]
                    table2, col2 = columns[j]
                    
                    # Skip same table
                    if table1 == table2:
                        continue
                    
                    candidate = RelationshipCandidate(
                        source_table=table1,
                        source_column=col1,
                        target_table=table2,
                        target_column=col2,
                        value_overlap_score=1.0,  # Exact match
                        data_type_match=(col1.data_type == col2.data_type),
                        evidence=["Exact value match (hash signature)"]
                    )
                    
                    self.candidates.append(candidate)
    
    def _find_statistical_relationships(self, column_index: Dict):
        """Find statistical correlations between numeric columns."""
        
        numeric_types = ['IntegerType', 'LongType', 'FloatType', 'DoubleType', 'DecimalType']
        
        for dtype in numeric_types:
            columns = column_index['by_type'].get(dtype, [])
            
            for i in range(len(columns)):
                for j in range(i + 1, len(columns)):
                    table1, col1 = columns[i]
                    table2, col2 = columns[j]
                    
                    # Skip same table
                    if table1 == table2:
                        continue
                    
                    # Check if numeric distributions are similar
                    if (col1.mean_value and col2.mean_value and 
                        col1.std_dev and col2.std_dev):
                        
                        # Calculate similarity of distributions
                        mean_ratio = min(col1.mean_value, col2.mean_value) / max(col1.mean_value, col2.mean_value) if max(col1.mean_value, col2.mean_value) != 0 else 0
                        std_ratio = min(col1.std_dev, col2.std_dev) / max(col1.std_dev, col2.std_dev) if max(col1.std_dev, col2.std_dev) != 0 else 0
                        
                        if mean_ratio > 0.8 and std_ratio > 0.8:
                            candidate = RelationshipCandidate(
                                source_table=table1,
                                source_column=col1,
                                target_table=table2,
                                target_column=col2,
                                statistical_correlation=(mean_ratio + std_ratio) / 2,
                                data_type_match=True,
                                evidence=["Similar numeric distribution"]
                            )
                            
                            self.candidates.append(candidate)
    
    def _find_cardinality_relationships(self, column_index: Dict):
        """Find parent-child relationships based on cardinality."""
        
        # Look for unique/high cardinality columns (potential primary keys)
        potential_parents = (
            column_index['by_cardinality'].get('unique', []) + 
            column_index['by_cardinality'].get('high', [])
        )
        
        # Look for lower cardinality columns (potential foreign keys)
        potential_children = (
            column_index['by_cardinality'].get('medium', []) + 
            column_index['by_cardinality'].get('low', [])
        )
        
        for parent_table, parent_col in potential_parents:
            for child_table, child_col in potential_children:
                # Skip same table
                if parent_table == child_table:
                    continue
                
                # Check if cardinalities suggest parent-child relationship
                if parent_col.distinct_count >= child_col.distinct_count:
                    # Check value overlap if sample values are available
                    overlap_score = self._calculate_value_overlap(parent_col, child_col)
                    
                    if overlap_score > 0.3:  # Some overlap detected
                        candidate = RelationshipCandidate(
                            source_table=child_table,
                            source_column=child_col,
                            target_table=parent_table,
                            target_column=parent_col,
                            value_overlap_score=overlap_score,
                            cardinality_score=child_col.distinct_count / parent_col.distinct_count if parent_col.distinct_count > 0 else 0,
                            data_type_match=(parent_col.data_type == child_col.data_type),
                            evidence=[f"Cardinality suggests parent-child ({parent_col.distinct_count} -> {child_col.distinct_count})"]
                        )
                        
                        self.candidates.append(candidate)
        
        # Enhanced check for all compatible numeric columns (ignoring cardinality classification)
        self._find_enhanced_numeric_relationships(column_index)
    
    def _find_enhanced_numeric_relationships(self, column_index: Dict):
        """Find numeric relationships with enhanced range and partial overlap detection."""
        
        # Check all compatible column pairs regardless of cardinality classification
        for table1, col1 in column_index['all_columns']:
            for table2, col2 in column_index['all_columns']:
                # Skip same table
                if table1 == table2:
                    continue
                    
                # Must be compatible numeric types
                if (col1.data_type in ['IntegerType', 'LongType', 'IntegerType()', 'LongType()'] and
                    col2.data_type in ['IntegerType', 'LongType', 'IntegerType()', 'LongType()']):
                    
                    # Check value overlap and range compatibility
                    overlap_score = self._calculate_value_overlap(col1, col2)
                    range_overlap = self._check_range_compatibility(col1, col2)
                    
                    # More lenient thresholds for detecting relationships with data quality issues
                    if overlap_score > 0.01 or range_overlap > 0.1:  # Even 1% overlap or 10% range compatibility
                        # Determine direction - lower distinct count is typically the parent (dimension table)
                        if col1.distinct_count <= col2.distinct_count:
                            parent_table, parent_col = table1, col1
                            child_table, child_col = table2, col2
                            cardinality_ratio = col1.distinct_count / col2.distinct_count if col2.distinct_count > 0 else 0
                        else:
                            parent_table, parent_col = table2, col2  
                            child_table, child_col = table1, col1
                            cardinality_ratio = col2.distinct_count / col1.distinct_count if col1.distinct_count > 0 else 0
                        
                        evidence = []
                        if overlap_score > 0:
                            evidence.append(f"Value overlap: {overlap_score:.3f}")
                        if range_overlap > 0:
                            evidence.append(f"Range compatibility: {range_overlap:.3f}")
                        evidence.append(f"Cardinality ratio: {cardinality_ratio:.3f}")
                        
                        candidate = RelationshipCandidate(
                            source_table=child_table,
                            source_column=child_col,
                            target_table=parent_table,
                            target_column=parent_col,
                            value_overlap_score=max(overlap_score, range_overlap),
                            cardinality_score=cardinality_ratio,
                            data_type_match=True,
                            evidence=evidence
                        )
                        
                        self.candidates.append(candidate)
    
    def _check_range_compatibility(self, col1: ColumnProfile, col2: ColumnProfile) -> float:
        """Check if numeric ranges overlap or are compatible for FK relationships."""
        
        # Need min/max values for range comparison
        if not (col1.min_value and col1.max_value and col2.min_value and col2.max_value):
            return 0.0
        
        try:
            # Convert to numeric values
            min1, max1 = float(col1.min_value), float(col1.max_value)
            min2, max2 = float(col2.min_value), float(col2.max_value)
            
            # Calculate range overlap
            overlap_start = max(min1, min2)
            overlap_end = min(max1, max2)
            
            if overlap_start <= overlap_end:
                # There is overlap
                overlap_size = overlap_end - overlap_start
                
                # For FK detection, focus on how much of the smaller range overlaps
                range1_size = max1 - min1
                range2_size = max2 - min2
                
                if range1_size <= range2_size:
                    # col1 is smaller, check what portion of it overlaps
                    return overlap_size / range1_size if range1_size > 0 else 0.0
                else:
                    # col2 is smaller, check what portion of it overlaps  
                    return overlap_size / range2_size if range2_size > 0 else 0.0
            else:
                # No overlap
                return 0.0
                    
        except (ValueError, TypeError):
            return 0.0
    
    def _find_pattern_relationships(self, column_index: Dict):
        """Find relationships based on value patterns and formats."""
        
        # Group columns by value patterns
        pattern_groups = defaultdict(list)
        
        for table_name, col in column_index['all_columns']:
            if col.value_pattern and col.value_pattern.detected_format.value != 'unknown':
                pattern_groups[col.value_pattern.detected_format.value].append((table_name, col))
        
        # Check columns with same patterns
        for pattern, columns in pattern_groups.items():
            if len(columns) < 2:
                continue
            
            for i in range(len(columns)):
                for j in range(i + 1, len(columns)):
                    table1, col1 = columns[i]
                    table2, col2 = columns[j]
                    
                    # Skip same table
                    if table1 == table2:
                        continue
                    
                    # Calculate value overlap
                    overlap_score = self._calculate_value_overlap(col1, col2)
                    
                    if overlap_score > 0.2:
                        candidate = RelationshipCandidate(
                            source_table=table1,
                            source_column=col1,
                            target_table=table2,
                            target_column=col2,
                            value_overlap_score=overlap_score,
                            data_type_match=(col1.data_type == col2.data_type),
                            evidence=[f"Same value pattern: {pattern}"]
                        )
                        
                        self.candidates.append(candidate)
    
    def _calculate_value_overlap(
        self, 
        col1: ColumnProfile, 
        col2: ColumnProfile
    ) -> float:
        """Calculate the overlap between two columns' values."""
        
        # Use top distinct values if available
        if col1.top_distinct_values and col2.top_distinct_values:
            set1 = set(col1.top_distinct_values)
            set2 = set(col2.top_distinct_values)
            
            if not set1 or not set2:
                return 0.0
            
            intersection = set1 & set2
            union = set1 | set2
            
            return len(intersection) / len(union) if union else 0.0
        
        # Use sample values if available
        if (col1.value_statistics and col1.value_statistics.sample_values and
            col2.value_statistics and col2.value_statistics.sample_values):
            
            set1 = set(col1.value_statistics.sample_values)
            set2 = set(col2.value_statistics.sample_values)
            
            if not set1 or not set2:
                return 0.0
            
            intersection = set1 & set2
            
            # For samples, we care more about whether child values exist in parent
            return len(intersection) / len(set2) if set2 else 0.0
        
        return 0.0
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between column names."""
        
        name1_lower = name1.lower()
        name2_lower = name2.lower()
        
        # Exact match
        if name1_lower == name2_lower:
            return 1.0
        
        # One contains the other
        if name1_lower in name2_lower or name2_lower in name1_lower:
            return 0.7
        
        # Common patterns
        common_suffixes = ['_id', '_key', '_code', '_num', '_no']
        for suffix in common_suffixes:
            if name1_lower.endswith(suffix) and name2_lower.endswith(suffix):
                base1 = name1_lower[:-len(suffix)]
                base2 = name2_lower[:-len(suffix)]
                if base1 in base2 or base2 in base1:
                    return 0.5
        
        return 0.0
    
    def _determine_relationship_type(
        self, 
        candidate: RelationshipCandidate
    ) -> RelationshipType:
        """Determine the type of relationship."""
        
        source_col = candidate.source_column
        target_col = candidate.target_column
        
        # Check if both are unique (potential 1:1)
        if (source_col.uniqueness == 1.0 and target_col.uniqueness == 1.0):
            return RelationshipType.ONE_TO_ONE
        
        # Check if target is unique (potential many:1)
        if target_col.uniqueness == 1.0:
            return RelationshipType.MANY_TO_ONE
        
        # Check if source is unique (potential 1:many)
        if source_col.uniqueness == 1.0:
            return RelationshipType.ONE_TO_MANY
        
        # Default to many:many
        return RelationshipType.MANY_TO_MANY
    
    def _score_candidates(
        self, 
        candidates: List[RelationshipCandidate]
    ) -> List[RelationshipCandidate]:
        """Score and rank relationship candidates."""
        
        for candidate in candidates:
            # Calculate name similarity if not already done
            if candidate.name_similarity_score == 0.0:
                candidate.name_similarity_score = self._calculate_name_similarity(
                    candidate.source_column.column_name,
                    candidate.target_column.column_name
                )
            
            # Determine relationship type
            candidate.relationship_type = self._determine_relationship_type(candidate)
            
            # Calculate composite confidence score
            scores = []
            weights = []
            
            # Value overlap is most important
            if candidate.value_overlap_score > 0:
                scores.append(candidate.value_overlap_score)
                weights.append(3.0)
            
            # Name similarity
            if candidate.name_similarity_score > 0:
                scores.append(candidate.name_similarity_score)
                weights.append(2.0)
            
            # Cardinality score
            if candidate.cardinality_score > 0:
                scores.append(candidate.cardinality_score)
                weights.append(1.5)
            
            # Statistical correlation
            if candidate.statistical_correlation > 0:
                scores.append(candidate.statistical_correlation)
                weights.append(1.5)
            
            # Data type match
            if candidate.data_type_match:
                scores.append(1.0)
                weights.append(1.0)
            
            # Calculate weighted average
            if scores:
                candidate.confidence = sum(s * w for s, w in zip(scores, weights)) / sum(weights)
            else:
                candidate.confidence = 0.0
            
            # Boost confidence for certain patterns
            if candidate.relationship_type in [RelationshipType.ONE_TO_ONE, RelationshipType.MANY_TO_ONE]:
                candidate.confidence *= 1.1
            
            # Cap at 1.0
            candidate.confidence = min(candidate.confidence, 1.0)
        
        # Sort by confidence
        return sorted(candidates, key=lambda x: x.confidence, reverse=True)
    
    def generate_relationship_report(
        self, 
        candidates: List[RelationshipCandidate]
    ) -> str:
        """Generate a detailed report of discovered relationships."""
        
        lines = ["# Cross-Profile Relationship Discovery Report", ""]
        
        # Group by confidence level
        high_confidence = [c for c in candidates if c.confidence >= 0.8]
        medium_confidence = [c for c in candidates if 0.5 <= c.confidence < 0.8]
        
        if high_confidence:
            lines.extend([
                "## High Confidence Relationships (≥ 0.8)",
                ""
            ])
            
            for candidate in high_confidence[:20]:  # Top 20
                lines.append(
                    f"- **{candidate.source_table}.{candidate.source_column.column_name}** → "
                    f"**{candidate.target_table}.{candidate.target_column.column_name}** "
                    f"(confidence: {candidate.confidence:.2f}, type: {candidate.relationship_type.value})"
                )
                
                if candidate.evidence:
                    for evidence in candidate.evidence:
                        lines.append(f"  - {evidence}")
                
                lines.append("")
        
        if medium_confidence:
            lines.extend([
                "## Medium Confidence Relationships (0.5 - 0.8)",
                ""
            ])
            
            for candidate in medium_confidence[:20]:  # Top 20
                lines.append(
                    f"- {candidate.source_table}.{candidate.source_column.column_name} → "
                    f"{candidate.target_table}.{candidate.target_column.column_name} "
                    f"(confidence: {candidate.confidence:.2f})"
                )
        
        # Statistics
        lines.extend([
            "",
            "## Discovery Statistics",
            "",
            f"- Total relationships found: {len(candidates)}",
            f"- High confidence: {len(high_confidence)}",
            f"- Medium confidence: {len(medium_confidence)}",
            ""
        ])
        
        # Relationship type breakdown
        type_counts = defaultdict(int)
        for c in candidates:
            if c.relationship_type:
                type_counts[c.relationship_type.value] += 1
        
        if type_counts:
            lines.extend(["### Relationship Types", ""])
            for rel_type, count in sorted(type_counts.items()):
                lines.append(f"- {rel_type}: {count}")
        
        return "\n".join(lines)
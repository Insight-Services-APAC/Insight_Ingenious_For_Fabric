"""
Ultra-Fast Data Profiler for PySpark

This module provides high-performance single-pass data profiling optimized for large datasets.
It uses aggregation-based profiling to minimize data scans and memory usage.
"""

import time
from typing import Any, Dict, List, Optional, Set
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType

from ingen_fab.python_libs.interfaces.data_profiling_interface import (
    ColumnProfile,
    DatasetProfile,
    ProfileType,
    RelationshipType,
    SemanticType,
    ColumnRelationship,
)


class UltraFastProfiler:
    """
    High-performance data profiler using single-pass aggregation.
    
    This profiler is optimized for large datasets and uses efficient
    aggregation queries to minimize memory usage and processing time.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the ultra-fast profiler.
        
        Args:
            spark: SparkSession to use for profiling operations
        """
        self.spark = spark
        
        # Import relationship discovery components if available
        try:
            from ingen_fab.python_libs.common.relationship_discovery import (
                ColumnNamingAnalyzer,
                ValueFormatDetector,
                SemanticTypeClassifier,
                BusinessRuleDetector,
            )
            self.naming_analyzer = ColumnNamingAnalyzer()
            self.format_detector = ValueFormatDetector()
            self.semantic_classifier = SemanticTypeClassifier()
            self.rule_detector = BusinessRuleDetector()
            self.relationship_analysis_available = True
        except ImportError:
            self.relationship_analysis_available = False
    
    def profile_dataset(
        self,
        dataset: Any,
        profile_type: ProfileType = ProfileType.BASIC,
        sample_size: Optional[float] = None,
    ) -> DatasetProfile:
        """
        Profile a dataset using ultra-fast single-pass aggregation.
        
        Args:
            dataset: Dataset to profile (DataFrame or table name)
            profile_type: Type of profiling to perform
            sample_size: Optional sampling fraction (0-1)
            
        Returns:
            DatasetProfile with profiling results
        """
        start_time = time.time()
        
        # Convert to DataFrame if needed
        if isinstance(dataset, str):
            df = self.spark.table(dataset)
        else:
            df = dataset
        
        # Cache for multiple operations
        df = df.cache()
        
        # Get row count
        row_count = df.count()
        
        # Determine if we need staged processing for very large datasets
        use_staged_processing = row_count > 500_000
        
        # Apply sampling
        if sample_size and 0 < sample_size < 1:
            df = df.sample(fraction=sample_size, seed=42)
            scale_factor = row_count / df.count()
        else:
            scale_factor = 1.0
        
        if use_staged_processing:
            print(f"🔄 Using staged processing for large dataset ({row_count:,} rows)")
            batch_result = self._staged_aggregation(df, row_count, profile_type)
            potential_keys = batch_result.pop('_potential_keys', [])
        else:
            # Single aggregation for ALL columns
            batch_result, potential_keys = self._single_pass_aggregation(
                df, row_count, profile_type
            )
        
        # Build column profiles
        column_profiles, relationship_signatures = self._build_column_profiles(
            df, batch_result, row_count, scale_factor, potential_keys, profile_type
        )
        
        # Calculate overall null count
        total_null_count = self._calculate_total_nulls(df)
        
        df.unpersist()
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(column_profiles, profile_type)
        
        # Detect relationships
        if profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL] and len(relationship_signatures) > 1:
            self._detect_ultra_fast_relationships(column_profiles, relationship_signatures)
        
        duration = time.time() - start_time
        statistics = {
            "profiling_duration_seconds": round(duration, 2),
            "relationship_analysis_enabled": profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL],
            "potential_key_columns": len(potential_keys),
            "signatures_generated": len(relationship_signatures),
        }
        
        return DatasetProfile(
            dataset_name=dataset if isinstance(dataset, str) else "dataframe",
            row_count=row_count,
            column_count=len(df.columns),
            profile_timestamp=datetime.now().isoformat(),
            column_profiles=column_profiles,
            null_count=total_null_count,
            duplicate_count=0,  # Skip expensive operation
            data_quality_score=quality_score,
            statistics=statistics,
        )
    
    def _single_pass_aggregation(
        self, df: DataFrame, row_count: int, profile_type: ProfileType
    ) -> tuple[Dict[str, Any], List[str]]:
        """
        Perform single-pass aggregation for all columns.
        
        Returns:
            Tuple of (aggregation results, potential key columns)
        """
        agg_exprs = []
        potential_keys = []
        
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            
            # Core stats for all columns
            agg_exprs.extend([
                F.count(F.when(F.col(col_name).isNotNull(), 1)).alias(f"{col_name}__non_null"),
                F.approx_count_distinct(F.when(F.col(col_name).isNotNull(), F.col(col_name))).alias(
                    f"{col_name}__distinct_non_null"
                ),
                F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"{col_name}__null_count"),
            ])
            
            # Type-specific stats
            if col_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                agg_exprs.extend([
                    F.min(col_name).alias(f"{col_name}__min"),
                    F.max(col_name).alias(f"{col_name}__max"),
                    F.mean(col_name).alias(f"{col_name}__mean"),
                ])
            elif col_type == 'string':
                agg_exprs.extend([
                    F.min(F.length(col_name)).alias(f"{col_name}__min_len"),
                    F.max(F.length(col_name)).alias(f"{col_name}__max_len"),
                    F.mean(F.length(col_name)).alias(f"{col_name}__mean_len"),
                ])
            elif col_type in ['date', 'timestamp']:
                agg_exprs.extend([
                    F.min(col_name).alias(f"{col_name}__min"),
                    F.max(col_name).alias(f"{col_name}__max"),
                ])
            
            # Relationship discovery aggregations
            if self._is_potential_key_column(col_name, col_type):
                potential_keys.append(col_name)
                
                if row_count < 100_000:  # Only for smaller tables
                    agg_exprs.extend([
                        F.expr(f"slice(sort_array(collect_set(hash({col_name}))), 1, 20)").alias(
                            f"{col_name}__hash_signature"
                        ),
                        (F.approx_count_distinct(col_name) / 
                         F.when(F.count(col_name) > 0, F.count(col_name)).otherwise(1)).alias(
                            f"{col_name}__selectivity"
                        ),
                        F.expr(f"slice(sort_array(collect_set({col_name})), 1, 3)").alias(
                            f"{col_name}__sample_values"
                        ),
                        F.expr(f"slice(sort_array(collect_set({col_name})), 1, 5)").alias(
                            f"{col_name}__frequent_values"
                        ),
                    ])
                else:
                    # For large datasets, skip memory-intensive operations
                    agg_exprs.extend([
                        F.lit(None).alias(f"{col_name}__hash_signature"),
                        (F.approx_count_distinct(col_name) / 
                         F.when(F.count(col_name) > 0, F.count(col_name)).otherwise(1)).alias(
                            f"{col_name}__selectivity"
                        ),
                        F.lit(None).alias(f"{col_name}__sample_values"),
                        F.lit(None).alias(f"{col_name}__frequent_values"),
                    ])
        
        # Execute single query
        batch_result = df.agg(*agg_exprs).collect()[0].asDict()
        return batch_result, potential_keys
    
    def _staged_aggregation(
        self, df: DataFrame, row_count: int, profile_type: ProfileType
    ) -> Dict[str, Any]:
        """
        Perform staged aggregation for very large datasets.
        
        Splits aggregation into chunks to avoid memory issues.
        """
        import math
        
        # Calculate optimal chunk size
        chunk_size = min(100_000, max(10_000, row_count // 20))
        num_chunks = math.ceil(row_count / chunk_size)
        
        print(f"📊 Processing {num_chunks} chunks of ~{chunk_size:,} rows each")
        
        # Add row numbers for chunking
        df_numbered = df.withColumn("_row_id", F.monotonically_increasing_id())
        df_numbered = df_numbered.withColumn("_chunk_id", F.col("_row_id") % num_chunks)
        
        # Initialize results
        column_names = df.columns
        potential_keys = []
        aggregated_results = {}
        
        # Identify potential key columns
        for col_name in column_names:
            col_type = dict(df.dtypes)[col_name]
            if self._is_potential_key_column(col_name, col_type):
                potential_keys.append(col_name)
        
        # Process each chunk
        for chunk_id in range(num_chunks):
            print(f"  Processing chunk {chunk_id + 1}/{num_chunks}")
            
            chunk_df = df_numbered.filter(F.col("_chunk_id") == chunk_id)
            chunk_results = self._process_chunk(chunk_df, column_names, potential_keys, profile_type)
            
            # Merge results
            if chunk_id == 0:
                aggregated_results = chunk_results
            else:
                aggregated_results = self._merge_chunk_results(aggregated_results, chunk_results)
        
        # Finalize results
        df_dtypes = dict(df.dtypes)
        final_results = self._finalize_staged_results(
            aggregated_results, row_count, column_names, df_dtypes
        )
        final_results['_potential_keys'] = potential_keys
        
        return final_results
    
    def _process_chunk(
        self,
        chunk_df: DataFrame,
        column_names: List[str],
        potential_keys: List[str],
        profile_type: ProfileType,
    ) -> Dict[str, Any]:
        """Process a single chunk and return aggregated statistics."""
        agg_exprs = []
        
        for col_name in column_names:
            if col_name.startswith('_'):  # Skip internal columns
                continue
            
            col_type = dict(chunk_df.dtypes)[col_name]
            
            # Basic stats
            agg_exprs.extend([
                F.count(F.when(F.col(col_name).isNotNull(), 1)).alias(f"{col_name}__non_null"),
                F.approx_count_distinct(F.when(F.col(col_name).isNotNull(), F.col(col_name))).alias(
                    f"{col_name}__distinct_non_null"
                ),
                F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"{col_name}__null_count"),
            ])
            
            # Type-specific stats
            if col_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                agg_exprs.extend([
                    F.min(col_name).alias(f"{col_name}__min"),
                    F.max(col_name).alias(f"{col_name}__max"),
                    F.sum(col_name).alias(f"{col_name}__sum"),
                ])
            elif col_type == 'string':
                agg_exprs.extend([
                    F.min(F.length(col_name)).alias(f"{col_name}__min_len"),
                    F.max(F.length(col_name)).alias(f"{col_name}__max_len"),
                    F.sum(F.length(col_name)).alias(f"{col_name}__sum_len"),
                ])
            elif col_type in ['date', 'timestamp']:
                agg_exprs.extend([
                    F.min(col_name).alias(f"{col_name}__min"),
                    F.max(col_name).alias(f"{col_name}__max"),
                ])
            
            # For potential keys, collect limited data
            if col_name in potential_keys and profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL]:
                agg_exprs.extend([
                    F.approx_count_distinct(col_name).alias(f"{col_name}__approx_distinct"),
                    F.lit(None).alias(f"{col_name}__hash_signature"),
                    F.lit(None).alias(f"{col_name}__sample_values"),
                    F.lit(None).alias(f"{col_name}__frequent_values"),
                ])
        
        return chunk_df.agg(*agg_exprs).collect()[0].asDict()
    
    def _merge_chunk_results(
        self, results1: Dict[str, Any], results2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge results from two chunks."""
        merged = {}
        
        for key in results1.keys():
            if key.endswith('__non_null') or key.endswith('__null_count'):
                merged[key] = (results1[key] or 0) + (results2[key] or 0)
            elif key.endswith('__distinct_non_null') or key.endswith('__approx_distinct'):
                merged[key] = max(results1[key] or 0, results2[key] or 0)
            elif key.endswith('__min') or key.endswith('__min_len'):
                val1, val2 = results1[key], results2[key]
                if val1 is None:
                    merged[key] = val2
                elif val2 is None:
                    merged[key] = val1
                else:
                    merged[key] = min(val1, val2)
            elif key.endswith('__max') or key.endswith('__max_len'):
                val1, val2 = results1[key], results2[key]
                if val1 is None:
                    merged[key] = val2
                elif val2 is None:
                    merged[key] = val1
                else:
                    merged[key] = max(val1, val2)
            elif key.endswith('__sum') or key.endswith('__sum_len'):
                merged[key] = (results1[key] or 0) + (results2[key] or 0)
            else:
                merged[key] = results1[key] if results1[key] is not None else results2[key]
        
        return merged
    
    def _finalize_staged_results(
        self,
        aggregated_results: Dict[str, Any],
        total_rows: int,
        column_names: List[str],
        df_dtypes: Dict[str, str],
    ) -> Dict[str, Any]:
        """Convert aggregated results to final format."""
        final_results = {}
        
        for col_name in column_names:
            if col_name.startswith('_'):
                continue
            
            col_type = df_dtypes.get(col_name, 'string')
            
            # Copy basic stats
            final_results[f"{col_name}__non_null"] = aggregated_results.get(f"{col_name}__non_null", 0)
            final_results[f"{col_name}__distinct_non_null"] = aggregated_results.get(
                f"{col_name}__distinct_non_null", 0
            )
            final_results[f"{col_name}__null_count"] = aggregated_results.get(f"{col_name}__null_count", 0)
            
            # Calculate means from sums
            non_null_count = aggregated_results.get(f"{col_name}__non_null", 0)
            if non_null_count > 0:
                if col_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                    sum_val = aggregated_results.get(f"{col_name}__sum", 0)
                    final_results[f"{col_name}__mean"] = sum_val / non_null_count if sum_val else None
                    final_results[f"{col_name}__min"] = aggregated_results.get(f"{col_name}__min")
                    final_results[f"{col_name}__max"] = aggregated_results.get(f"{col_name}__max")
                elif col_type == 'string':
                    sum_len = aggregated_results.get(f"{col_name}__sum_len", 0)
                    final_results[f"{col_name}__mean_len"] = sum_len / non_null_count if sum_len else None
                    final_results[f"{col_name}__min_len"] = aggregated_results.get(f"{col_name}__min_len")
                    final_results[f"{col_name}__max_len"] = aggregated_results.get(f"{col_name}__max_len")
                elif col_type in ['date', 'timestamp']:
                    final_results[f"{col_name}__min"] = aggregated_results.get(f"{col_name}__min")
                    final_results[f"{col_name}__max"] = aggregated_results.get(f"{col_name}__max")
            
            # Add relationship fields
            final_results[f"{col_name}__hash_signature"] = None
            final_results[f"{col_name}__sample_values"] = None
            final_results[f"{col_name}__frequent_values"] = None
            
            # Calculate selectivity
            approx_distinct = aggregated_results.get(f"{col_name}__approx_distinct", 0)
            final_results[f"{col_name}__selectivity"] = approx_distinct / total_rows if total_rows > 0 else 0
        
        return final_results
    
    def _build_column_profiles(
        self,
        df: DataFrame,
        batch_result: Dict[str, Any],
        row_count: int,
        scale_factor: float,
        potential_keys: List[str],
        profile_type: ProfileType,
    ) -> tuple[List[ColumnProfile], Dict[str, Any]]:
        """Build column profiles from aggregation results."""
        column_profiles = []
        relationship_signatures = {}
        
        for col_name in df.columns:
            non_null = batch_result[f"{col_name}__non_null"]
            distinct_non_null = batch_result[f"{col_name}__distinct_non_null"]
            has_nulls = batch_result[f"{col_name}__null_count"] > 0
            null_count = int(row_count - (non_null * scale_factor))
            distinct_count = int(distinct_non_null * scale_factor) + (1 if has_nulls else 0)
            
            profile = ColumnProfile(
                column_name=col_name,
                data_type=dict(df.dtypes)[col_name],
                null_count=null_count,
                null_percentage=(null_count / row_count * 100) if row_count > 0 else 0,
                distinct_count=distinct_count,
                distinct_percentage=(distinct_count / row_count * 100) if row_count > 0 else 0,
                completeness=1 - (null_count / row_count) if row_count > 0 else 0,
                uniqueness=distinct_count / row_count if row_count > 0 else 0,
            )
            
            # Add type-specific values
            col_type = dict(df.dtypes)[col_name]
            if col_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                profile.min_value = batch_result.get(f"{col_name}__min")
                profile.max_value = batch_result.get(f"{col_name}__max")
                profile.mean_value = batch_result.get(f"{col_name}__mean")
            elif col_type == 'string':
                profile.min_value = batch_result.get(f"{col_name}__min_len")
                profile.max_value = batch_result.get(f"{col_name}__max_len")
                profile.mean_value = batch_result.get(f"{col_name}__mean_len")
            elif col_type in ['date', 'timestamp']:
                profile.min_value = batch_result.get(f"{col_name}__min")
                profile.max_value = batch_result.get(f"{col_name}__max")
            
            # Enhanced relationship discovery
            if col_name in potential_keys and profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL]:
                if self.relationship_analysis_available:
                    # Semantic analysis
                    profile.naming_pattern = self.naming_analyzer.analyze_column_name(col_name)
                    profile.semantic_type = self._classify_semantic_type(col_name, col_type, profile)
                    
                    # Sample values for pattern detection
                    sample_values = batch_result.get(f"{col_name}__sample_values")
                    if sample_values and len(sample_values) > 0:
                        profile.top_distinct_values = sample_values[:10]
                        profile.value_pattern = self.format_detector.detect_format(sample_values[:5])
                        
                        # Business rule detection
                        profile.business_rules = self.rule_detector.detect_rules(
                            column_name=col_name,
                            data_type=col_type,
                            min_value=profile.min_value,
                            max_value=profile.max_value,
                            distinct_values=sample_values[:5],
                            null_count=profile.null_count,
                            total_count=row_count,
                            semantic_type=profile.semantic_type or SemanticType.UNKNOWN,
                        )
                    else:
                        profile.top_distinct_values = []
                        profile.value_pattern = None
                        profile.business_rules = []
                    
                    # Store signature for relationship matching
                    hash_sig = batch_result.get(f"{col_name}__hash_signature")
                    frequent_vals = batch_result.get(f"{col_name}__frequent_values") or []
                    sample_vals = batch_result.get(f"{col_name}__sample_values") or []
                    
                    if (hash_sig and len(hash_sig) > 0) or frequent_vals or sample_vals:
                        relationship_signatures[col_name] = {
                            'hash_signature': set(hash_sig) if hash_sig else set(),
                            'selectivity': batch_result.get(f"{col_name}__selectivity", 0.0),
                            'frequent_values': set(str(v) for v in frequent_vals if v is not None),
                            'sample_values': set(str(v) for v in sample_vals if v is not None),
                            'distinct_count': distinct_count,
                            'semantic_type': profile.semantic_type,
                            'min_value': profile.min_value,
                            'max_value': profile.max_value,
                        }
            elif len(column_profiles) < 5:  # Quick top values for first few columns
                try:
                    top_vals = (
                        df.groupBy(col_name)
                        .count()
                        .orderBy(F.desc("count"))
                        .limit(5)
                        .rdd.map(lambda x: x[0])
                        .collect()
                    )
                    profile.top_distinct_values = top_vals[:5] if top_vals else []
                except:
                    profile.top_distinct_values = []
            
            column_profiles.append(profile)
        
        return column_profiles, relationship_signatures
    
    def _calculate_total_nulls(self, df: DataFrame) -> int:
        """Calculate total number of rows with at least one null value."""
        null_conditions = [F.col(col_name).isNull() for col_name in df.columns]
        any_null_condition = null_conditions[0]
        for condition in null_conditions[1:]:
            any_null_condition = any_null_condition | condition
        return df.filter(any_null_condition).count()
    
    def _calculate_quality_score(
        self, column_profiles: List[ColumnProfile], profile_type: ProfileType
    ) -> Optional[float]:
        """Calculate overall data quality score."""
        if profile_type in [ProfileType.DATA_QUALITY, ProfileType.FULL]:
            completeness_scores = [
                cp.completeness for cp in column_profiles if cp.completeness is not None
            ]
            return sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0.0
        return None
    
    def _is_potential_key_column(self, col_name: str, col_type: str) -> bool:
        """Identify columns that could be keys or foreign keys."""
        key_indicators = ['id', 'key', 'code', 'ref', 'fk', 'pk']
        name_lower = col_name.lower()
        
        # Name-based detection
        has_key_name = any(indicator in name_lower for indicator in key_indicators)
        
        # Type-based detection
        is_key_type = col_type in ['int', 'bigint', 'string']
        
        return has_key_name or (is_key_type and ('_' in name_lower or name_lower.endswith('id')))
    
    def _classify_semantic_type(
        self, col_name: str, col_type: str, profile: ColumnProfile
    ) -> Optional[SemanticType]:
        """Quick semantic type classification."""
        if not self.relationship_analysis_available:
            return None
        
        return self.semantic_classifier.classify_column(
            column_name=col_name,
            data_type=col_type,
            null_percentage=profile.null_percentage,
            distinct_percentage=profile.distinct_percentage,
            uniqueness=profile.uniqueness or 0.0,
            naming_pattern=getattr(profile, 'naming_pattern', None),
            value_pattern=getattr(profile, 'value_pattern', None),
            sample_values=(
                getattr(profile, 'top_distinct_values', [])[:5]
                if getattr(profile, 'top_distinct_values')
                else []
            ),
        )
    
    def _detect_ultra_fast_relationships(
        self, column_profiles: List[ColumnProfile], signatures: Dict[str, Any]
    ) -> None:
        """Detect relationships using hash signatures."""
        profile_map = {cp.column_name: cp for cp in column_profiles}
        
        # Compare all pairs of columns
        col_names = list(signatures.keys())
        for i, col1 in enumerate(col_names):
            for col2 in col_names[i + 1:]:
                sig1 = signatures[col1]
                sig2 = signatures[col2]
                
                # Calculate similarity scores
                hash_jaccard = self._calculate_jaccard(
                    sig1['hash_signature'], sig2['hash_signature']
                )
                freq_jaccard = self._calculate_jaccard(
                    sig1['frequent_values'], sig2['frequent_values']
                )
                sample_jaccard = self._calculate_jaccard(
                    sig1['sample_values'], sig2['sample_values']
                )
                
                # Combined similarity
                similarity = (hash_jaccard * 0.4 + freq_jaccard * 0.4 + sample_jaccard * 0.2)
                
                if similarity > 0.2:
                    # Create relationship
                    rel_type = self._infer_relationship_type(
                        sig1['distinct_count'], sig2['distinct_count'], similarity
                    )
                    
                    relationship = ColumnRelationship(
                        source_table="current_table",
                        source_column=col1,
                        target_table="current_table",
                        target_column=col2,
                        relationship_type=(
                            RelationshipType.MANY_TO_MANY
                            if rel_type == "many-to-many"
                            else RelationshipType.ONE_TO_MANY
                        ),
                        confidence_score=min(similarity * 1.2, 1.0),
                        overlap_percentage=similarity * 100,
                        referential_integrity_score=max(hash_jaccard, freq_jaccard),
                        suggested_join_condition=f"{col1} = {col2}",
                    )
                    
                    # Add to profiles
                    if not hasattr(profile_map[col1], 'relationships'):
                        profile_map[col1].relationships = []
                    if not hasattr(profile_map[col2], 'relationships'):
                        profile_map[col2].relationships = []
                    
                    profile_map[col1].relationships.append(relationship)
                    
                    # Create reverse relationship
                    reverse_rel = ColumnRelationship(
                        source_table="current_table",
                        source_column=col2,
                        target_table="current_table",
                        target_column=col1,
                        relationship_type=relationship.relationship_type,
                        confidence_score=relationship.confidence_score,
                        overlap_percentage=relationship.overlap_percentage,
                        referential_integrity_score=relationship.referential_integrity_score,
                        suggested_join_condition=f"{col2} = {col1}",
                    )
                    profile_map[col2].relationships.append(reverse_rel)
    
    def _calculate_jaccard(self, set1: Set, set2: Set) -> float:
        """Calculate Jaccard similarity between two sets."""
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    def _infer_relationship_type(
        self, cardinality1: int, cardinality2: int, similarity: float
    ) -> str:
        """Infer relationship type from cardinalities and similarity."""
        if cardinality1 == 0 or cardinality2 == 0:
            return "many-to-many"
        
        ratio = cardinality1 / cardinality2
        
        if similarity > 0.95 and abs(ratio - 1) < 0.1:
            return "one-to-one"
        elif ratio > 2 and similarity > 0.5:
            return "one-to-many"
        elif ratio < 0.5 and similarity > 0.5:
            return "many-to-one"
        else:
            return "many-to-many"
    
    def generate_quality_report(
        self, profile: DatasetProfile, output_format: str = "yaml"
    ) -> str:
        """Generate quality report from profile."""
        if output_format == "yaml":
            # Import YAML generation from main profiler
            from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark
            
            full_profiler = DataProfilingPySpark(self.spark)
            return full_profiler._generate_yaml_report(profile)
        else:
            # Simple fallback for other formats
            quality_display = (
                f"{profile.data_quality_score:.2%}" if profile.data_quality_score else "N/A"
            )
            columns_summary = "\n".join([
                f"- {cp.column_name}: {cp.data_type}, {cp.null_percentage:.1f}% nulls, "
                f"{cp.distinct_count:,} distinct"
                for cp in profile.column_profiles
            ])
            
            return f"""# Ultra-Fast Profile Report
Dataset: {profile.dataset_name}
Timestamp: {profile.profile_timestamp}
Rows: {profile.row_count:,}
Columns: {profile.column_count}
Quality Score: {quality_display}
Duration: {profile.statistics.get('profiling_duration_seconds', 'N/A')}s

## Column Summary
{columns_summary}
"""
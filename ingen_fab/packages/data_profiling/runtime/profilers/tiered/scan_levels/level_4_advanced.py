"""Level 4 Advanced Scanner - Multi-pass advanced analysis."""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingen_fab.packages.data_profiling.runtime.core.enums.profile_types import (
    ScanLevel,
    SemanticType,
)
from ingen_fab.packages.data_profiling.runtime.core.interfaces.persistence_interface import (
    PersistenceInterface,
)
from ingen_fab.packages.data_profiling.runtime.core.models.metadata import ScanProgress
from ingen_fab.packages.data_profiling.runtime.core.models.profile_models import (
    ColumnProfile,
    DatasetProfile,
)
from ingen_fab.packages.data_profiling.runtime.profilers.tiered.scan_levels.base_scan_level import (
    BaseScanLevel,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


class Level4AdvancedScanner(BaseScanLevel):
    """
    Level 4 Scanner: Advanced multi-pass profiling.
    
    This scan performs advanced analysis including:
    - Cross-column correlations
    - Relationship discovery
    - Pattern detection
    - Anomaly detection
    - Business rule inference
    - Percentiles and entropy calculations
    - Top value distributions
    """
    
    @property
    def scan_level(self) -> ScanLevel:
        """Return the ScanLevel enum for this scan level."""
        return ScanLevel.LEVEL_4_ADVANCED
    
    @property
    def scan_name(self) -> str:
        """Return human-readable name for this scan level."""
        return "LEVEL 4 SCAN: Advanced Multi-Pass Analysis"
    
    @property
    def scan_description(self) -> str:
        """Return description of what this scan level does."""
        return "Multi-pass advanced analysis including correlations and relationships"
    
    def execute(
        self,
        table_names: Optional[List[str]] = None,
        resume: bool = True,
        **kwargs
    ) -> List[DatasetProfile]:
        """
        Execute Level 4 advanced analysis scan.
        
        Args:
            table_names: Optional list of specific tables to scan
            resume: Whether to skip already scanned tables
            
        Returns:
            List of DatasetProfile objects with advanced analytics
        """
        print("\n" + "="*60)
        print("🔬 LEVEL 4 SCAN: Advanced Multi-Pass Analysis")
        print("="*60)
        
        profiles = []
        
        # Get tables to scan
        if table_names:
            tables_to_scan = table_names
        else:
            # Get all tables that completed Level 3
            level_3_completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_3_PROFILE)
            tables_to_scan = level_3_completed
        
        # Filter out already scanned tables if resuming
        if resume:
            completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_4_ADVANCED)
            tables_to_scan = [t for t in tables_to_scan if t not in completed]
            if completed:
                print(f"ℹ️  Skipping {len(completed)} already scanned tables")
        
        print(f"📊 Processing {len(tables_to_scan)} tables")
        
        for table_name in tables_to_scan:
            print(f"\n  Advanced analysis for: {table_name}")
            
            start_time = time.time()
            progress = self.persistence.load_progress(table_name) or ScanProgress(table_name=table_name)
            
            try:
                # Load Level 3 profile
                level_3_profile = self._load_level_3_profile(table_name)
                if not level_3_profile:
                    print("    ⚠️  No Level 3 profile found, skipping")
                    continue
                
                # Perform advanced analysis
                advanced_profile = self._perform_advanced_analysis(
                    table_name,
                    level_3_profile
                )
                
                # Save the enhanced profile
                self.persistence.save_profile(advanced_profile)
                profiles.append(advanced_profile)
                
                # Update progress
                duration_ms = int((time.time() - start_time) * 1000)
                progress.level_4_completed = datetime.now()
                progress.level_4_duration_ms = duration_ms
                self.persistence.save_progress(progress)
                
                print(f"    ✅ Completed in {duration_ms}ms")
                
            except Exception as e:
                import traceback
                error_details = f"{str(e)}\nFile: {__file__}\nLine: {traceback.extract_tb(e.__traceback__)[-1].lineno}\n{traceback.format_exc()}"
                print(f"    ❌ Error in Level 4 scan: {str(error_details)}")
                progress.last_error = error_details
                progress.last_error_time = datetime.now()
                self.persistence.save_progress(progress)
        
        print(f"\n✨ Level 4 scan complete. Analyzed {len(profiles)} tables")
        return profiles
    
    def _load_level_3_profile(self, table_name: str) -> Optional[DatasetProfile]:
        """Load Level 3 profile from persistence with full fidelity."""
        try:
            profile_data = self.persistence.load_profile(
                table_name=table_name,
                level=ScanLevel.LEVEL_3_PROFILE
            )
            
            if profile_data:
                # Use enhanced deserialization to maintain full fidelity
                return profile_data
        except Exception as e:
            print(f"    ⚠️  Could not load Level 3 profile: {e}")
            import traceback
            traceback.print_exc()
        return None
    
    def _perform_advanced_analysis(
        self,
        table_name: str,
        base_profile: DatasetProfile
    ) -> DatasetProfile:
        """Perform advanced multi-pass analysis on a table."""
        # Read the table
        df = self.lakehouse.read_table(table_name)
        
        # Enhanced profile with additional analytics - copy all base profile attributes
        enhanced_profile = DatasetProfile(
            dataset_name=base_profile.dataset_name,
            row_count=base_profile.row_count,
            column_count=base_profile.column_count,
            column_profiles=base_profile.column_profiles.copy(),
            profile_timestamp=datetime.now().isoformat(),
            # Preserve all existing attributes from L3
            data_quality_score=base_profile.data_quality_score,
            correlations=base_profile.correlations,
            anomalies=base_profile.anomalies,
            recommendations=base_profile.recommendations,
            null_count=base_profile.null_count,
            duplicate_count=base_profile.duplicate_count,
            statistics=base_profile.statistics,
            data_quality_issues=base_profile.data_quality_issues,
            entity_relationships=base_profile.entity_relationships,
            semantic_summary=base_profile.semantic_summary.copy() if base_profile.semantic_summary else {},
            business_glossary=base_profile.business_glossary.copy() if base_profile.business_glossary else {}
        )
        
        # Add advanced statistics to each column
        for col_profile in enhanced_profile.column_profiles:
            col_name = col_profile.column_name
            col_type = col_profile.data_type
            
            # Calculate percentiles for numeric columns
            if self._is_numeric_type(col_type) and col_profile.distinct_count > 10:
                percentiles = df.select(
                    F.expr(f"percentile_approx({col_name}, 0.25)").alias("p25"),
                    F.expr(f"percentile_approx({col_name}, 0.5)").alias("p50"),
                    F.expr(f"percentile_approx({col_name}, 0.75)").alias("p75"),
                ).collect()[0]
                
                col_profile.median_value = percentiles.p50
                if not hasattr(col_profile, "percentiles") or col_profile.percentiles is None:
                    col_profile.percentiles = {}
                col_profile.percentiles[25] = percentiles.p25
                col_profile.percentiles[50] = percentiles.p50
                col_profile.percentiles[75] = percentiles.p75
            
            # Calculate entropy for categorical columns
            if col_profile.distinct_count > 1 and col_profile.distinct_count < 1000:
                col_profile.entropy = self._calculate_entropy_efficient(
                    df, col_name, col_profile.distinct_count, base_profile.row_count
                )
            
            # Get top values with frequencies
            if col_profile.distinct_count < 100:
                top_values = (
                    df.groupBy(col_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(10)
                    .collect()
                )
                col_profile.top_distinct_values = [row[col_name] for row in top_values]
                if not hasattr(col_profile, "value_distribution") or col_profile.value_distribution is None:
                    col_profile.value_distribution = {}
                for row in top_values:
                    if row[col_name] is not None:
                        col_profile.value_distribution[str(row[col_name])] = row["count"]
        
        # Detect relationships between columns
        self._detect_column_relationships(enhanced_profile, df)
        
        # Calculate data quality score
        enhanced_profile.data_quality_score = self._calculate_advanced_quality_score(
            enhanced_profile
        )
        
        return enhanced_profile
    
    def _detect_column_relationships(
        self, 
        profile: DatasetProfile, 
        df: DataFrame
    ) -> None:
        """Detect relationships between columns."""
        # Find potential key columns
        key_columns = [
            cp.column_name for cp in profile.column_profiles
            if cp.semantic_type in [SemanticType.IDENTIFIER, SemanticType.FOREIGN_KEY]
        ]
        
        # Find potential measure columns
        measure_columns = [
            cp.column_name for cp in profile.column_profiles
            if cp.semantic_type == SemanticType.MEASURE
        ]
        
        # Initialize relationships list if not exists
        if not profile.entity_relationships:
            profile.entity_relationships = []
        
        # Detect primary key candidates
        for cp in profile.column_profiles:
            if cp.value_statistics and cp.value_statistics.is_unique_key:
                profile.entity_relationships.append({
                    "type": "primary_key_candidate",
                    "column": cp.column_name,
                    "confidence": 0.95 if cp.null_count == 0 else 0.8
                })
        
        # Detect foreign key relationships (simplified)
        for cp in profile.column_profiles:
            if cp.naming_pattern and cp.naming_pattern.is_foreign_key:
                # Try to find referenced table from column name
                col_lower = cp.column_name.lower()
                potential_table = None
                
                # Common patterns: table_id, table_code, etc.
                for suffix in ['_id', '_code', '_key']:
                    if col_lower.endswith(suffix):
                        potential_table = col_lower[:-len(suffix)]
                        break
                
                if potential_table:
                    profile.entity_relationships.append({
                        "type": "foreign_key_candidate",
                        "column": cp.column_name,
                        "referenced_table": potential_table,
                        "confidence": cp.naming_pattern.naming_confidence
                    })
        
        # Calculate numeric column correlations (simplified for performance)
        numeric_cols = [
            cp.column_name for cp in profile.column_profiles
            if self._is_numeric_type(cp.data_type) and cp.null_percentage < 50
        ]
        
        if len(numeric_cols) >= 2 and len(numeric_cols) <= 20:  # Limit for performance
            correlations = {}
            # Sample for performance on large datasets
            sample_df = df.sample(False, min(10000.0 / profile.row_count, 1.0)) if profile.row_count > 10000 else df
            
            for i, col1 in enumerate(numeric_cols):
                for col2 in numeric_cols[i+1:]:
                    try:
                        corr_value = sample_df.stat.corr(col1, col2)
                        if corr_value is not None and abs(corr_value) > 0.5:
                            correlations[f"{col1}:{col2}"] = corr_value
                    except:
                        pass  # Skip correlation if calculation fails
            
            if correlations:
                profile.correlations = correlations
    
    def _calculate_entropy_efficient(
        self, 
        df: DataFrame, 
        column_name: str, 
        distinct_count: int, 
        total_rows: int
    ) -> float:
        """Calculate entropy more efficiently for large datasets using approximation."""
        try:
            # For large datasets, use a more memory-efficient approach
            # Limit the groupBy operation and use approximation
            freq_df = (df.groupBy(column_name)
                      .count()
                      .withColumnRenamed("count", "freq")
                      .limit(distinct_count + 10))  # Small buffer for safety
            
            # Calculate entropy using the frequencies
            entropy_df = freq_df.withColumn(
                "p", F.col("freq") / F.lit(total_rows)
            ).withColumn(
                "entropy_contrib", -F.col("p") * F.log2(F.col("p"))
            )
            
            # Use collect() on the limited result set
            entropy_values = entropy_df.select("entropy_contrib").collect()
            entropy = sum(row.entropy_contrib for row in entropy_values if row.entropy_contrib is not None)
            
            return float(entropy) if entropy else 0.0
        except Exception as entropy_error:
            import traceback
            print(f"    ⚠️  Could not calculate efficient entropy for column '{column_name}': {entropy_error}")
            print(f"    📍 Error location: {__file__}:{traceback.extract_tb(entropy_error.__traceback__)[-1].lineno}")
            return 0.0
    
    def _calculate_advanced_quality_score(
        self, 
        profile: DatasetProfile
    ) -> float:
        """Calculate an advanced data quality score."""
        scores = []
        
        for cp in profile.column_profiles:
            # Completeness score
            completeness = cp.completeness if cp.completeness else 0
            
            # Uniqueness penalty for non-key columns
            uniqueness_penalty = 0
            if cp.semantic_type not in [SemanticType.IDENTIFIER, SemanticType.FOREIGN_KEY]:
                if cp.uniqueness and cp.uniqueness > 0.9:
                    uniqueness_penalty = 0.2  # Suspicious high uniqueness
            
            # Consistency score (based on patterns)
            consistency = 1.0
            if cp.naming_pattern:
                consistency = cp.naming_pattern.naming_confidence
            
            # Calculate column score
            column_score = (completeness * 0.5 + consistency * 0.3 - uniqueness_penalty) * 100
            scores.append(max(0, min(100, column_score)))
        
        # Return average score across all columns
        return sum(scores) / len(scores) if scores else 0.0
    
    def _is_numeric_type(self, type_str: str) -> bool:
        """Check if a type string represents a numeric type."""
        numeric_types = ['int', 'long', 'float', 'double', 'decimal', 'bigint', 'smallint', 'tinyint']
        return any(t in type_str.lower() for t in numeric_types)
    
    def get_scan_statistics(self) -> Dict[str, Any]:
        """Get statistics for Level 4 scan."""
        try:
            completed = self.persistence.list_completed_tables(ScanLevel.LEVEL_4_ADVANCED)
            return {
                "completed_tables": len(completed),
                "scan_level": "LEVEL_4_ADVANCED"
            }
        except Exception as e:
            return {"error": str(e)}
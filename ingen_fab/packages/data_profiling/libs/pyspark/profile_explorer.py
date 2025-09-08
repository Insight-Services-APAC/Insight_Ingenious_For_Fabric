#!/usr/bin/env python3
"""
Profile Explorer - Easy exploration of L3 profile results.

This module provides various ways to explore and analyze the profile results
from the L3 scan, making it easy to understand data quality and characteristics.
"""

import json
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingen_fab.packages.data_profiling.libs.interfaces.data_profiling_interface import (
    DatasetProfile, ColumnProfile, SemanticType
)


@dataclass
class ProfileSummary:
    """Summary statistics for easy exploration."""
    table_name: str
    row_count: int
    column_count: int
    scan_timestamp: str
    data_quality_score: float
    completeness_avg: float
    uniqueness_avg: float
    semantic_types: Dict[str, int]
    columns_with_issues: List[str]


class ProfileExplorer:
    """
    Easy-to-use interface for exploring L3 profile results.
    
    Provides multiple ways to analyze and understand your data profiling results:
    - Summary dashboards
    - Data quality reports  
    - Column-level analysis
    - Interactive queries
    - Export capabilities
    """
    
    def __init__(self, lakehouse, table_prefix: str = "tiered_profile"):
        """
        Initialize the ProfileExplorer.
        
        Args:
            lakehouse: lakehouse_utils instance
            table_prefix: Prefix used for profile tables
        """
        self.lakehouse = lakehouse
        self.spark = lakehouse.spark
        self.profile_table = f"{table_prefix}_profiles"
        
    def list_available_profiles(self) -> List[Dict[str, Any]]:
        """List all available profile results."""
        try:
            profiles_df = self.lakehouse.read_table(self.profile_table)
            profiles = profiles_df.select(
                "table_name", "row_count", "column_count", "scan_timestamp"
            ).orderBy(F.desc("scan_timestamp")).collect()
            
            return [
                {
                    "table_name": row.table_name,
                    "row_count": row.row_count,
                    "column_count": row.column_count, 
                    "scan_timestamp": row.scan_timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.scan_timestamp else None
                }
                for row in profiles
            ]
        except Exception as e:
            print(f"⚠️ Could not load profiles: {e}")
            return []
    
    def get_profile_summary(self, table_name: str) -> Optional[ProfileSummary]:
        """Get a high-level summary for a specific table."""
        try:
            profiles_df = self.lakehouse.read_table(self.profile_table)
            profile_row = profiles_df.filter(F.col("table_name") == table_name).collect()
            
            if not profile_row:
                print(f"❌ get_profile_summary - No profile found for table: {table_name}")
                return None
            
            profile_data = json.loads(profile_row[0].profile_data)
            columns = profile_data.get("column_profiles", [])
            
            if not columns:
                return None
            
            # Calculate summary metrics
            completeness_values = [col.get("completeness", 0) for col in columns]
            uniqueness_values = [col.get("uniqueness", 0) for col in columns]
            
            completeness_avg = sum(completeness_values) / len(completeness_values)
            uniqueness_avg = sum(uniqueness_values) / len(uniqueness_values)
            
            # Count semantic types
            semantic_types = {}
            columns_with_issues = []
            
            for col in columns:
                # Semantic type counts
                sem_type = col.get("semantic_type", "unknown")
                semantic_types[sem_type] = semantic_types.get(sem_type, 0) + 1
                
                # Identify potential issues
                if col.get("null_percentage", 0) > 50:
                    columns_with_issues.append(f"{col['column_name']} (>50% nulls)")
                elif col.get("distinct_count", 0) == 1:
                    columns_with_issues.append(f"{col['column_name']} (constant values)")
                elif col.get("completeness", 1) < 0.8:
                    columns_with_issues.append(f"{col['column_name']} (<80% complete)")
            
            # Simple data quality score (0-100)
            quality_score = (completeness_avg * 0.6 + uniqueness_avg * 0.4) * 100
            
            return ProfileSummary(
                table_name=table_name,
                row_count=profile_data.get("row_count", 0),
                column_count=profile_data.get("column_count", 0),
                scan_timestamp=profile_data.get("profile_timestamp", ""),
                data_quality_score=quality_score,
                completeness_avg=completeness_avg,
                uniqueness_avg=uniqueness_avg,
                semantic_types=semantic_types,
                columns_with_issues=columns_with_issues
            )
            
        except Exception as e:
            print(f"❌ Error getting profile summary: {e}")
            return None
    
    def show_data_quality_dashboard(self, table_name: Optional[str] = None):
        """Show a data quality dashboard for one or all tables."""
        print("\n" + "="*80)
        print("📊 DATA QUALITY DASHBOARD")
        print("="*80)
        
        if table_name:
            # Single table dashboard
            summary = self.get_profile_summary(table_name)
            if summary:
                self._print_table_dashboard(summary)
        else:
            # All tables dashboard
            profiles = self.list_available_profiles()
            if not profiles:
                print("❌ No profiles found")
                return
            
            print(f"\n📋 Profile Overview ({len(profiles)} tables)")
            print("-" * 80)
            
            for profile in profiles:
                summary = self.get_profile_summary(profile["table_name"])
                if summary:
                    quality_emoji = "🟢" if summary.data_quality_score >= 80 else "🟡" if summary.data_quality_score >= 60 else "🔴"
                    print(f"{quality_emoji} {summary.table_name:<30} | Quality: {summary.data_quality_score:5.1f}% | Rows: {summary.row_count:>10,} | Cols: {summary.column_count:>3}")
    
    def _print_table_dashboard(self, summary: ProfileSummary):
        """Print detailed dashboard for a single table."""
        quality_emoji = "🟢" if summary.data_quality_score >= 80 else "🟡" if summary.data_quality_score >= 60 else "🔴"
        
        print(f"\n{quality_emoji} Table: {summary.table_name}")
        print(f"📅 Scanned: {summary.scan_timestamp}")
        print(f"📊 Dimensions: {summary.row_count:,} rows × {summary.column_count} columns")
        print(f"🎯 Quality Score: {summary.data_quality_score:.1f}/100")
        
        print(f"\n📈 Data Quality Metrics:")
        print(f"  • Average Completeness: {summary.completeness_avg:.1%}")
        print(f"  • Average Uniqueness: {summary.uniqueness_avg:.1%}")
        
        print(f"\n🏷️  Column Types:")
        for sem_type, count in sorted(summary.semantic_types.items()):
            print(f"  • {sem_type.replace('_', ' ').title()}: {count}")
        
        if summary.columns_with_issues:
            print(f"\n⚠️  Potential Issues ({len(summary.columns_with_issues)}):")
            for issue in summary.columns_with_issues[:10]:  # Show max 10
                print(f"  • {issue}")
            if len(summary.columns_with_issues) > 10:
                print(f"  • ... and {len(summary.columns_with_issues) - 10} more")
        else:
            print(f"\n✅ No significant data quality issues detected")
    
    def explore_column(self, table_name: str, column_name: str):
        """Detailed exploration of a specific column."""
        try:
            profiles_df = self.lakehouse.read_table(self.profile_table)
            profile_row = profiles_df.filter(F.col("table_name") == table_name).collect()
            
            if not profile_row:
                print(f"❌ explore_column - No profile found for table: {table_name}")
                return
            
            profile_data = json.loads(profile_row[0].profile_data)
            columns = profile_data.get("column_profiles", [])
            
            # Find the specific column
            col_data = None
            for col in columns:
                if col.get("column_name") == column_name:
                    col_data = col
                    break
            
            if not col_data:
                print(f"❌ Column '{column_name}' not found in table '{table_name}'")
                return
            
            print("\n" + "="*80)
            print(f"🔍 COLUMN ANALYSIS: {table_name}.{column_name}")
            print("="*80)
            
            print(f"\n📋 Basic Information:")
            print(f"  • Data Type: {col_data.get('data_type', 'unknown')}")
            print(f"  • Semantic Type: {col_data.get('semantic_type', 'unknown').replace('_', ' ').title()}")
            
            print(f"\n📊 Statistics:")
            print(f"  • Null Count: {col_data.get('null_count', 0):,} ({col_data.get('null_percentage', 0):.1f}%)")
            print(f"  • Distinct Count: ~{col_data.get('distinct_count', 0):,} ({col_data.get('distinct_percentage', 0):.1f}%)")
            print(f"  • Completeness: {col_data.get('completeness', 0):.1%}")
            print(f"  • Uniqueness: {col_data.get('uniqueness', 0):.1%}")
            
            # Type-specific details
            if col_data.get('min_value') is not None:
                print(f"  • Range: {col_data.get('min_value')} to {col_data.get('max_value')}")
            
            if col_data.get('mean_value') is not None:
                print(f"  • Mean: {col_data.get('mean_value'):.2f}")
                print(f"  • Std Dev: {col_data.get('std_dev', 0):.2f}")
            
            # Value statistics
            value_stats = col_data.get('value_statistics', {})
            if value_stats:
                print(f"\n🎯 Value Analysis:")
                if value_stats.get('is_unique_key'):
                    print("  • ✅ Appears to be a unique key")
                if value_stats.get('is_constant'):
                    print("  • ⚠️  Contains only constant values")
                
                length_stats = value_stats.get('value_length_stats', {})
                if length_stats:
                    print(f"  • String Length Range: {length_stats.get('min', 0)}-{length_stats.get('max', 0)} (avg: {length_stats.get('avg', 0):.1f})")
                
                sample_values = value_stats.get('sample_values', [])
                if sample_values and sample_values[0] is not None:
                    print(f"  • Sample Value: '{sample_values[0]}'")
            
            # Naming pattern analysis
            naming_pattern = col_data.get('naming_pattern', {})
            if naming_pattern and any(naming_pattern.values()):
                print(f"\n🏷️  Naming Analysis:")
                patterns = []
                if naming_pattern.get('is_id_column'): patterns.append("ID Column")
                if naming_pattern.get('is_foreign_key'): patterns.append("Foreign Key")
                if naming_pattern.get('is_timestamp'): patterns.append("Timestamp")
                if naming_pattern.get('is_status_flag'): patterns.append("Status/Flag")
                if naming_pattern.get('is_measurement'): patterns.append("Measurement")
                
                if patterns:
                    print(f"  • Detected Patterns: {', '.join(patterns)}")
                    
                confidence = naming_pattern.get('confidence', 0)
                if confidence > 0:
                    print(f"  • Pattern Confidence: {confidence:.1%}")
            
        except Exception as e:
            print(f"❌ Error exploring column: {e}")
    
    def find_columns_by_type(self, semantic_type: str) -> List[Dict[str, str]]:
        """Find all columns of a specific semantic type across all tables."""
        try:
            profiles_df = self.lakehouse.read_table(self.profile_table)
            all_profiles = profiles_df.collect()
            
            matching_columns = []
            
            for profile_row in all_profiles:
                profile_data = json.loads(profile_row.profile_data)
                table_name = profile_data.get("table_name", "unknown")
                
                for col in profile_data.get("column_profiles", []):
                    if col.get("semantic_type", "").lower() == semantic_type.lower():
                        matching_columns.append({
                            "table": table_name,
                            "column": col.get("column_name", "unknown"),
                            "data_type": col.get("data_type", "unknown"),
                            "distinct_count": col.get("distinct_count", 0),
                            "completeness": col.get("completeness", 0)
                        })
            
            return matching_columns
            
        except Exception as e:
            print(f"❌ Error finding columns: {e}")
            return []
    
    def show_columns_by_type(self, semantic_type: str):
        """Show all columns of a specific semantic type."""
        columns = self.find_columns_by_type(semantic_type)
        
        if not columns:
            print(f"❌ No columns found with semantic type: {semantic_type}")
            return
        
        print(f"\n📋 Columns with semantic type: {semantic_type.replace('_', ' ').title()}")
        print("-" * 80)
        
        for col in sorted(columns, key=lambda x: (x['table'], x['column'])):
            completeness_emoji = "✅" if col['completeness'] >= 0.95 else "⚠️" if col['completeness'] >= 0.8 else "❌"
            print(f"{completeness_emoji} {col['table']:<25}.{col['column']:<20} | {col['data_type']:<15} | ~{col['distinct_count']:>8,} distinct | {col['completeness']:>6.1%} complete")
    
    def get_data_quality_report(self, table_name: str) -> str:
        """Generate a comprehensive data quality report."""
        summary = self.get_profile_summary(table_name)
        if not summary:
            return f"❌ No profile data available for table: {table_name}"
        
        report = []
        report.append("="*80)
        report.append(f"DATA QUALITY REPORT: {table_name}")
        report.append("="*80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Profile Date: {summary.scan_timestamp}")
        report.append("")
        
        # Executive Summary
        report.append("EXECUTIVE SUMMARY")
        report.append("-" * 20)
        quality_rating = "Excellent" if summary.data_quality_score >= 90 else "Good" if summary.data_quality_score >= 80 else "Fair" if summary.data_quality_score >= 60 else "Poor"
        report.append(f"Overall Quality: {quality_rating} ({summary.data_quality_score:.1f}/100)")
        report.append(f"Dataset Size: {summary.row_count:,} rows × {summary.column_count} columns")
        report.append(f"Average Completeness: {summary.completeness_avg:.1%}")
        report.append(f"Average Uniqueness: {summary.uniqueness_avg:.1%}")
        report.append("")
        
        # Column Type Distribution
        report.append("COLUMN TYPE DISTRIBUTION")
        report.append("-" * 25)
        for sem_type, count in sorted(summary.semantic_types.items()):
            percentage = (count / summary.column_count) * 100
            report.append(f"{sem_type.replace('_', ' ').title():<15}: {count:>3} columns ({percentage:>5.1f}%)")
        report.append("")
        
        # Data Quality Issues
        if summary.columns_with_issues:
            report.append("DATA QUALITY ISSUES")
            report.append("-" * 20)
            for issue in summary.columns_with_issues:
                report.append(f"⚠️  {issue}")
            report.append("")
        else:
            report.append("✅ NO SIGNIFICANT DATA QUALITY ISSUES DETECTED")
            report.append("")
        
        # Recommendations
        report.append("RECOMMENDATIONS")
        report.append("-" * 15)
        if summary.data_quality_score < 80:
            report.append("• Review columns with high null percentages")
            report.append("• Investigate constant value columns")
            report.append("• Consider data validation rules")
        else:
            report.append("• Data quality appears good")
            report.append("• Consider monitoring for ongoing quality")
        
        if summary.completeness_avg < 0.9:
            report.append("• Address missing value issues")
        
        if len([t for t in summary.semantic_types.keys() if t == "unknown"]) > 0:
            report.append("• Review columns with unknown semantic types")
        
        report.append("")
        report.append("="*80)
        
        return "\n".join(report)
    
    def export_to_csv(self, table_name: str, output_file: str):
        """Export profile results to CSV format."""
        try:
            profiles_df = self.lakehouse.read_table(self.profile_table)
            profile_row = profiles_df.filter(F.col("table_name") == table_name).collect()
            
            if not profile_row:
                print(f"❌ export_to_csv - No profile found for table: {table_name}")
                return
            
            profile_data = json.loads(profile_row[0].profile_data)
            columns = profile_data.get("column_profiles", [])
            
            # Create CSV content
            csv_lines = []
            csv_lines.append("column_name,data_type,semantic_type,null_count,null_percentage,distinct_count,distinct_percentage,completeness,uniqueness,min_value,max_value,mean_value")
            
            for col in columns:
                csv_lines.append(
                    f"{col.get('column_name', '')},"
                    f"{col.get('data_type', '')},"
                    f"{col.get('semantic_type', '')},"
                    f"{col.get('null_count', 0)},"
                    f"{col.get('null_percentage', 0):.2f},"
                    f"{col.get('distinct_count', 0)},"
                    f"{col.get('distinct_percentage', 0):.2f},"
                    f"{col.get('completeness', 0):.4f},"
                    f"{col.get('uniqueness', 0):.4f},"
                    f"{col.get('min_value', '')},"
                    f"{col.get('max_value', '')},"
                    f"{col.get('mean_value', '')}"
                )
            
            with open(output_file, 'w') as f:
                f.write('\n'.join(csv_lines))
            
            print(f"✅ Profile data exported to: {output_file}")
            
        except Exception as e:
            print(f"❌ Error exporting to CSV: {e}")
    
    def search_tables_by_quality(self, min_quality_score: float = 80.0) -> List[str]:
        """Find tables that meet a minimum data quality threshold."""
        profiles = self.list_available_profiles()
        good_quality_tables = []
        
        for profile in profiles:
            summary = self.get_profile_summary(profile["table_name"])
            if summary and summary.data_quality_score >= min_quality_score:
                good_quality_tables.append(summary.table_name)
        
        return good_quality_tables
    
    def interactive_explore(self):
        """Start an interactive exploration session."""
        print("\n" + "="*80)
        print("🔍 INTERACTIVE PROFILE EXPLORER")
        print("="*80)
        print("Commands:")
        print("  'list' - Show all available profiles")
        print("  'dashboard [table_name]' - Show data quality dashboard")  
        print("  'column <table_name> <column_name>' - Explore specific column")
        print("  'type <semantic_type>' - Find columns by semantic type")
        print("  'quality <min_score>' - Find high-quality tables")
        print("  'report <table_name>' - Generate quality report")
        print("  'export <table_name> <file.csv>' - Export to CSV")
        print("  'help' - Show this help")
        print("  'quit' - Exit explorer")
        print("="*80)
        
        while True:
            try:
                command = input("\n🔍 > ").strip().lower()
                
                if command == 'quit' or command == 'exit':
                    print("👋 Goodbye!")
                    break
                elif command == 'list':
                    profiles = self.list_available_profiles()
                    print(f"\n📋 Available Profiles ({len(profiles)}):")
                    for p in profiles:
                        print(f"  • {p['table_name']} ({p['row_count']:,} rows, {p['column_count']} columns)")
                elif command.startswith('dashboard'):
                    parts = command.split()
                    table_name = parts[1] if len(parts) > 1 else None
                    self.show_data_quality_dashboard(table_name)
                elif command.startswith('column'):
                    parts = command.split()
                    if len(parts) >= 3:
                        self.explore_column(parts[1], parts[2])
                    else:
                        print("Usage: column <table_name> <column_name>")
                elif command.startswith('type'):
                    parts = command.split()
                    if len(parts) >= 2:
                        self.show_columns_by_type(parts[1])
                    else:
                        print("Usage: type <semantic_type>")
                elif command.startswith('quality'):
                    parts = command.split()
                    min_score = float(parts[1]) if len(parts) > 1 else 80.0
                    tables = self.search_tables_by_quality(min_score)
                    print(f"\n✅ Tables with quality score >= {min_score}:")
                    for table in tables:
                        print(f"  • {table}")
                elif command.startswith('report'):
                    parts = command.split()
                    if len(parts) >= 2:
                        report = self.get_data_quality_report(parts[1])
                        print(f"\n{report}")
                    else:
                        print("Usage: report <table_name>")
                elif command.startswith('export'):
                    parts = command.split()
                    if len(parts) >= 3:
                        self.export_to_csv(parts[1], parts[2])
                    else:
                        print("Usage: export <table_name> <file.csv>")
                elif command == 'help':
                    print(self.interactive_explore.__doc__)
                else:
                    print("Unknown command. Type 'help' for available commands.")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
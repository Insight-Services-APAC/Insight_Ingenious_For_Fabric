#!/usr/bin/env python3
"""
Profile Explorer - Easy exploration of L3 profile results.

This module provides various ways to explore and analyze the profile results
from the L3 scan, making it easy to understand data quality and characteristics.
"""

import json
import traceback
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..core.models.profile_models import DatasetProfile, ColumnProfile
from ..core.enums.profile_types import SemanticType
from .yaml_exporter import ProfileYamlExporter


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
    
    def __init__(self, lakehouse, persistence=None, table_prefix: str = "tiered_profile"):
        """
        Initialize the ProfileExplorer.
        
        Args:
            lakehouse: lakehouse_utils instance
            persistence: Optional persistence instance. If not provided, will create one.
            table_prefix: Prefix used for profile tables
        """
        self.lakehouse = lakehouse
        self.spark = lakehouse.spark
        
        # Use provided persistence or create new one
        if persistence:
            self.persistence = persistence
        else:
            from ..persistence.enhanced_lakehouse_persistence import EnhancedLakehousePersistence
            self.persistence = EnhancedLakehousePersistence(
                lakehouse=lakehouse,
                spark=self.spark,
                table_prefix=table_prefix
            )
        
        self.yaml_exporter = ProfileYamlExporter()
        
    def list_available_profiles(self) -> List[Dict[str, Any]]:
        """List all available profile results."""
        try:
            profiles = self.persistence.list_all_profiles()
            
            # Format timestamps
            for profile in profiles:
                if profile.get("scan_timestamp"):
                    profile["scan_timestamp"] = profile["scan_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            
            return profiles
        except Exception as e:
            tb = traceback.format_exc()
            print(f"⚠️ Could not load profiles: {e}\nTraceback:\n{tb}")
            return []
    
    def get_profile_summary(self, table_name: str) -> Optional[ProfileSummary]:
        """Get a high-level summary for a specific table."""
        try:
            profile = self.persistence.load_profile(table_name)
            
            if not profile:
                print(f"❌ get_profile_summary - No profile found for table: {table_name}")
                return None
            
            columns = profile.column_profiles
            
            if not columns:
                return None
            
            # Calculate summary metrics
            completeness_values = [col.completeness or 0 for col in columns]
            uniqueness_values = [col.uniqueness or 0 for col in columns]
            
            completeness_avg = sum(completeness_values) / len(completeness_values)
            uniqueness_avg = sum(uniqueness_values) / len(uniqueness_values)
            
            # Count semantic types
            semantic_types = {}
            columns_with_issues = []
            
            for col in columns:
                # Semantic type counts
                sem_type = col.semantic_type.value if col.semantic_type else "unknown"
                semantic_types[sem_type] = semantic_types.get(sem_type, 0) + 1
                
                # Identify potential issues
                if col.null_percentage > 50:
                    columns_with_issues.append(f"{col.column_name} (>50% nulls)")
                elif col.distinct_count == 1:
                    columns_with_issues.append(f"{col.column_name} (constant values)")
                elif col.completeness and col.completeness < 0.8:
                    columns_with_issues.append(f"{col.column_name} (<80% complete)")
            
            # Simple data quality score (0-100)
            quality_score = (completeness_avg * 0.6 + uniqueness_avg * 0.4) * 100
            
            return ProfileSummary(
                table_name=table_name,
                row_count=profile.row_count,
                column_count=profile.column_count,
                scan_timestamp=profile.profile_timestamp,
                data_quality_score=quality_score,
                completeness_avg=completeness_avg,
                uniqueness_avg=uniqueness_avg,
                semantic_types=semantic_types,
                columns_with_issues=columns_with_issues
            )
            
        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Error getting profile summary: {e}\nTraceback:\n{tb}")
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
            profile = self.persistence.load_profile(table_name)
            
            if not profile:
                print(f"❌ explore_column - No profile found for table: {table_name}")
                return
            
            # Find the specific column
            col_data = None
            for col in profile.column_profiles:
                if col.column_name == column_name:
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
            tb = traceback.format_exc()
            print(f"❌ Error exploring column: {e}\nTraceback:\n{tb}")
    
    def find_columns_by_type(self, semantic_type: str) -> List[Dict[str, str]]:
        """Find all columns of a specific semantic type across all tables."""
        try:
            profiles_df = self.lakehouse.read_table(self.profile_table)
            all_profiles = profiles_df.collect()
            
            matching_columns = []
            
            for profile_row in all_profiles:
                profile_data = json.loads(profile_row.profile_data)
                table_name = profile_data.get("dataset_name", "unknown")  # Updated field name
                
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
            tb = traceback.format_exc()
            print(f"❌ Error finding columns: {e}\nTraceback:\n{tb}")
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
            profile = self.persistence.load_profile(table_name)
            
            if not profile:
                print(f"❌ export_to_csv - No profile found for table: {table_name}")
                return
            
            columns = profile.column_profiles
            
            # Create CSV content
            csv_lines = []
            csv_lines.append("column_name,data_type,semantic_type,null_count,null_percentage,distinct_count,distinct_percentage,completeness,uniqueness,min_value,max_value,mean_value")
            
            for col in columns:
                sem_type = col.semantic_type.value if col.semantic_type else ''
                csv_lines.append(
                    f"{col.column_name},"
                    f"{col.data_type},"
                    f"{sem_type},"
                    f"{col.null_count},"
                    f"{col.null_percentage:.2f},"
                    f"{col.distinct_count},"
                    f"{col.distinct_percentage:.2f},"
                    f"{col.completeness or 0:.4f},"
                    f"{col.uniqueness or 0:.4f},"
                    f"{col.min_value or ''},"
                    f"{col.max_value or ''},"
                    f"{col.mean_value or ''}"
                )
            
            with open(output_file, 'w') as f:
                f.write('\n'.join(csv_lines))
            
            print(f"✅ Profile data exported to: {output_file}")
            
        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Error exporting to CSV: {e}\nTraceback:\n{tb}")
    
    def export_to_yaml(
        self, 
        table_name: str, 
        output_file: str,
        detail_level: str = "standard",
        llm_optimized: bool = False
    ):
        """
        Export profile results to YAML format.
        
        Args:
            table_name: Name of the table to export
            output_file: Path to save the YAML file
            detail_level: Level of detail ("summary", "standard", "full")
            llm_optimized: If True, uses LLM-optimized format
        """
        try:
            profile = self._load_profile_object(table_name)
            if not profile:
                print(f"❌ export_to_yaml - No profile found for table: {table_name}")
                return False
            
            if llm_optimized:
                self.yaml_exporter.export_llm_optimized(
                    profile, output_file
                )
                print(f"✅ LLM-optimized profile exported to: {output_file}")
            else:
                self.yaml_exporter.export_profile_to_yaml(
                    profile, output_file, detail_level
                )
                print(f"✅ Profile data exported to YAML: {output_file}")
            
            return True
                
        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Error exporting to YAML: {e}\nTraceback:\n{tb}")
            return False
    
    def export_schema_docs(
        self,
        table_name: str,
        output_file: str,
        format: str = "markdown"
    ):
        """
        Export profile as schema documentation.
        
        Args:
            table_name: Name of the table to document
            output_file: Path to save the documentation
            format: Documentation format ("markdown" or "yaml")
        """
        try:
            profile = self._load_profile_object(table_name)
            if not profile:
                print(f"❌ export_schema_docs - No profile found for table: {table_name}")
                return
            
            doc = self.yaml_exporter.export_schema_documentation(
                profile, output_file, format
            )
            print(f"✅ Schema documentation exported to: {output_file}")
            
        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Error exporting documentation: {e}\nTraceback:\n{tb}")
    
    def _load_profile_object(self, table_name: str) -> Optional[DatasetProfile]:
        """Load a DatasetProfile object from persistence."""
        try:
            print(f"🔄 _load_profile_object - Loading profile for table: {table_name}")
            return self.persistence.load_profile(table_name)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"❌ Error loading profile: {e}\nTraceback:\n{tb}")
            return None
    
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
        print("  'yaml <table_name> <file.yaml> [detail_level]' - Export to YAML")
        print("  'llm <table_name> <file.yaml>' - Export LLM-optimized YAML")
        print("  'docs <table_name> <file> [format]' - Export schema documentation")
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
                elif command.startswith('yaml'):
                    parts = command.split()
                    if len(parts) >= 3:
                        detail_level = parts[3] if len(parts) > 3 else "standard"
                        self.export_to_yaml(parts[1], parts[2], detail_level)
                    else:
                        print("Usage: yaml <table_name> <file.yaml> [detail_level]")
                elif command.startswith('llm'):
                    parts = command.split()
                    if len(parts) >= 3:
                        self.export_to_yaml(parts[1], parts[2], llm_optimized=True)
                    else:
                        print("Usage: llm <table_name> <file.yaml>")
                elif command.startswith('docs'):
                    parts = command.split()
                    if len(parts) >= 3:
                        format = parts[3] if len(parts) > 3 else "markdown"
                        self.export_schema_docs(parts[1], parts[2], format)
                    else:
                        print("Usage: docs <table_name> <file> [format]")
                elif command == 'help':
                    print("Available commands:")
                    print("  'list' - Show all available profiles")
                    print("  'dashboard [table_name]' - Show data quality dashboard")  
                    print("  'column <table_name> <column_name>' - Explore specific column")
                    print("  'type <semantic_type>' - Find columns by semantic type")
                    print("  'quality <min_score>' - Find high-quality tables")
                    print("  'report <table_name>' - Generate quality report")
                    print("  'export <table_name> <file.csv>' - Export to CSV")
                    print("  'yaml <table_name> <file.yaml> [detail_level]' - Export to YAML")
                    print("  'llm <table_name> <file.yaml>' - Export LLM-optimized YAML")
                    print("  'docs <table_name> <file> [format]' - Export schema documentation")
                    print("  'quit' - Exit explorer")
                else:
                    print("Unknown command. Type 'help' for available commands.")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                tb = traceback.format_exc()
                print(f"❌ Error: {e}\nTraceback:\n{tb}")
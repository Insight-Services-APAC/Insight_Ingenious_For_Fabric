#!/usr/bin/env python3
"""
YAML Exporter/Importer for Data Profiles.

This module provides functionality to export and import data profiles to/from YAML format,
making them portable and suitable for use with LLMs and other external tools.
"""

import json
import yaml
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..core.models.profile_models import DatasetProfile, ColumnProfile
from ..core.models.statistics import NamingPattern, ValuePattern, ValueStatistics, BusinessRule
from ..core.models.relationships import ColumnRelationship, EntityRelationshipGraph
from ..core.enums.profile_types import SemanticType


class ProfileYamlExporter:
    """
    Export and import data profiles to/from YAML format.
    
    Provides methods to:
    - Export profiles to YAML with various detail levels
    - Create LLM-optimized summaries
    - Import profiles from YAML
    - Generate schema documentation
    """
    
    def __init__(self, simplify_for_llm: bool = False):
        """
        Initialize the YAML exporter.
        
        Args:
            simplify_for_llm: If True, simplifies output for LLM consumption
        """
        self.simplify_for_llm = simplify_for_llm
    
    def export_profile_to_yaml(
        self, 
        profile: DatasetProfile, 
        output_path: Optional[str] = None,
        detail_level: str = "full"
    ) -> str:
        """
        Export a DatasetProfile to YAML format.
        
        Args:
            profile: The DatasetProfile to export
            output_path: Optional path to save the YAML file
            detail_level: Level of detail ("summary", "standard", "full")
        
        Returns:
            YAML string representation of the profile
        """
        if detail_level == "summary":
            data = self._create_summary_dict(profile)
        elif detail_level == "standard":
            data = self._create_standard_dict(profile)
        else:  # full
            data = self._create_full_dict(profile)
        
        # Convert to YAML
        yaml_str = yaml.dump(
            data, 
            default_flow_style=False, 
            sort_keys=False,
            allow_unicode=True,
            width=120
        )
        
        # Save to file if path provided
        if output_path:
            Path(output_path).write_text(yaml_str)
        
        return yaml_str
    
    def _create_summary_dict(self, profile: DatasetProfile) -> Dict[str, Any]:
        """Create a summary-level dictionary for YAML export."""
        summary = {
            'dataset': {
                'name': profile.dataset_name,
                'rows': profile.row_count,
                'columns': profile.column_count,
                'profile_date': profile.profile_timestamp,
                'quality_score': profile.data_quality_score
            },
            'quality_metrics': {
                'null_count': profile.null_count,
                'duplicate_count': profile.duplicate_count,
                'completeness': self._calculate_avg_completeness(profile),
                'uniqueness': self._calculate_avg_uniqueness(profile)
            },
            'columns_summary': []
        }
        
        # Add column summaries
        for col in profile.column_profiles[:20]:  # Limit to first 20 columns in summary
            col_summary = {
                'name': col.column_name,
                'type': col.data_type,
                'semantic_type': col.semantic_type.value if col.semantic_type else 'unknown',
                'null_percentage': round(col.null_percentage, 2),
                'distinct_count': col.distinct_count,
                'completeness': round(col.completeness, 3) if col.completeness else None
            }
            summary['columns_summary'].append(col_summary)
        
        if len(profile.column_profiles) > 20:
            summary['note'] = f"Showing 20 of {len(profile.column_profiles)} columns"
        
        return summary
    
    def _create_standard_dict(self, profile: DatasetProfile) -> Dict[str, Any]:
        """Create a standard-level dictionary for YAML export."""
        data = self._create_summary_dict(profile)
        
        # Add all columns with standard detail
        data['columns'] = []
        for col in profile.column_profiles:
            col_data = {
                'name': col.column_name,
                'data_type': col.data_type,
                'semantic_type': col.semantic_type.value if col.semantic_type else None,
                'statistics': {
                    'null_count': col.null_count,
                    'null_percentage': round(col.null_percentage, 2),
                    'distinct_count': col.distinct_count,
                    'distinct_percentage': round(col.distinct_percentage, 2),
                    'completeness': round(col.completeness, 3) if col.completeness else None,
                    'uniqueness': round(col.uniqueness, 3) if col.uniqueness else None
                }
            }
            
            # Add numeric statistics if available
            if col.min_value is not None:
                col_data['statistics']['range'] = {
                    'min': col.min_value,
                    'max': col.max_value
                }
            
            if col.mean_value is not None:
                col_data['statistics']['numeric'] = {
                    'mean': round(col.mean_value, 2) if col.mean_value else None,
                    'median': round(col.median_value, 2) if col.median_value else None,
                    'std_dev': round(col.std_dev, 2) if col.std_dev else None
                }
            
            # Add top values for categorical data
            if col.top_distinct_values:
                col_data['top_values'] = col.top_distinct_values[:5]
            
            data['columns'].append(col_data)
        
        # Remove the summary columns
        del data['columns_summary']
        
        return data
    
    def _create_full_dict(self, profile: DatasetProfile) -> Dict[str, Any]:
        """Create a full-detail dictionary for YAML export."""
        # Use the existing to_dict method and clean it up
        data = profile.to_dict()
        
        # Clean up for better YAML readability
        data = self._clean_dict_for_yaml(data)
        
        return data
    
    def _clean_dict_for_yaml(self, data: Any) -> Any:
        """Clean dictionary for better YAML output."""
        if isinstance(data, dict):
            cleaned = {}
            for key, value in data.items():
                if value is not None:  # Skip None values
                    cleaned[key] = self._clean_dict_for_yaml(value)
            return cleaned
        elif isinstance(data, list):
            return [self._clean_dict_for_yaml(item) for item in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, float):
            return round(data, 4)
        else:
            return data
    
    def export_llm_optimized(
        self, 
        profile: DatasetProfile,
        output_path: Optional[str] = None,
        max_columns: int = 30
    ) -> str:
        """
        Export profile in LLM-optimized format.
        
        Creates a concise, structured format that's easy for LLMs to parse
        and understand, focusing on the most important information.
        
        Args:
            profile: The DatasetProfile to export
            output_path: Optional path to save the YAML file
            max_columns: Maximum number of columns to include in detail
        
        Returns:
            YAML string optimized for LLM consumption
        """
        # Create LLM-optimized structure
        llm_data = {
            'table_schema': {
                'name': profile.dataset_name,
                'total_rows': profile.row_count,
                'total_columns': profile.column_count,
                'profile_timestamp': profile.profile_timestamp
            },
            'data_quality': {
                'overall_score': round(profile.data_quality_score, 1) if profile.data_quality_score else None,
                'issues_found': len(profile.data_quality_issues) if profile.data_quality_issues else 0,
                'recommendations': profile.recommendations[:3] if profile.recommendations else []
            },
            'column_definitions': []
        }
        
        # Group columns by semantic type for better organization
        columns_by_type = {}
        for col in profile.column_profiles[:max_columns]:
            sem_type = col.semantic_type.value if col.semantic_type else 'unknown'
            if sem_type not in columns_by_type:
                columns_by_type[sem_type] = []
            
            # Create concise column definition
            col_def = {
                'name': col.column_name,
                'type': col.data_type,
                'nullable': col.null_percentage > 0,
                'unique': col.uniqueness > 0.99 if col.uniqueness else False,
                'description': self._generate_column_description(col)
            }
            
            # Add sample values if available
            if col.value_statistics and col.value_statistics.sample_values:
                col_def['samples'] = col.value_statistics.sample_values[:3]
            
            columns_by_type[sem_type].append(col_def)
        
        # Organize columns by semantic type
        for sem_type, columns in columns_by_type.items():
            llm_data['column_definitions'].append({
                'semantic_category': sem_type.replace('_', ' ').title(),
                'columns': columns
            })
        
        # Add relationship information if available
        if profile.entity_relationships:
            llm_data['relationships'] = self._extract_key_relationships(profile)
        
        # Add business glossary if available
        if profile.business_glossary:
            llm_data['business_terms'] = dict(list(profile.business_glossary.items())[:10])
        
        # Convert to YAML
        yaml_str = yaml.dump(
            llm_data,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=100
        )
        
        # Add header comment for LLM context
        header = """# Data Profile for LLM Analysis
# This YAML contains structured metadata about the dataset
# Use this information to understand the data schema and quality

"""
        yaml_str = header + yaml_str
        
        if output_path:
            Path(output_path).write_text(yaml_str)
        
        return yaml_str
    
    def _generate_column_description(self, col: ColumnProfile) -> str:
        """Generate a natural language description of a column."""
        parts = []
        
        # Basic type info
        if col.semantic_type:
            parts.append(f"{col.semantic_type.value.replace('_', ' ').title()} field")
        else:
            parts.append(f"{col.data_type} field")
        
        # Null information
        if col.null_percentage > 50:
            parts.append("mostly null")
        elif col.null_percentage > 10:
            parts.append("partially null")
        elif col.null_percentage == 0:
            parts.append("never null")
        
        # Uniqueness
        if col.uniqueness and col.uniqueness > 0.99:
            parts.append("unique values")
        elif col.distinct_count == 1:
            parts.append("constant value")
        elif col.distinct_count < 10:
            parts.append(f"{col.distinct_count} distinct values")
        
        # Pattern information
        if col.naming_pattern:
            if col.naming_pattern.is_id_column:
                parts.append("likely an ID")
            elif col.naming_pattern.is_foreign_key:
                parts.append("likely a foreign key")
        
        return "; ".join(parts)
    
    def _extract_key_relationships(self, profile: DatasetProfile) -> List[Dict[str, Any]]:
        """Extract key relationships for LLM understanding."""
        relationships = []
        
        # Extract column relationships
        for col in profile.column_profiles:
            if col.relationships:
                for rel in col.relationships[:2]:  # Limit to 2 per column
                    relationships.append({
                        'type': rel.relationship_type,
                        'from': col.column_name,
                        'to': f"{rel.target_table}.{rel.target_column}",
                        'confidence': round(rel.confidence, 2)
                    })
        
        return relationships[:10]  # Limit total relationships
    
    def import_profile_from_yaml(self, yaml_path: str) -> DatasetProfile:
        """
        Import a DatasetProfile from a YAML file.
        
        Args:
            yaml_path: Path to the YAML file
        
        Returns:
            DatasetProfile object
        """
        yaml_str = Path(yaml_path).read_text()
        data = yaml.safe_load(yaml_str)
        
        # Handle different YAML formats
        if 'table_schema' in data:
            # LLM-optimized format
            return self._import_from_llm_format(data)
        elif 'dataset' in data and 'columns' not in data:
            # Summary format - can't fully reconstruct
            raise ValueError("Cannot fully reconstruct profile from summary format")
        else:
            # Standard or full format
            return DatasetProfile.from_dict(data)
    
    def _import_from_llm_format(self, data: Dict[str, Any]) -> DatasetProfile:
        """Import from LLM-optimized format."""
        # Reconstruct basic profile structure
        profile_data = {
            'dataset_name': data['table_schema']['name'],
            'row_count': data['table_schema']['total_rows'],
            'column_count': data['table_schema']['total_columns'],
            'profile_timestamp': data['table_schema']['profile_timestamp'],
            'data_quality_score': data['data_quality'].get('overall_score'),
            'recommendations': data['data_quality'].get('recommendations', []),
            'column_profiles': [],
            'business_glossary': data.get('business_terms', {})
        }
        
        # Reconstruct column profiles
        for category in data.get('column_definitions', []):
            for col in category['columns']:
                col_profile = {
                    'column_name': col['name'],
                    'data_type': col['type'],
                    'null_count': 0,  # Not available in LLM format
                    'null_percentage': 0.0 if not col.get('nullable') else 50.0,
                    'distinct_count': 0,
                    'distinct_percentage': 0.0,
                    'uniqueness': 1.0 if col.get('unique') else 0.5
                }
                profile_data['column_profiles'].append(col_profile)
        
        return DatasetProfile.from_dict(profile_data)
    
    def _calculate_avg_completeness(self, profile: DatasetProfile) -> float:
        """Calculate average completeness across all columns."""
        if not profile.column_profiles:
            return 0.0
        
        completeness_values = [
            col.completeness for col in profile.column_profiles 
            if col.completeness is not None
        ]
        
        if not completeness_values:
            return 0.0
        
        return round(sum(completeness_values) / len(completeness_values), 3)
    
    def _calculate_avg_uniqueness(self, profile: DatasetProfile) -> float:
        """Calculate average uniqueness across all columns."""
        if not profile.column_profiles:
            return 0.0
        
        uniqueness_values = [
            col.uniqueness for col in profile.column_profiles 
            if col.uniqueness is not None
        ]
        
        if not uniqueness_values:
            return 0.0
        
        return round(sum(uniqueness_values) / len(uniqueness_values), 3)
    
    def export_schema_documentation(
        self,
        profile: DatasetProfile,
        output_path: Optional[str] = None,
        format: str = "markdown"
    ) -> str:
        """
        Export profile as schema documentation.
        
        Args:
            profile: The DatasetProfile to export
            output_path: Optional path to save the documentation
            format: Format for documentation ("markdown" or "yaml")
        
        Returns:
            Documentation string
        """
        if format == "markdown":
            doc = self._generate_markdown_docs(profile)
        else:
            doc = self._generate_yaml_schema_docs(profile)
        
        if output_path:
            Path(output_path).write_text(doc)
        
        return doc
    
    def _generate_markdown_docs(self, profile: DatasetProfile) -> str:
        """Generate Markdown documentation from profile."""
        lines = []
        
        # Header
        lines.append(f"# {profile.dataset_name} Schema Documentation")
        lines.append("")
        lines.append(f"**Generated:** {datetime.now().isoformat()}")
        lines.append(f"**Profile Date:** {profile.profile_timestamp}")
        lines.append(f"**Total Records:** {profile.row_count:,}")
        lines.append(f"**Total Columns:** {profile.column_count}")
        lines.append("")
        
        # Data Quality Summary
        if profile.data_quality_score:
            lines.append("## Data Quality Summary")
            lines.append("")
            lines.append(f"- **Overall Score:** {profile.data_quality_score:.1f}/100")
            lines.append(f"- **Null Values:** {profile.null_count:,}" if profile.null_count else "- **Null Values:** N/A")
            lines.append(f"- **Duplicate Rows:** {profile.duplicate_count:,}" if profile.duplicate_count else "- **Duplicate Rows:** N/A")
            lines.append("")
        
        # Column Documentation
        lines.append("## Column Definitions")
        lines.append("")
        
        # Group by semantic type
        columns_by_type = {}
        for col in profile.column_profiles:
            sem_type = col.semantic_type.value if col.semantic_type else 'unknown'
            if sem_type not in columns_by_type:
                columns_by_type[sem_type] = []
            columns_by_type[sem_type].append(col)
        
        for sem_type, columns in sorted(columns_by_type.items()):
            lines.append(f"### {sem_type.replace('_', ' ').title()} Fields")
            lines.append("")
            
            for col in columns:
                lines.append(f"#### `{col.column_name}`")
                lines.append("")
                lines.append(f"- **Data Type:** {col.data_type}")
                lines.append(f"- **Nullable:** {'Yes' if col.null_percentage > 0 else 'No'}")
                lines.append(f"- **Distinct Values:** {col.distinct_count:,}")
                lines.append(f"- **Completeness:** {col.completeness:.1%}" if col.completeness else "- **Completeness:** N/A")
                
                if col.min_value is not None:
                    lines.append(f"- **Range:** {col.min_value} to {col.max_value}")
                
                if col.top_distinct_values:
                    lines.append(f"- **Sample Values:** {', '.join(str(v) for v in col.top_distinct_values[:3])}")
                
                lines.append("")
        
        return "\n".join(lines)
    
    def _generate_yaml_schema_docs(self, profile: DatasetProfile) -> str:
        """Generate YAML schema documentation."""
        schema = {
            'schema': {
                'name': profile.dataset_name,
                'version': '1.0.0',
                'generated': datetime.now().isoformat(),
                'profile_date': profile.profile_timestamp,
                'statistics': {
                    'row_count': profile.row_count,
                    'column_count': profile.column_count
                },
                'columns': {}
            }
        }
        
        for col in profile.column_profiles:
            schema['schema']['columns'][col.column_name] = {
                'type': col.data_type,
                'semantic_type': col.semantic_type.value if col.semantic_type else None,
                'nullable': col.null_percentage > 0,
                'unique': col.uniqueness > 0.99 if col.uniqueness else False,
                'statistics': {
                    'distinct_count': col.distinct_count,
                    'null_percentage': round(col.null_percentage, 2),
                    'completeness': round(col.completeness, 3) if col.completeness else None
                }
            }
            
            if col.min_value is not None:
                schema['schema']['columns'][col.column_name]['range'] = {
                    'min': col.min_value,
                    'max': col.max_value
                }
        
        return yaml.dump(schema, default_flow_style=False, sort_keys=False, width=120)
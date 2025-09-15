"""Naming pattern utilities for data profiling operations."""

import re
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class NamingConvention(Enum):
    """Standard naming conventions for database objects."""
    SNAKE_CASE = "snake_case"
    CAMEL_CASE = "camelCase" 
    PASCAL_CASE = "PascalCase"
    KEBAB_CASE = "kebab-case"
    UPPER_SNAKE_CASE = "UPPER_SNAKE_CASE"


class TableNamingPatterns:
    """Utilities for table naming patterns and conventions."""
    
    # Common table prefixes by domain
    COMMON_PREFIXES = {
        "fact": ["fact_", "f_", "fct_"],
        "dimension": ["dim_", "d_", "dimension_"],
        "staging": ["stg_", "staging_", "temp_", "tmp_"],
        "raw": ["raw_", "bronze_", "landing_"],
        "processed": ["processed_", "silver_", "curated_"],
        "mart": ["mart_", "gold_", "presentation_"],
        "metadata": ["meta_", "metadata_", "sys_"],
        "audit": ["audit_", "log_", "tracking_"],
        "backup": ["bak_", "backup_", "archive_"],
        "test": ["test_", "tst_", "dev_"]
    }
    
    # Common table suffixes
    COMMON_SUFFIXES = {
        "historical": ["_hist", "_historical", "_archive"],
        "snapshot": ["_snap", "_snapshot", "_scd2"],
        "aggregate": ["_agg", "_summary", "_rollup"],
        "delta": ["_delta", "_changes", "_diff"],
        "backup": ["_bak", "_backup"],
        "temp": ["_temp", "_tmp", "_staging"],
        "view": ["_vw", "_view"],
        "log": ["_log", "_audit", "_track"]
    }
    
    @classmethod
    def detect_naming_convention(cls, table_name: str) -> NamingConvention:
        """
        Detect the naming convention used in a table name.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Detected naming convention
        """
        if not table_name:
            return NamingConvention.SNAKE_CASE
        
        # Check for kebab-case (contains hyphens)
        if "-" in table_name and "_" not in table_name:
            return NamingConvention.KEBAB_CASE
        
        # Check for snake_case (contains underscores)
        if "_" in table_name:
            if table_name.isupper():
                return NamingConvention.UPPER_SNAKE_CASE
            else:
                return NamingConvention.SNAKE_CASE
        
        # Check for PascalCase (starts with uppercase)
        if table_name[0].isupper() and any(c.isupper() for c in table_name[1:]):
            return NamingConvention.PASCAL_CASE
        
        # Check for camelCase (starts with lowercase, has uppercase letters)
        if table_name[0].islower() and any(c.isupper() for c in table_name):
            return NamingConvention.CAMEL_CASE
        
        # Default to snake_case
        return NamingConvention.SNAKE_CASE
    
    @classmethod
    def analyze_table_name_pattern(cls, table_name: str) -> Dict[str, Any]:
        """
        Analyze a table name to identify patterns and components.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with pattern analysis
        """
        analysis = {
            "original_name": table_name,
            "naming_convention": cls.detect_naming_convention(table_name).value,
            "detected_prefix": None,
            "detected_suffix": None,
            "prefix_category": None,
            "suffix_category": None,
            "core_name": table_name,
            "length": len(table_name),
            "word_count": 1,
            "contains_numbers": bool(re.search(r'\d', table_name)),
            "contains_special_chars": bool(re.search(r'[^a-zA-Z0-9_-]', table_name))
        }
        
        if not table_name:
            return analysis
        
        # Normalize for analysis
        normalized_name = table_name.lower()
        
        # Detect prefix
        for category, prefixes in cls.COMMON_PREFIXES.items():
            for prefix in prefixes:
                if normalized_name.startswith(prefix):
                    analysis["detected_prefix"] = prefix
                    analysis["prefix_category"] = category
                    analysis["core_name"] = table_name[len(prefix):]
                    break
            if analysis["detected_prefix"]:
                break
        
        # Detect suffix
        for category, suffixes in cls.COMMON_SUFFIXES.items():
            for suffix in suffixes:
                if normalized_name.endswith(suffix):
                    analysis["detected_suffix"] = suffix
                    analysis["suffix_category"] = category
                    # Adjust core name if we found a suffix
                    if analysis["detected_prefix"]:
                        core_start = len(analysis["detected_prefix"])
                        core_end = len(table_name) - len(suffix)
                        analysis["core_name"] = table_name[core_start:core_end]
                    else:
                        analysis["core_name"] = table_name[:-len(suffix)]
                    break
            if analysis["detected_suffix"]:
                break
        
        # Count words based on naming convention
        convention = cls.detect_naming_convention(table_name)
        if convention in [NamingConvention.SNAKE_CASE, NamingConvention.UPPER_SNAKE_CASE]:
            analysis["word_count"] = len(table_name.split('_'))
        elif convention == NamingConvention.KEBAB_CASE:
            analysis["word_count"] = len(table_name.split('-'))
        elif convention in [NamingConvention.CAMEL_CASE, NamingConvention.PASCAL_CASE]:
            # Count capital letters as word boundaries
            analysis["word_count"] = len(re.findall(r'[A-Z]', table_name)) + 1
        
        return analysis
    
    @classmethod
    def suggest_table_name_improvements(cls, table_name: str) -> List[Dict[str, str]]:
        """
        Suggest improvements for table naming.
        
        Args:
            table_name: Current table name
            
        Returns:
            List of improvement suggestions
        """
        suggestions = []
        analysis = cls.analyze_table_name_pattern(table_name)
        
        # Length suggestions
        if analysis["length"] > 64:
            suggestions.append({
                "type": "length",
                "message": "Table name is very long (>64 chars), consider shortening",
                "severity": "warning"
            })
        elif analysis["length"] > 128:
            suggestions.append({
                "type": "length",
                "message": "Table name exceeds common database limits (>128 chars)",
                "severity": "error"
            })
        
        # Special character suggestions
        if analysis["contains_special_chars"]:
            suggestions.append({
                "type": "special_chars",
                "message": "Table name contains special characters, use only letters, numbers, and underscores",
                "severity": "warning"
            })
        
        # Naming convention consistency
        if not analysis["detected_prefix"] and not analysis["detected_suffix"]:
            suggestions.append({
                "type": "pattern",
                "message": "Consider using prefixes/suffixes to indicate table purpose (e.g., dim_, fact_, stg_)",
                "severity": "info"
            })
        
        # Word count suggestions
        if analysis["word_count"] == 1:
            suggestions.append({
                "type": "descriptiveness",
                "message": "Single-word table names may not be descriptive enough",
                "severity": "info"
            })
        elif analysis["word_count"] > 5:
            suggestions.append({
                "type": "descriptiveness",
                "message": "Table name has many words, consider simplification",
                "severity": "info"
            })
        
        return suggestions


class ColumnNamingPatterns:
    """Utilities for column naming patterns and conventions."""
    
    # Common column patterns
    COMMON_COLUMN_PATTERNS = {
        "id_columns": [r".*_id$", r"^id$", r".*_key$", r"^key$"],
        "timestamp_columns": [
            r".*_date$", r".*_time$", r".*_timestamp$", r"^created_at$", 
            r"^updated_at$", r"^deleted_at$", r".*_dt$", r".*_ts$"
        ],
        "flag_columns": [
            r"^is_.*", r"^has_.*", r".*_flag$", r".*_ind$", 
            r"^active$", r"^enabled$", r"^deleted$"
        ],
        "count_columns": [r".*_count$", r".*_cnt$", r"^count_.*", r"^num_.*"],
        "amount_columns": [
            r".*_amount$", r".*_total$", r".*_sum$", r".*_value$",
            r"^amount$", r"^total$", r"^price$", r"^cost$"
        ],
        "name_columns": [
            r".*_name$", r"^name$", r".*_desc$", r".*_description$", 
            r"^title$", r"^label$"
        ]
    }
    
    # Reserved keywords to avoid
    RESERVED_KEYWORDS = {
        "sql_keywords": {
            "select", "from", "where", "insert", "update", "delete", "create", 
            "drop", "alter", "table", "index", "view", "database", "schema",
            "union", "join", "inner", "outer", "left", "right", "on", "as",
            "group", "order", "by", "having", "distinct", "count", "sum",
            "avg", "min", "max", "case", "when", "then", "else", "end"
        },
        "spark_keywords": {
            "partition", "location", "format", "options", "refresh", "analyze",
            "compute", "statistics", "show", "describe", "explain"
        },
        "python_keywords": {
            "and", "or", "not", "is", "in", "for", "if", "else", "elif",
            "while", "def", "class", "import", "from", "as", "try", "except",
            "finally", "with", "lambda", "global", "nonlocal"
        }
    }
    
    @classmethod
    def categorize_column_name(cls, column_name: str) -> List[str]:
        """
        Categorize a column name based on common patterns.
        
        Args:
            column_name: Name of the column
            
        Returns:
            List of categories that match the column name
        """
        categories = []
        normalized_name = column_name.lower()
        
        for category, patterns in cls.COMMON_COLUMN_PATTERNS.items():
            for pattern in patterns:
                if re.match(pattern, normalized_name):
                    categories.append(category)
                    break
        
        return categories if categories else ["general"]
    
    @classmethod
    def analyze_column_name_pattern(cls, column_name: str) -> Dict[str, Any]:
        """
        Analyze a column name to identify patterns and potential issues.
        
        Args:
            column_name: Name of the column
            
        Returns:
            Dictionary with pattern analysis
        """
        analysis = {
            "original_name": column_name,
            "naming_convention": TableNamingPatterns.detect_naming_convention(column_name).value,
            "categories": cls.categorize_column_name(column_name),
            "length": len(column_name),
            "word_count": 1,
            "contains_numbers": bool(re.search(r'\d', column_name)),
            "contains_special_chars": bool(re.search(r'[^a-zA-Z0-9_]', column_name)),
            "is_reserved_keyword": cls.is_reserved_keyword(column_name),
            "reserved_keyword_type": None
        }
        
        # Check for reserved keywords
        if analysis["is_reserved_keyword"]:
            analysis["reserved_keyword_type"] = cls.get_reserved_keyword_type(column_name)
        
        # Count words
        convention = TableNamingPatterns.detect_naming_convention(column_name)
        if convention in [NamingConvention.SNAKE_CASE, NamingConvention.UPPER_SNAKE_CASE]:
            analysis["word_count"] = len(column_name.split('_'))
        elif convention == NamingConvention.KEBAB_CASE:
            analysis["word_count"] = len(column_name.split('-'))
        elif convention in [NamingConvention.CAMEL_CASE, NamingConvention.PASCAL_CASE]:
            analysis["word_count"] = len(re.findall(r'[A-Z]', column_name)) + 1
        
        return analysis
    
    @classmethod
    def is_reserved_keyword(cls, column_name: str) -> bool:
        """Check if a column name is a reserved keyword."""
        normalized_name = column_name.lower()
        
        for keyword_set in cls.RESERVED_KEYWORDS.values():
            if normalized_name in keyword_set:
                return True
        
        return False
    
    @classmethod
    def get_reserved_keyword_type(cls, column_name: str) -> Optional[str]:
        """Get the type of reserved keyword."""
        normalized_name = column_name.lower()
        
        for keyword_type, keyword_set in cls.RESERVED_KEYWORDS.items():
            if normalized_name in keyword_set:
                return keyword_type
        
        return None
    
    @classmethod
    def suggest_column_name_improvements(cls, column_name: str) -> List[Dict[str, str]]:
        """
        Suggest improvements for column naming.
        
        Args:
            column_name: Current column name
            
        Returns:
            List of improvement suggestions
        """
        suggestions = []
        analysis = cls.analyze_column_name_pattern(column_name)
        
        # Reserved keyword warnings
        if analysis["is_reserved_keyword"]:
            suggestions.append({
                "type": "reserved_keyword",
                "message": f"Column name '{column_name}' is a reserved {analysis['reserved_keyword_type']} keyword",
                "severity": "error",
                "suggested_fix": f"Consider renaming to '{column_name}_col' or '{column_name}_value'"
            })
        
        # Length suggestions
        if analysis["length"] > 64:
            suggestions.append({
                "type": "length",
                "message": "Column name is very long (>64 chars), consider shortening",
                "severity": "warning"
            })
        
        # Special character suggestions
        if analysis["contains_special_chars"]:
            suggestions.append({
                "type": "special_chars",
                "message": "Column name contains special characters, use only letters, numbers, and underscores",
                "severity": "warning"
            })
        
        # Single letter column names
        if analysis["length"] == 1:
            suggestions.append({
                "type": "descriptiveness",
                "message": "Single-letter column names are not descriptive",
                "severity": "warning"
            })
        
        # Category-specific suggestions
        if "general" in analysis["categories"] and analysis["word_count"] == 1:
            suggestions.append({
                "type": "descriptiveness", 
                "message": "Consider using more descriptive column names",
                "severity": "info"
            })
        
        return suggestions


class ProfileNamingUtils:
    """Utilities for profiling-specific naming operations."""
    
    @classmethod
    def generate_profile_table_name(
        cls, 
        source_table: str,
        profile_type: str = "profile",
        timestamp: Optional[datetime] = None,
        convention: NamingConvention = NamingConvention.SNAKE_CASE
    ) -> str:
        """
        Generate a standardized name for a profile table.
        
        Args:
            source_table: Name of the source table being profiled
            profile_type: Type of profile (profile, metadata, quality, etc.)
            timestamp: Optional timestamp to include
            convention: Naming convention to use
            
        Returns:
            Generated profile table name
        """
        # Clean source table name
        clean_source = re.sub(r'[^a-zA-Z0-9_]', '_', source_table)
        
        # Build components
        components = [profile_type, clean_source]
        
        if timestamp:
            timestamp_str = timestamp.strftime("%Y%m%d")
            components.append(timestamp_str)
        
        # Join based on convention
        if convention == NamingConvention.SNAKE_CASE:
            return "_".join(components).lower()
        elif convention == NamingConvention.UPPER_SNAKE_CASE:
            return "_".join(components).upper()
        elif convention == NamingConvention.KEBAB_CASE:
            return "-".join(components).lower()
        elif convention == NamingConvention.CAMEL_CASE:
            result = components[0].lower()
            for component in components[1:]:
                result += component.capitalize()
            return result
        elif convention == NamingConvention.PASCAL_CASE:
            return "".join(component.capitalize() for component in components)
        
        return "_".join(components).lower()  # Default to snake_case
    
    @classmethod
    def generate_column_profile_name(cls, column_name: str, metric: str) -> str:
        """
        Generate a standardized name for a column profile metric.
        
        Args:
            column_name: Name of the column
            metric: Name of the metric
            
        Returns:
            Generated profile column name
        """
        # Clean names
        clean_column = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
        clean_metric = re.sub(r'[^a-zA-Z0-9_]', '_', metric)
        
        return f"{clean_column}_{clean_metric}".lower()
    
    @classmethod
    def extract_table_components(cls, table_name: str) -> Dict[str, Optional[str]]:
        """
        Extract components from a table name (database, schema, table).
        
        Args:
            table_name: Full table name (may include database.schema.table)
            
        Returns:
            Dictionary with database, schema, and table components
        """
        components = {
            "database": None,
            "schema": None, 
            "table": table_name,
            "full_name": table_name
        }
        
        # Split on dots
        parts = table_name.split('.')
        
        if len(parts) == 3:
            components.update({
                "database": parts[0],
                "schema": parts[1],
                "table": parts[2]
            })
        elif len(parts) == 2:
            components.update({
                "schema": parts[0],
                "table": parts[1]
            })
        
        return components
    
    @classmethod
    def standardize_names_in_list(
        cls, 
        names: List[str], 
        target_convention: NamingConvention = NamingConvention.SNAKE_CASE
    ) -> Dict[str, str]:
        """
        Standardize a list of names to a target naming convention.
        
        Args:
            names: List of names to standardize
            target_convention: Target naming convention
            
        Returns:
            Dictionary mapping original names to standardized names
        """
        mapping = {}
        
        for name in names:
            standardized = cls._convert_naming_convention(name, target_convention)
            mapping[name] = standardized
        
        return mapping
    
    @classmethod
    def _convert_naming_convention(cls, name: str, target_convention: NamingConvention) -> str:
        """Convert a name to the target naming convention."""
        if not name:
            return name
        
        # First, split the name into words
        words = cls._extract_words_from_name(name)
        
        # Then join according to target convention
        if target_convention == NamingConvention.SNAKE_CASE:
            return "_".join(word.lower() for word in words)
        elif target_convention == NamingConvention.UPPER_SNAKE_CASE:
            return "_".join(word.upper() for word in words)
        elif target_convention == NamingConvention.KEBAB_CASE:
            return "-".join(word.lower() for word in words)
        elif target_convention == NamingConvention.CAMEL_CASE:
            if not words:
                return name.lower()
            result = words[0].lower()
            for word in words[1:]:
                result += word.capitalize()
            return result
        elif target_convention == NamingConvention.PASCAL_CASE:
            return "".join(word.capitalize() for word in words)
        
        return name
    
    @classmethod
    def _extract_words_from_name(cls, name: str) -> List[str]:
        """Extract words from a name regardless of current convention."""
        if not name:
            return []
        
        # Handle snake_case and UPPER_SNAKE_CASE
        if "_" in name:
            return [word for word in name.split("_") if word]
        
        # Handle kebab-case
        if "-" in name:
            return [word for word in name.split("-") if word]
        
        # Handle camelCase and PascalCase
        # Split on capital letters but keep them
        words = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)', name)
        if words:
            return words
        
        # Fallback: treat as single word
        return [name]
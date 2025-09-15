"""Data type detection and validation utilities for profiling."""

import re
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)


class DataTypeCategory(Enum):
    """High-level data type categories for profiling."""
    NUMERIC = "numeric"
    STRING = "string"
    TEMPORAL = "temporal"
    BOOLEAN = "boolean"
    COMPLEX = "complex"
    UNKNOWN = "unknown"


class DataTypeUtils:
    """Utilities for data type detection and categorization."""
    
    # Type mappings for various string representations
    NUMERIC_TYPES = {
        "int", "integer", "bigint", "long", "float", "double", "decimal", "numeric",
        "tinyint", "smallint", "mediumint", "real", "money", "number"
    }
    
    STRING_TYPES = {
        "string", "varchar", "char", "text", "nvarchar", "nchar", "ntext",
        "clob", "blob", "binary", "varbinary"
    }
    
    TEMPORAL_TYPES = {
        "timestamp", "datetime", "date", "time", "timestamptype", "datetype",
        "timestamp_ntz", "timestamp_ltz", "interval"
    }
    
    BOOLEAN_TYPES = {
        "boolean", "bool", "bit"
    }
    
    COMPLEX_TYPES = {
        "array", "map", "struct", "json", "xml"
    }
    
    @classmethod
    def get_data_type_category(cls, data_type: str) -> DataTypeCategory:
        """
        Get the high-level category for a data type.
        
        Args:
            data_type: String representation of the data type
            
        Returns:
            DataTypeCategory enum value
        """
        # Normalize the type string
        normalized_type = cls.normalize_type_string(data_type)
        
        # Check each category
        if normalized_type in cls.NUMERIC_TYPES:
            return DataTypeCategory.NUMERIC
        elif normalized_type in cls.STRING_TYPES:
            return DataTypeCategory.STRING
        elif normalized_type in cls.TEMPORAL_TYPES:
            return DataTypeCategory.TEMPORAL
        elif normalized_type in cls.BOOLEAN_TYPES:
            return DataTypeCategory.BOOLEAN
        elif normalized_type in cls.COMPLEX_TYPES:
            return DataTypeCategory.COMPLEX
        else:
            return DataTypeCategory.UNKNOWN
    
    @classmethod
    def normalize_type_string(cls, data_type: str) -> str:
        """
        Normalize a data type string for comparison.
        
        Args:
            data_type: Raw data type string
            
        Returns:
            Normalized type string (lowercase, simplified)
        """
        if not data_type:
            return "unknown"
        
        # Convert to lowercase and remove extra whitespace
        normalized = data_type.lower().strip()
        
        # Remove common prefixes/suffixes
        normalized = re.sub(r'^(spark\.|sql\.|types\.)', '', normalized)
        normalized = re.sub(r'type$', '', normalized)
        
        # Handle parentheses for precision/scale
        normalized = re.sub(r'\([^)]*\)', '', normalized)
        
        # Handle array/map/struct types
        if 'array<' in normalized:
            return "array"
        elif 'map<' in normalized:
            return "map"
        elif 'struct<' in normalized:
            return "struct"
        
        return normalized.strip()
    
    @classmethod
    def is_numeric_type(cls, data_type: str) -> bool:
        """Check if a data type is numeric."""
        return cls.get_data_type_category(data_type) == DataTypeCategory.NUMERIC
    
    @classmethod
    def is_string_type(cls, data_type: str) -> bool:
        """Check if a data type is string/text."""
        return cls.get_data_type_category(data_type) == DataTypeCategory.STRING
    
    @classmethod
    def is_temporal_type(cls, data_type: str) -> bool:
        """Check if a data type is temporal (date/time)."""
        return cls.get_data_type_category(data_type) == DataTypeCategory.TEMPORAL
    
    @classmethod
    def is_boolean_type(cls, data_type: str) -> bool:
        """Check if a data type is boolean."""
        return cls.get_data_type_category(data_type) == DataTypeCategory.BOOLEAN
    
    @classmethod
    def is_complex_type(cls, data_type: str) -> bool:
        """Check if a data type is complex (array, map, struct)."""
        return cls.get_data_type_category(data_type) == DataTypeCategory.COMPLEX
    
    @classmethod
    def can_calculate_statistics(cls, data_type: str) -> bool:
        """Check if statistical calculations can be performed on this data type."""
        category = cls.get_data_type_category(data_type)
        return category in [DataTypeCategory.NUMERIC, DataTypeCategory.TEMPORAL]
    
    @classmethod
    def can_calculate_min_max(cls, data_type: str) -> bool:
        """Check if min/max calculations can be performed on this data type."""
        category = cls.get_data_type_category(data_type)
        return category in [
            DataTypeCategory.NUMERIC, 
            DataTypeCategory.TEMPORAL, 
            DataTypeCategory.STRING
        ]
    
    @classmethod
    def supports_aggregation(cls, data_type: str) -> bool:
        """Check if the data type supports aggregation functions."""
        return cls.can_calculate_statistics(data_type)
    
    @classmethod
    def get_spark_data_type(cls, type_string: str) -> Optional[DataType]:
        """
        Convert a type string to a Spark DataType object.
        
        Args:
            type_string: String representation of the data type
            
        Returns:
            Spark DataType object or None if not recognized
        """
        normalized = cls.normalize_type_string(type_string)
        
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "integer": IntegerType(),
            "bigint": LongType(),
            "long": LongType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "bool": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "decimal": DecimalType(),
        }
        
        return type_mapping.get(normalized)
    
    @classmethod
    def get_type_compatibility_score(cls, type1: str, type2: str) -> float:
        """
        Calculate a compatibility score between two data types.
        
        Args:
            type1: First data type
            type2: Second data type
            
        Returns:
            Compatibility score from 0.0 (incompatible) to 1.0 (identical)
        """
        if type1 == type2:
            return 1.0
        
        cat1 = cls.get_data_type_category(type1)
        cat2 = cls.get_data_type_category(type2)
        
        if cat1 == cat2:
            # Same category, high compatibility
            return 0.8
        
        # Check for numeric compatibility
        if cat1 == DataTypeCategory.NUMERIC and cat2 == DataTypeCategory.NUMERIC:
            return 0.9
        
        # String types are somewhat compatible with everything for display
        if cat1 == DataTypeCategory.STRING or cat2 == DataTypeCategory.STRING:
            return 0.3
        
        return 0.0
    
    @classmethod
    def suggest_data_type_improvements(cls, data_type: str, sample_values: List[str]) -> Dict[str, any]:
        """
        Suggest potential data type improvements based on sample values.
        
        Args:
            data_type: Current data type
            sample_values: Sample values from the column
            
        Returns:
            Dictionary with improvement suggestions
        """
        suggestions = {
            "current_type": data_type,
            "suggested_type": None,
            "confidence": 0.0,
            "reasons": []
        }
        
        if not sample_values:
            return suggestions
        
        # If currently string, check if it could be a more specific type
        if cls.is_string_type(data_type):
            # Check if all values are numeric
            numeric_count = 0
            date_count = 0
            boolean_count = 0
            
            for value in sample_values:
                if value is None:
                    continue
                    
                value_str = str(value).strip()
                if not value_str:
                    continue
                
                # Check for numeric
                try:
                    float(value_str)
                    numeric_count += 1
                    continue
                except ValueError:
                    pass
                
                # Check for boolean
                if value_str.lower() in ['true', 'false', 'yes', 'no', '1', '0']:
                    boolean_count += 1
                    continue
                
                # Check for date patterns
                if re.match(r'\d{4}-\d{2}-\d{2}', value_str) or re.match(r'\d{2}/\d{2}/\d{4}', value_str):
                    date_count += 1
                    continue
            
            total_values = len([v for v in sample_values if v is not None and str(v).strip()])
            
            if total_values > 0:
                numeric_ratio = numeric_count / total_values
                boolean_ratio = boolean_count / total_values
                date_ratio = date_count / total_values
                
                if numeric_ratio > 0.8:
                    suggestions["suggested_type"] = "double"
                    suggestions["confidence"] = numeric_ratio
                    suggestions["reasons"].append(f"{numeric_ratio:.1%} of values are numeric")
                elif boolean_ratio > 0.8:
                    suggestions["suggested_type"] = "boolean"
                    suggestions["confidence"] = boolean_ratio
                    suggestions["reasons"].append(f"{boolean_ratio:.1%} of values are boolean")
                elif date_ratio > 0.8:
                    suggestions["suggested_type"] = "date"
                    suggestions["confidence"] = date_ratio
                    suggestions["reasons"].append(f"{date_ratio:.1%} of values match date patterns")
        
        return suggestions


class SparkDataTypeAnalyzer:
    """Analyzer for Spark DataFrame data types."""
    
    @staticmethod
    def analyze_schema_types(schema: StructType) -> Dict[str, any]:
        """
        Analyze the data types in a Spark DataFrame schema.
        
        Args:
            schema: Spark StructType schema
            
        Returns:
            Dictionary with type analysis results
        """
        analysis = {
            "total_columns": len(schema.fields),
            "type_distribution": {},
            "category_distribution": {},
            "complex_columns": [],
            "nullable_columns": [],
            "non_nullable_columns": []
        }
        
        for field in schema.fields:
            # Get type string
            type_str = str(field.dataType)
            category = DataTypeUtils.get_data_type_category(type_str)
            
            # Update distributions
            analysis["type_distribution"][type_str] = analysis["type_distribution"].get(type_str, 0) + 1
            analysis["category_distribution"][category.value] = analysis["category_distribution"].get(category.value, 0) + 1
            
            # Track complex columns
            if DataTypeUtils.is_complex_type(type_str):
                analysis["complex_columns"].append(field.name)
            
            # Track nullable columns
            if field.nullable:
                analysis["nullable_columns"].append(field.name)
            else:
                analysis["non_nullable_columns"].append(field.name)
        
        return analysis
"""
Notebook Parameter Validation System

This module provides validation for runtime parameters used in generic synthetic data notebooks.
"""

import json
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
from .synthetic_data_dataset_configs import DatasetConfigurationRepository


class NotebookParameterValidator:
    """Validates parameters for generic synthetic data notebooks."""
    
    @staticmethod
    def validate_dataset_id(dataset_id: str) -> Tuple[bool, Optional[str]]:
        """
        Validate that dataset_id exists in the repository.
        
        Returns:
            (is_valid, error_message)
        """
        try:
            DatasetConfigurationRepository.get_predefined_dataset(dataset_id)
            return True, None
        except ValueError:
            available = DatasetConfigurationRepository.list_available_datasets()
            return False, f"Dataset '{dataset_id}' not found. Available: {list(available.keys())}"
    
    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> Tuple[bool, Optional[str]]:
        """
        Validate date format and range logic.
        
        Returns:
            (is_valid, error_message)
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            if end_dt < start_dt:
                return False, "End date must be after start date"
            
            # Check for reasonable date range (not too far in the future or past)
            today = date.today()
            if start_dt > today + timedelta(days=365):
                return False, "Start date cannot be more than 1 year in the future"
            
            if end_dt < today - timedelta(days=365*10):
                return False, "End date cannot be more than 10 years in the past"
                
            return True, None
            
        except ValueError as e:
            return False, f"Invalid date format. Use YYYY-MM-DD format. Error: {e}"
    
    @staticmethod
    def validate_batch_size(batch_size: int, date_range_days: int) -> Tuple[bool, Optional[str]]:
        """
        Validate batch size is reasonable for the date range.
        
        Returns:
            (is_valid, error_message)
        """
        if not isinstance(batch_size, int):
            return False, "Batch size must be an integer"
        
        if batch_size < 1:
            return False, "Batch size must be at least 1"
        
        if batch_size > 365:
            return False, "Batch size cannot exceed 365 days"
        
        if batch_size > date_range_days:
            return False, f"Batch size ({batch_size}) cannot exceed date range ({date_range_days} days)"
        
        return True, None
    
    @staticmethod
    def validate_output_mode(output_mode: str, target_environment: str) -> Tuple[bool, Optional[str]]:
        """
        Validate output mode is compatible with target environment.
        
        Returns:
            (is_valid, error_message)
        """
        valid_modes = ["table", "parquet", "csv"]
        
        if output_mode not in valid_modes:
            return False, f"Output mode must be one of: {valid_modes}"
        
        # Environment-specific validations
        if target_environment == "warehouse" and output_mode == "parquet":
            return False, "Parquet output mode may not be optimal for warehouse environment"
        
        return True, None
    
    @staticmethod
    def validate_path_format(path_format: str) -> Tuple[bool, Optional[str]]:
        """
        Validate path format option.
        
        Returns:
            (is_valid, error_message)
        """
        valid_formats = ["nested", "flat"]
        
        if path_format not in valid_formats:
            return False, f"Path format must be one of: {valid_formats}"
        
        return True, None
    
    @staticmethod
    def validate_generation_mode(generation_mode: str, target_environment: str) -> Tuple[bool, Optional[str]]:
        """
        Validate generation mode is compatible with environment.
        
        Returns:
            (is_valid, error_message)
        """
        valid_modes = ["python", "pyspark", "auto"]
        
        if generation_mode not in valid_modes:
            return False, f"Generation mode must be one of: {valid_modes}"
        
        # Environment-specific validations
        if target_environment == "warehouse" and generation_mode == "pyspark":
            return False, "PySpark mode not recommended for warehouse environment"
        
        return True, None
    
    @staticmethod
    def validate_target_rows(target_rows: int, generation_mode: str = "auto") -> Tuple[bool, Optional[str]]:
        """
        Validate target rows count is reasonable.
        
        Returns:
            (is_valid, error_message)
        """
        if not isinstance(target_rows, int):
            return False, "Target rows must be an integer"
        
        if target_rows < 1:
            return False, "Target rows must be at least 1"
        
        if target_rows > 1_000_000_000:  # 1 billion
            return False, "Target rows cannot exceed 1 billion"
        
        # Warn about performance implications
        if generation_mode == "python" and target_rows > 10_000_000:
            return False, "Python mode not recommended for more than 10 million rows. Use 'pyspark' or 'auto'"
        
        return True, None
    
    @staticmethod
    def validate_custom_schema(custom_schema: Optional[str]) -> Tuple[bool, Optional[str]]:
        """
        Validate custom schema JSON string if provided.
        
        Returns:
            (is_valid, error_message)
        """
        if custom_schema is None or custom_schema == "":
            return True, None
        
        try:
            schema_dict = json.loads(custom_schema)
            
            if not isinstance(schema_dict, dict):
                return False, "Custom schema must be a JSON object"
            
            # Basic validation of common schema fields
            if 'tables' in schema_dict:
                if not isinstance(schema_dict['tables'], list):
                    return False, "Schema 'tables' field must be an array"
            
            if 'dimensions' in schema_dict:
                if not isinstance(schema_dict['dimensions'], list):
                    return False, "Schema 'dimensions' field must be an array"
            
            return True, None
            
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON in custom schema: {e}"
    
    @staticmethod
    def validate_scale_factor(scale_factor: float) -> Tuple[bool, Optional[str]]:
        """
        Validate scale factor is within reasonable bounds.
        
        Returns:
            (is_valid, error_message)
        """
        if not isinstance(scale_factor, (int, float)):
            return False, "Scale factor must be a number"
        
        if scale_factor <= 0:
            return False, "Scale factor must be greater than 0"
        
        if scale_factor > 100:
            return False, "Scale factor cannot exceed 100"
        
        return True, None
    
    @classmethod
    def validate_incremental_series_parameters(
        cls, 
        parameters: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate all parameters for incremental series generation.
        
        Returns:
            (is_valid, list_of_error_messages)
        """
        errors = []
        
        # Required parameters
        required_params = ["dataset_id", "start_date", "end_date"]
        for param in required_params:
            if param not in parameters or parameters[param] is None:
                errors.append(f"Required parameter '{param}' is missing")
        
        if errors:  # Don't continue if required params are missing
            return False, errors
        
        # Validate dataset_id
        is_valid, error = cls.validate_dataset_id(parameters["dataset_id"])
        if not is_valid:
            errors.append(error)
        
        # Validate date range
        is_valid, error = cls.validate_date_range(
            parameters["start_date"], 
            parameters["end_date"]
        )
        if not is_valid:
            errors.append(error)
        else:
            # Calculate date range for batch size validation
            start_dt = datetime.strptime(parameters["start_date"], "%Y-%m-%d").date()
            end_dt = datetime.strptime(parameters["end_date"], "%Y-%m-%d").date()
            date_range_days = (end_dt - start_dt).days + 1
            
            # Validate batch size
            batch_size = parameters.get("batch_size", 10)
            is_valid, error = cls.validate_batch_size(batch_size, date_range_days)
            if not is_valid:
                errors.append(error)
        
        # Validate output mode
        output_mode = parameters.get("output_mode", "table")
        target_environment = parameters.get("target_environment", "lakehouse")
        is_valid, error = cls.validate_output_mode(output_mode, target_environment)
        if not is_valid:
            errors.append(error)
        
        # Validate path format
        path_format = parameters.get("path_format", "nested")
        is_valid, error = cls.validate_path_format(path_format)
        if not is_valid:
            errors.append(error)
        
        # Validate generation mode
        generation_mode = parameters.get("generation_mode", "auto")
        is_valid, error = cls.validate_generation_mode(generation_mode, target_environment)
        if not is_valid:
            errors.append(error)
        
        return len(errors) == 0, errors
    
    @classmethod
    def validate_single_dataset_parameters(
        cls, 
        parameters: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate all parameters for single dataset generation.
        
        Returns:
            (is_valid, list_of_error_messages)
        """
        errors = []
        
        # Required parameters
        required_params = ["dataset_id", "target_rows"]
        for param in required_params:
            if param not in parameters or parameters[param] is None:
                errors.append(f"Required parameter '{param}' is missing")
        
        if errors:  # Don't continue if required params are missing
            return False, errors
        
        # Validate dataset_id
        is_valid, error = cls.validate_dataset_id(parameters["dataset_id"])
        if not is_valid:
            errors.append(error)
        
        # Validate target rows
        generation_mode = parameters.get("generation_mode", "auto")
        is_valid, error = cls.validate_target_rows(
            parameters["target_rows"], 
            generation_mode
        )
        if not is_valid:
            errors.append(error)
        
        # Validate output mode
        output_mode = parameters.get("output_mode", "table")
        target_environment = parameters.get("target_environment", "lakehouse")
        is_valid, error = cls.validate_output_mode(output_mode, target_environment)
        if not is_valid:
            errors.append(error)
        
        # Validate generation mode
        is_valid, error = cls.validate_generation_mode(generation_mode, target_environment)
        if not is_valid:
            errors.append(error)
        
        # Validate scale factor
        scale_factor = parameters.get("scale_factor", 1.0)
        is_valid, error = cls.validate_scale_factor(scale_factor)
        if not is_valid:
            errors.append(error)
        
        # Validate custom schema
        custom_schema = parameters.get("custom_schema")
        is_valid, error = cls.validate_custom_schema(custom_schema)
        if not is_valid:
            errors.append(error)
        
        return len(errors) == 0, errors


class SmartDefaultProvider:
    """Provides intelligent defaults for notebook parameters."""
    
    @staticmethod
    def get_optimal_batch_size(date_range_days: int, target_rows_per_day: int = 100000) -> int:
        """
        Calculate optimal batch size based on data volume and date range.
        
        Args:
            date_range_days: Number of days in the date range
            target_rows_per_day: Estimated rows per day
            
        Returns:
            Optimal batch size in days
        """
        # Calculate total estimated rows
        total_estimated_rows = date_range_days * target_rows_per_day
        
        # For small datasets, process all at once
        if total_estimated_rows < 1_000_000:
            return date_range_days
        
        # For medium datasets, use smaller batches
        if total_estimated_rows < 10_000_000:
            return min(7, date_range_days)  # Weekly batches
        
        # For large datasets, use daily or small batches
        if total_estimated_rows < 100_000_000:
            return min(3, date_range_days)  # 3-day batches
        
        # For very large datasets, use daily batches
        return 1
    
    @staticmethod
    def suggest_generation_mode(
        target_rows: int, 
        target_environment: str,
        date_range_days: int = 1
    ) -> str:
        """
        Auto-suggest generation mode based on data size and environment.
        
        Args:
            target_rows: Target number of rows
            target_environment: Target environment ("lakehouse" or "warehouse")
            date_range_days: Number of days for incremental generation
            
        Returns:
            Suggested generation mode
        """
        total_rows = target_rows * date_range_days
        
        # For warehouse, prefer Python for better compatibility
        if target_environment == "warehouse":
            return "python" if total_rows < 10_000_000 else "pyspark"
        
        # For lakehouse, prefer PySpark for better performance
        return "pyspark" if total_rows > 1_000_000 else "python"
    
    @staticmethod
    def suggest_output_mode(
        target_rows: int,
        target_environment: str,
        use_case: str = "general"
    ) -> str:
        """
        Suggest optimal output mode based on requirements.
        
        Args:
            target_rows: Target number of rows
            target_environment: Target environment
            use_case: Use case ("analytics", "export", "general")
            
        Returns:
            Suggested output mode
        """
        if use_case == "export":
            return "csv" if target_rows < 1_000_000 else "parquet"
        
        if use_case == "analytics":
            return "parquet"
        
        # For general use, prefer tables for integration
        return "table"
    
    @staticmethod
    def get_parameter_recommendations(
        notebook_type: str,
        basic_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get comprehensive parameter recommendations for a notebook type.
        
        Args:
            notebook_type: Type of notebook ("incremental_series", "single_dataset")
            basic_params: Basic parameters provided by user
            
        Returns:
            Dictionary of recommended parameters
        """
        recommendations = basic_params.copy()
        
        if notebook_type == "incremental_series":
            # Calculate date range
            start_date = datetime.strptime(basic_params["start_date"], "%Y-%m-%d").date()
            end_date = datetime.strptime(basic_params["end_date"], "%Y-%m-%d").date()
            date_range_days = (end_date - start_date).days + 1
            
            # Recommend batch size
            if "batch_size" not in recommendations:
                recommendations["batch_size"] = SmartDefaultProvider.get_optimal_batch_size(
                    date_range_days
                )
            
            # Recommend generation mode
            if "generation_mode" not in recommendations:
                recommendations["generation_mode"] = SmartDefaultProvider.suggest_generation_mode(
                    basic_params.get("target_rows_per_day", 100000),
                    basic_params.get("target_environment", "lakehouse"),
                    date_range_days
                )
        
        elif notebook_type == "single_dataset":
            # Recommend generation mode
            if "generation_mode" not in recommendations:
                recommendations["generation_mode"] = SmartDefaultProvider.suggest_generation_mode(
                    basic_params["target_rows"],
                    basic_params.get("target_environment", "lakehouse")
                )
        
        # Common recommendations
        if "output_mode" not in recommendations:
            recommendations["output_mode"] = SmartDefaultProvider.suggest_output_mode(
                basic_params.get("target_rows", 100000),
                basic_params.get("target_environment", "lakehouse"),
                basic_params.get("use_case", "general")
            )
        
        return recommendations
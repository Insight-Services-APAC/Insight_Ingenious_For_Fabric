"""Abstract interface for data profiling implementations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..enums.profile_types import ProfileType
from ..models.profile_models import DatasetProfile, ColumnProfile


class DataProfilingInterface(ABC):
    """Abstract interface for data profiling implementations."""

    @abstractmethod
    def profile_dataset(
        self,
        dataset: Any,
        profile_type: ProfileType = ProfileType.BASIC,
        columns: Optional[List[str]] = None,
        sample_size: Optional[float] = None
    ) -> DatasetProfile:
        """
        Profile a dataset to generate statistics and quality metrics.
        
        Args:
            dataset: The dataset to profile (DataFrame, table name, or path)
            profile_type: Type of profiling to perform
            columns: Specific columns to profile (None = all columns)
            sample_size: Fraction of data to sample (None = full dataset)
            
        Returns:
            DatasetProfile containing profiling results
        """
        pass

    @abstractmethod
    def profile_column(
        self,
        dataset: Any,
        column_name: str,
        profile_type: ProfileType = ProfileType.BASIC
    ) -> ColumnProfile:
        """
        Profile a single column in detail.
        
        Args:
            dataset: The dataset containing the column
            column_name: Name of the column to profile
            profile_type: Type of profiling to perform
            
        Returns:
            ColumnProfile containing column-specific profiling results
        """
        pass

    @abstractmethod
    def compare_profiles(
        self,
        profile1: DatasetProfile,
        profile2: DatasetProfile
    ) -> Dict[str, Any]:
        """
        Compare two dataset profiles to identify changes.
        
        Args:
            profile1: First dataset profile
            profile2: Second dataset profile
            
        Returns:
            Dictionary containing comparison results and drift metrics
        """
        pass

    @abstractmethod
    def generate_quality_report(
        self,
        profile: DatasetProfile,
        output_format: str = "html"
    ) -> str:
        """
        Generate a formatted quality report from a profile.
        
        Args:
            profile: Dataset profile to report on
            output_format: Format for the report (html, json, markdown)
            
        Returns:
            Formatted report as string
        """
        pass

    @abstractmethod
    def suggest_data_quality_rules(
        self,
        profile: DatasetProfile
    ) -> List[Dict[str, Any]]:
        """
        Suggest data quality rules based on profiling results.
        
        Args:
            profile: Dataset profile to analyze
            
        Returns:
            List of suggested data quality rules
        """
        pass

    @abstractmethod
    def validate_against_rules(
        self,
        dataset: Any,
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Validate a dataset against a set of data quality rules.
        
        Args:
            dataset: Dataset to validate
            rules: List of data quality rules to apply
            
        Returns:
            Validation results including pass/fail status and violations
        """
        pass
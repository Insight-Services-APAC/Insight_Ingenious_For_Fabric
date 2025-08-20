"""Interface for data profiling implementations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum


class ProfileType(Enum):
    """Types of data profiles that can be generated."""
    BASIC = "basic"  # Row count, column count, basic stats
    STATISTICAL = "statistical"  # Mean, median, std dev, percentiles
    DATA_QUALITY = "data_quality"  # Nulls, duplicates, patterns
    DISTRIBUTION = "distribution"  # Value distributions, histograms
    CORRELATION = "correlation"  # Column correlations
    FULL = "full"  # All profile types


@dataclass
class ColumnProfile:
    """Profile information for a single column."""
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    completeness: Optional[float] = None
    uniqueness: Optional[float] = None
    entropy: Optional[float] = None
    value_distribution: Optional[Dict[Any, int]] = None


@dataclass
class DatasetProfile:
    """Complete profile information for a dataset."""
    dataset_name: str
    row_count: int
    column_count: int
    profile_timestamp: str
    column_profiles: List[ColumnProfile]
    data_quality_score: Optional[float] = None
    correlations: Optional[Dict[str, Dict[str, float]]] = None
    anomalies: Optional[List[Dict[str, Any]]] = None
    recommendations: Optional[List[str]] = None
    null_count: Optional[int] = None
    duplicate_count: Optional[int] = None
    statistics: Optional[Dict[str, Any]] = None
    data_quality_issues: Optional[List[Dict[str, Any]]] = None


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
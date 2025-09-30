"""
Abstract interface for synthetic data generators.

This module defines the base interface that all synthetic data generators must implement,
ensuring consistency across Python and PySpark implementations.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, Optional


class ISyntheticDataGenerator(ABC):
    """Abstract interface for synthetic data generators."""

    def __init__(self, seed: Optional[int] = None):
        """Initialize the generator with optional seed for reproducibility."""
        self.seed = seed

    @abstractmethod
    def generate_customers_table(self, num_rows: int, **kwargs) -> Any:
        """Generate a customers table with realistic customer data."""
        pass

    @abstractmethod
    def generate_products_table(self, num_rows: int, **kwargs) -> Any:
        """Generate a products table with realistic product data."""
        pass

    @abstractmethod
    def generate_orders_table(self, num_rows: int, customers_df: Any, products_df: Any, **kwargs) -> Any:
        """Generate an orders table with realistic order data."""
        pass

    @abstractmethod
    def generate_order_items_table(self, orders_df: Any, products_df: Any, **kwargs) -> Any:
        """Generate an order items table linking orders to products."""
        pass

    @abstractmethod
    def generate_date_dimension(self, start_date: str, end_date: str, **kwargs) -> Any:
        """Generate a date dimension table for data warehousing."""
        pass

    @abstractmethod
    def generate_custom_table(self, table_config: Dict[str, Any], num_rows: int, **kwargs) -> Any:
        """Generate a custom table based on configuration."""
        pass


class IDatasetBuilder(ABC):
    """Abstract interface for dataset builders."""

    def __init__(self, generator: ISyntheticDataGenerator):
        """Initialize with a synthetic data generator instance."""
        self.generator = generator

    @abstractmethod
    def build_retail_oltp_dataset(self, scale_factor: float = 1.0, **kwargs) -> Dict[str, Any]:
        """Build a complete retail OLTP dataset."""
        pass

    @abstractmethod
    def build_retail_star_schema(self, scale_factor: float = 1.0, **kwargs) -> Dict[str, Any]:
        """Build a retail star schema dataset."""
        pass

    @abstractmethod
    def build_financial_dataset(self, scale_factor: float = 1.0, **kwargs) -> Dict[str, Any]:
        """Build a financial services dataset."""
        pass

    @abstractmethod
    def build_healthcare_dataset(self, scale_factor: float = 1.0, **kwargs) -> Dict[str, Any]:
        """Build a healthcare dataset."""
        pass

    @abstractmethod
    def build_custom_dataset(self, dataset_config: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Build a custom dataset based on configuration."""
        pass


class IIncrementalDataGenerator(ABC):
    """Abstract interface for incremental data generation."""

    @abstractmethod
    def generate_incremental_data(
        self,
        table_name: str,
        generation_date: date,
        base_rows: int,
        table_config: Dict[str, Any],
        state: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Any:
        """Generate incremental data for a specific date."""
        pass

    @abstractmethod
    def apply_seasonal_patterns(self, base_rows: int, generation_date: date, seasonal_config: Dict[str, Any]) -> int:
        """Apply seasonal patterns to determine row count."""
        pass

    @abstractmethod
    def apply_data_drift(self, data: Any, drift_config: Dict[str, Any], generation_date: date) -> Any:
        """Apply data drift to simulate changing patterns over time."""
        pass

    @abstractmethod
    def manage_state(
        self,
        table_name: str,
        generation_date: date,
        state_data: Dict[str, Any],
        operation: str = "update",
    ) -> Optional[Dict[str, Any]]:
        """Manage state for consistent incremental generation."""
        pass

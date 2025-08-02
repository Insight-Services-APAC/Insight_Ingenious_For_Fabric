"""
Synthetic Data Generation Utilities - Python Implementation

This module provides utilities for generating synthetic datasets using Python libraries
optimized for small to medium datasets (up to ~1M rows).

Dependencies: faker, pandas, numpy
"""

import json
import logging
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from ingen_fab.python_libs.interfaces.synthetic_data_generator_interface import ISyntheticDataGenerator, IDatasetBuilder

try:
    import numpy as np
    import pandas as pd
    from faker import Faker

    _DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Required library not available: {e}")
    pd = None
    np = None
    Faker = None
    _DEPENDENCIES_AVAILABLE = False


if _DEPENDENCIES_AVAILABLE:

    class SyntheticDataGenerator(ISyntheticDataGenerator):
        """Python-based synthetic data generator for small to medium datasets."""

        def __init__(self, seed: Optional[int] = None, locale: str = "en_US"):
            """Initialize the generator with optional seed for reproducibility."""
            super().__init__(seed)
            
            if Faker is None:
                raise ImportError(
                    "faker library is required. Install with: pip install faker"
                )
            if pd is None:
                raise ImportError(
                    "pandas library is required. Install with: pip install pandas"
                )
            if np is None:
                raise ImportError(
                    "numpy library is required. Install with: pip install numpy"
                )

            if seed:
                random.seed(seed)
                np.random.seed(seed)

            self.fake = Faker(locale)
            if seed:
                Faker.seed(seed)

            self.logger = logging.getLogger(__name__)

        def generate_customers_table(
            self, num_rows: int, start_id: int = 1
        ) -> pd.DataFrame:
            """Generate a customers table with realistic customer data."""
            customers = []

            for i in range(num_rows):
                customer = {
                    "customer_id": start_id + i,
                    "first_name": self.fake.first_name(),
                    "last_name": self.fake.last_name(),
                    "email": self.fake.email(),
                    "phone": self.fake.phone_number(),
                    "date_of_birth": self.fake.date_of_birth(
                        minimum_age=18, maximum_age=80
                    ),
                    "gender": random.choice(["M", "F", "Other"]),
                    "address_line1": self.fake.street_address(),
                    "address_line2": self.fake.secondary_address()
                    if random.random() < 0.3
                    else None,
                    "city": self.fake.city(),
                    "state": self.fake.state_abbr(),
                    "postal_code": self.fake.postcode(),
                    "country": self.fake.country_code(),
                    "registration_date": self.fake.date_between(
                        start_date="-5y", end_date="today"
                    ),
                    "customer_segment": random.choice(["Premium", "Standard", "Basic"]),
                    "is_active": random.choices([True, False], weights=[0.85, 0.15])[0],
                }
                customers.append(customer)

            return pd.DataFrame(customers)

        def generate_products_table(
            self, num_rows: int, start_id: int = 1
        ) -> pd.DataFrame:
            """Generate a products table with realistic product data."""
            categories = [
                "Electronics",
                "Clothing",
                "Home & Garden",
                "Sports",
                "Books",
                "Health",
                "Automotive",
                "Toys",
                "Beauty",
                "Food",
            ]

            products = []

            for i in range(num_rows):
                category = random.choice(categories)
                base_price = random.uniform(5.0, 500.0)

                product = {
                    "product_id": start_id + i,
                    "product_name": f"{self.fake.word().title()} {category[:-1]}",
                    "product_code": f"SKU{start_id + i:06d}",
                    "category": category,
                    "subcategory": f"{category} - {self.fake.word().title()}",
                    "brand": self.fake.company(),
                    "description": self.fake.text(max_nb_chars=200),
                    "unit_price": round(base_price, 2),
                    "cost_price": round(base_price * random.uniform(0.4, 0.8), 2),
                    "weight_kg": round(random.uniform(0.1, 50.0), 2),
                    "dimensions": f"{random.randint(1, 100)}x{random.randint(1, 100)}x{random.randint(1, 100)}",
                    "stock_quantity": random.randint(0, 1000),
                    "reorder_level": random.randint(10, 100),
                    "supplier_id": random.randint(1, 50),
                    "is_active": random.choices([True, False], weights=[0.9, 0.1])[0],
                    "created_date": self.fake.date_between(
                        start_date="-3y", end_date="today"
                    ),
                }
                products.append(product)

            return pd.DataFrame(products)

        def generate_orders_table(
            self, num_rows: int, customer_ids: List[int], start_id: int = 1
        ) -> pd.DataFrame:
            """Generate an orders table with realistic order data."""
            statuses = [
                "Pending",
                "Processing",
                "Shipped",
                "Delivered",
                "Cancelled",
                "Returned",
            ]
            payment_methods = [
                "Credit Card",
                "Debit Card",
                "PayPal",
                "Bank Transfer",
                "Cash",
            ]

            orders = []

            for i in range(num_rows):
                order_date = self.fake.date_time_between(
                    start_date="-2y", end_date="now"
                )
                status = random.choice(statuses)

                # Calculate dates based on status
                if status in ["Shipped", "Delivered"]:
                    shipped_date = order_date + timedelta(days=random.randint(1, 5))
                    delivered_date = (
                        shipped_date + timedelta(days=random.randint(1, 7))
                        if status == "Delivered"
                        else None
                    )
                else:
                    shipped_date = None
                    delivered_date = None

                order = {
                    "order_id": start_id + i,
                    "customer_id": random.choice(customer_ids),
                    "order_date": order_date,
                    "status": status,
                    "payment_method": random.choice(payment_methods),
                    "shipping_address": self.fake.address(),
                    "billing_address": self.fake.address(),
                    "order_total": round(random.uniform(10.0, 1000.0), 2),
                    "tax_amount": 0.0,  # Will be calculated based on items
                    "shipping_cost": round(random.uniform(0.0, 25.0), 2),
                    "discount_amount": round(random.uniform(0.0, 50.0), 2)
                    if random.random() < 0.3
                    else 0.0,
                    "shipped_date": shipped_date,
                    "delivered_date": delivered_date,
                    "notes": self.fake.text(max_nb_chars=100)
                    if random.random() < 0.2
                    else None,
                }
                orders.append(order)

            return pd.DataFrame(orders)

        def generate_order_items_table(
            self, orders_df: pd.DataFrame, product_ids: List[int]
        ) -> pd.DataFrame:
            """Generate order items table based on existing orders and products."""
            order_items = []
            item_id = 1

            for _, order in orders_df.iterrows():
                # Each order has 1-5 items
                num_items = random.randint(1, 5)
                order_products = random.sample(
                    product_ids, min(num_items, len(product_ids))
                )

                for product_id in order_products:
                    quantity = random.randint(1, 5)
                    unit_price = round(random.uniform(5.0, 200.0), 2)
                    line_total = quantity * unit_price

                    order_item = {
                        "order_item_id": item_id,
                        "order_id": order["order_id"],
                        "product_id": product_id,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "discount_amount": round(
                            line_total * random.uniform(0.0, 0.1), 2
                        )
                        if random.random() < 0.2
                        else 0.0,
                    }
                    order_items.append(order_item)
                    item_id += 1

            return pd.DataFrame(order_items)

        def generate_star_schema_dimensions(
            self, config: Dict[str, Any]
        ) -> Dict[str, pd.DataFrame]:
            """Generate dimension tables for star schema analytics."""
            dimensions = {}

            # Date dimension
            if "dim_date" in config.get("dimensions", []):
                dimensions["dim_date"] = self._generate_date_dimension()

            # Customer dimension (SCD Type 2)
            if "dim_customer" in config.get("dimensions", []):
                dimensions["dim_customer"] = self._generate_customer_dimension(10000)

            # Product dimension
            if "dim_product" in config.get("dimensions", []):
                dimensions["dim_product"] = self._generate_product_dimension(1000)

            # Store dimension
            if "dim_store" in config.get("dimensions", []):
                dimensions["dim_store"] = self._generate_store_dimension(100)

            return dimensions

        def generate_fact_sales(
            self, num_rows: int, dimensions: Dict[str, pd.DataFrame]
        ) -> pd.DataFrame:
            """Generate fact table for sales analytics."""
            facts = []

            # Get dimension keys
            date_keys = (
                dimensions["dim_date"]["date_key"].tolist()
                if "dim_date" in dimensions
                else [20240101]
            )
            customer_keys = (
                dimensions["dim_customer"]["customer_key"].tolist()
                if "dim_customer" in dimensions
                else list(range(1, 1001))
            )
            product_keys = (
                dimensions["dim_product"]["product_key"].tolist()
                if "dim_product" in dimensions
                else list(range(1, 101))
            )
            store_keys = (
                dimensions["dim_store"]["store_key"].tolist()
                if "dim_store" in dimensions
                else [1]
            )

            for i in range(num_rows):
                quantity = random.randint(1, 10)
                unit_price = round(random.uniform(5.0, 500.0), 2)
                sales_amount = quantity * unit_price
                cost_amount = round(sales_amount * random.uniform(0.4, 0.8), 2)

                fact = {
                    "sales_fact_key": i + 1,
                    "date_key": random.choice(date_keys),
                    "customer_key": random.choice(customer_keys),
                    "product_key": random.choice(product_keys),
                    "store_key": random.choice(store_keys),
                    "quantity_sold": quantity,
                    "unit_price": unit_price,
                    "sales_amount": sales_amount,
                    "cost_amount": cost_amount,
                    "profit_amount": sales_amount - cost_amount,
                    "discount_amount": round(
                        sales_amount * random.uniform(0.0, 0.15), 2
                    ),
                    "tax_amount": round(sales_amount * 0.08, 2),  # 8% tax
                }
                facts.append(fact)

            return pd.DataFrame(facts)

        def _generate_date_dimension(
            self, start_year: int = 2020, end_year: int = 2025
        ) -> pd.DataFrame:
            """Generate a comprehensive date dimension table."""
            dates = []
            current_date = datetime(start_year, 1, 1)
            end_date = datetime(end_year, 12, 31)

            while current_date <= end_date:
                date_key = int(current_date.strftime("%Y%m%d"))

                date_record = {
                    "date_key": date_key,
                    "full_date": current_date,
                    "day_of_week": current_date.weekday() + 1,
                    "day_name": current_date.strftime("%A"),
                    "day_of_month": current_date.day,
                    "day_of_year": current_date.timetuple().tm_yday,
                    "week_of_year": current_date.isocalendar()[1],
                    "month": current_date.month,
                    "month_name": current_date.strftime("%B"),
                    "quarter": (current_date.month - 1) // 3 + 1,
                    "year": current_date.year,
                    "is_weekend": current_date.weekday() >= 5,
                    "is_holiday": self._is_holiday(current_date),
                }
                dates.append(date_record)
                current_date += timedelta(days=1)

            return pd.DataFrame(dates)

        def _generate_customer_dimension(self, num_customers: int) -> pd.DataFrame:
            """Generate customer dimension with SCD Type 2 support."""
            customers = []

            for i in range(num_customers):
                # Some customers might have multiple versions (SCD Type 2)
                versions = random.randint(1, 3) if random.random() < 0.2 else 1

                for version in range(versions):
                    customer = {
                        "customer_key": i * 10 + version + 1,  # Surrogate key
                        "customer_id": i + 1,  # Natural key
                        "first_name": self.fake.first_name(),
                        "last_name": self.fake.last_name(),
                        "email": self.fake.email(),
                        "customer_segment": random.choice(
                            ["Premium", "Standard", "Basic"]
                        ),
                        "city": self.fake.city(),
                        "state": self.fake.state(),
                        "country": self.fake.country(),
                        "effective_date": self.fake.date_between(
                            start_date="-3y", end_date="today"
                        ),
                        "expiry_date": self.fake.date_between(
                            start_date="today", end_date="+1y"
                        )
                        if version < versions - 1
                        else None,
                        "is_current": version == versions - 1,
                    }
                    customers.append(customer)

            return pd.DataFrame(customers)

        def _generate_product_dimension(self, num_products: int) -> pd.DataFrame:
            """Generate product dimension table."""
            products = []
            categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]

            for i in range(num_products):
                category = random.choice(categories)
                product = {
                    "product_key": i + 1,
                    "product_id": f"SKU{i + 1:06d}",
                    "product_name": f"{self.fake.word().title()} {category}",
                    "category": category,
                    "subcategory": f"{category} - {self.fake.word().title()}",
                    "brand": self.fake.company(),
                    "unit_price": round(random.uniform(10.0, 1000.0), 2),
                    "cost_price": round(random.uniform(5.0, 500.0), 2),
                }
                products.append(product)

            return pd.DataFrame(products)

        def _generate_store_dimension(self, num_stores: int) -> pd.DataFrame:
            """Generate store dimension table."""
            stores = []
            store_types = ["Flagship", "Standard", "Outlet", "Online"]

            for i in range(num_stores):
                store = {
                    "store_key": i + 1,
                    "store_id": f"ST{i + 1:03d}",
                    "store_name": f"{self.fake.city()} {random.choice(['Mall', 'Plaza', 'Center', 'Outlet'])}",
                    "store_type": random.choice(store_types),
                    "address": self.fake.address(),
                    "city": self.fake.city(),
                    "state": self.fake.state(),
                    "country": self.fake.country(),
                    "square_footage": random.randint(1000, 50000),
                    "manager_name": self.fake.name(),
                }
                stores.append(store)

            return pd.DataFrame(stores)

        def _is_holiday(self, date: datetime) -> bool:
            """Simple holiday detection (US holidays)."""
            # New Year's Day
            if date.month == 1 and date.day == 1:
                return True
            # Christmas
            if date.month == 12 and date.day == 25:
                return True
            # July 4th
            if date.month == 7 and date.day == 4:
                return True
            return False

        def export_to_parquet(
            self, dataframes: Dict[str, pd.DataFrame], output_dir: str
        ):
            """Export generated dataframes to Parquet files."""
            import os

            os.makedirs(output_dir, exist_ok=True)

            for table_name, df in dataframes.items():
                output_path = os.path.join(output_dir, f"{table_name}.parquet")
                df.to_parquet(output_path, index=False)
                self.logger.info(
                    f"Exported {table_name} with {len(df)} rows to {output_path}"
                )

        def export_to_csv(self, dataframes: Dict[str, pd.DataFrame], output_dir: str):
            """Export generated dataframes to CSV files."""
            import os

            os.makedirs(output_dir, exist_ok=True)

            for table_name, df in dataframes.items():
                output_path = os.path.join(output_dir, f"{table_name}.csv")
                df.to_csv(output_path, index=False)
                self.logger.info(
                    f"Exported {table_name} with {len(df)} rows to {output_path}"
                )

    class DatasetBuilder:
        """High-level builder for creating complete synthetic datasets."""

        def __init__(self, generator: SyntheticDataGenerator):
            self.generator = generator

        def build_retail_oltp_dataset(
            self, scale: str = "small"
        ) -> Dict[str, pd.DataFrame]:
            """Build a complete retail OLTP dataset."""
            scales = {
                "small": {"customers": 1000, "products": 100, "orders": 5000},
                "medium": {"customers": 10000, "products": 1000, "orders": 50000},
                "large": {"customers": 100000, "products": 10000, "orders": 500000},
            }

            if scale not in scales:
                raise ValueError(f"Scale must be one of: {list(scales.keys())}")

            config = scales[scale]

            # Generate tables
            customers_df = self.generator.generate_customers_table(config["customers"])
            products_df = self.generator.generate_products_table(config["products"])
            orders_df = self.generator.generate_orders_table(
                config["orders"], customers_df["customer_id"].tolist()
            )
            order_items_df = self.generator.generate_order_items_table(
                orders_df, products_df["product_id"].tolist()
            )

            return {
                "customers": customers_df,
                "products": products_df,
                "orders": orders_df,
                "order_items": order_items_df,
            }

        def build_retail_star_schema_dataset(
            self, fact_rows: int = 100000
        ) -> Dict[str, pd.DataFrame]:
            """Build a retail star schema dataset."""
            config = {
                "dimensions": ["dim_date", "dim_customer", "dim_product", "dim_store"],
                "fact_tables": ["fact_sales"],
            }

            # Generate dimensions
            dimensions = self.generator.generate_star_schema_dimensions(config)

            # Generate fact table
            fact_sales = self.generator.generate_fact_sales(fact_rows, dimensions)

            # Combine all tables
            dataset = {**dimensions, "fact_sales": fact_sales}

            return dataset

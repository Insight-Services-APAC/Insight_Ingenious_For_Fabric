"""
Synthetic Data Generation Utilities - PySpark Implementation

This module provides utilities for generating large-scale synthetic datasets using PySpark
optimized for datasets ranging from 1M to 1B+ rows.

Dependencies: pyspark
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    date_add,
    expr,
    greatest,
    least,
    lit,
    lpad,
    when,
)


class PySparkSyntheticDataGenerator:
    """PySpark-based synthetic data generator for large-scale datasets."""

    def __init__(
        self,
        lakehouse_utils_instance,
        seed: Optional[int] = None,
    ):
        self.lakehouse_utils = lakehouse_utils_instance
        self.seed = seed

        if seed:
            self.lakehouse_utils.set_spark_config("spark.sql.shuffle.partitions", "200")

        self.logger = logging.getLogger(__name__)

    def generate_customers_table(self, num_rows: int, num_partitions: int = None) -> DataFrame:
        """Generate a customers table with realistic customer data using PySpark."""
        if num_partitions is None:
            num_partitions = max(1, num_rows // 100000)  # 100k rows per partition

        # Create base sequence
        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, num_partitions).toDF("customer_id")

        # Define customer generation logic
        customers_df = base_df.select(
            col("customer_id"),
            # Generate names using random patterns
            concat(
                expr(
                    "CASE WHEN customer_id % 1000 < 100 THEN 'John' "
                    + "WHEN customer_id % 1000 < 200 THEN 'Jane' "
                    + "WHEN customer_id % 1000 < 300 THEN 'Michael' "
                    + "WHEN customer_id % 1000 < 400 THEN 'Sarah' "
                    + "WHEN customer_id % 1000 < 500 THEN 'David' "
                    + "WHEN customer_id % 1000 < 600 THEN 'Lisa' "
                    + "WHEN customer_id % 1000 < 700 THEN 'Robert' "
                    + "WHEN customer_id % 1000 < 800 THEN 'Emily' "
                    + "WHEN customer_id % 1000 < 900 THEN 'James' "
                    + "ELSE 'Maria' END"
                )
            ).alias("first_name"),
            concat(
                expr(
                    "CASE WHEN customer_id % 1234 < 200 THEN 'Smith' "
                    + "WHEN customer_id % 1234 < 400 THEN 'Johnson' "
                    + "WHEN customer_id % 1234 < 600 THEN 'Williams' "
                    + "WHEN customer_id % 1234 < 800 THEN 'Brown' "
                    + "WHEN customer_id % 1234 < 1000 THEN 'Jones' "
                    + "ELSE 'Davis' END"
                )
            ).alias("last_name"),
            # Generate email
            concat(
                col("first_name"),
                lit("."),
                col("last_name"),
                lit("@"),
                expr(
                    "CASE WHEN customer_id % 5 = 0 THEN 'gmail.com' "
                    + "WHEN customer_id % 5 = 1 THEN 'yahoo.com' "
                    + "WHEN customer_id % 5 = 2 THEN 'outlook.com' "
                    + "WHEN customer_id % 5 = 3 THEN 'hotmail.com' "
                    + "ELSE 'company.com' END"
                ),
            ).alias("email"),
            # Generate phone numbers
            concat(
                lit("("),
                lpad((col("customer_id") % 800 + 200).cast("string"), 3, "0"),
                lit(") "),
                lpad((col("customer_id") % 900 + 100).cast("string"), 3, "0"),
                lit("-"),
                lpad((col("customer_id") % 10000).cast("string"), 4, "0"),
            ).alias("phone"),
            # Generate dates (cast BIGINT expressions to INT for date_add compatibility)
            date_add(lit("1970-01-01"), (col("customer_id") % 18250 + 6570).cast("int")).alias(
                "date_of_birth"
            ),  # Ages 18-80
            date_add(lit("2019-01-01"), (col("customer_id") % 1826).cast("int")).alias(
                "registration_date"
            ),  # Last 5 years
            # Generate categorical data
            expr(
                "CASE WHEN customer_id % 3 = 0 THEN 'M' " + "WHEN customer_id % 3 = 1 THEN 'F' " + "ELSE 'Other' END"
            ).alias("gender"),
            expr(
                "CASE WHEN customer_id % 10 < 2 THEN 'Premium' "
                + "WHEN customer_id % 10 < 7 THEN 'Standard' "
                + "ELSE 'Basic' END"
            ).alias("customer_segment"),
            # Generate addresses
            concat(
                (col("customer_id") % 9999 + 1).cast("string"),
                lit(" "),
                expr(
                    "CASE WHEN customer_id % 20 < 5 THEN 'Main St' "
                    + "WHEN customer_id % 20 < 10 THEN 'Oak Ave' "
                    + "WHEN customer_id % 20 < 15 THEN 'Park Blvd' "
                    + "ELSE 'First St' END"
                ),
            ).alias("address_line1"),
            expr(
                "CASE WHEN customer_id % 50 < 5 THEN 'New York' "
                + "WHEN customer_id % 50 < 10 THEN 'Los Angeles' "
                + "WHEN customer_id % 50 < 15 THEN 'Chicago' "
                + "WHEN customer_id % 50 < 20 THEN 'Houston' "
                + "WHEN customer_id % 50 < 25 THEN 'Phoenix' "
                + "WHEN customer_id % 50 < 30 THEN 'Philadelphia' "
                + "WHEN customer_id % 50 < 35 THEN 'San Antonio' "
                + "WHEN customer_id % 50 < 40 THEN 'San Diego' "
                + "WHEN customer_id % 50 < 45 THEN 'Dallas' "
                + "ELSE 'Austin' END"
            ).alias("city"),
            expr(
                "CASE WHEN customer_id % 50 < 10 THEN 'NY' "
                + "WHEN customer_id % 50 < 20 THEN 'CA' "
                + "WHEN customer_id % 50 < 25 THEN 'IL' "
                + "WHEN customer_id % 50 < 30 THEN 'TX' "
                + "WHEN customer_id % 50 < 35 THEN 'AZ' "
                + "WHEN customer_id % 50 < 40 THEN 'PA' "
                + "WHEN customer_id % 50 < 45 THEN 'FL' "
                + "ELSE 'OH' END"
            ).alias("state"),
            lpad((col("customer_id") % 90000 + 10000).cast("string"), 5, "0").alias("postal_code"),
            lit("US").alias("country"),
            # Boolean flags
            (col("customer_id") % 10 != 0).alias("is_active"),  # 90% active
        )

        return customers_df

    def generate_products_table(self, num_rows: int, num_partitions: int = None) -> DataFrame:
        """Generate a products table with realistic product data using PySpark."""
        if num_partitions is None:
            num_partitions = max(1, num_rows // 10000)  # 10k rows per partition

        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, num_partitions).toDF("product_id")

        products_df = base_df.select(
            col("product_id"),
            # Product names and codes
            concat(
                expr(
                    "CASE WHEN product_id % 100 < 20 THEN 'Premium' "
                    + "WHEN product_id % 100 < 40 THEN 'Deluxe' "
                    + "WHEN product_id % 100 < 60 THEN 'Standard' "
                    + "WHEN product_id % 100 < 80 THEN 'Basic' "
                    + "ELSE 'Economy' END"
                ),
                lit(" "),
                expr(
                    "CASE WHEN product_id % 10 < 2 THEN 'Widget' "
                    + "WHEN product_id % 10 < 4 THEN 'Gadget' "
                    + "WHEN product_id % 10 < 6 THEN 'Device' "
                    + "WHEN product_id % 10 < 8 THEN 'Tool' "
                    + "ELSE 'Item' END"
                ),
            ).alias("product_name"),
            concat(lit("SKU"), lpad(col("product_id").cast("string"), 6, "0")).alias("product_code"),
            # Categories
            expr(
                "CASE WHEN product_id % 10 = 0 THEN 'Electronics' "
                + "WHEN product_id % 10 = 1 THEN 'Clothing' "
                + "WHEN product_id % 10 = 2 THEN 'Home & Garden' "
                + "WHEN product_id % 10 = 3 THEN 'Sports' "
                + "WHEN product_id % 10 = 4 THEN 'Books' "
                + "WHEN product_id % 10 = 5 THEN 'Health' "
                + "WHEN product_id % 10 = 6 THEN 'Automotive' "
                + "WHEN product_id % 10 = 7 THEN 'Toys' "
                + "WHEN product_id % 10 = 8 THEN 'Beauty' "
                + "ELSE 'Food' END"
            ).alias("category"),
            # Brands
            expr(
                "CASE WHEN product_id % 20 < 4 THEN 'BrandA Corp' "
                + "WHEN product_id % 20 < 8 THEN 'BrandB Industries' "
                + "WHEN product_id % 20 < 12 THEN 'BrandC Solutions' "
                + "WHEN product_id % 20 < 16 THEN 'BrandD Enterprises' "
                + "ELSE 'BrandE Systems' END"
            ).alias("brand"),
            # Pricing
            ((col("product_id") % 4950 + 50) / 10.0).alias("unit_price"),  # $5.00 to $500.00
            ((col("product_id") % 3960 + 40) / 10.0).alias("cost_price"),  # $4.00 to $400.00
            # Physical properties
            ((col("product_id") % 499 + 1) / 10.0).alias("weight_kg"),  # 0.1 to 50.0 kg
            (col("product_id") % 1000 + 1).alias("stock_quantity"),
            (col("product_id") % 100 + 10).alias("reorder_level"),
            (col("product_id") % 50 + 1).alias("supplier_id"),
            # Dates and flags (cast BIGINT expression to INT for date_add compatibility)
            date_add(lit("2021-01-01"), (col("product_id") % 1095).cast("int")).alias("created_date"),  # Last 3 years
            (col("product_id") % 20 != 0).alias("is_active"),  # 95% active
        )

        return products_df

    def generate_orders_table(
        self,
        num_rows: int,
        customers_df: DataFrame,
        products_df: DataFrame = None,
        **kwargs,
    ) -> DataFrame:
        """Generate an orders table with realistic order data using PySpark."""
        # Get customer count from the DataFrame
        customer_count = customers_df.count()

        num_partitions = kwargs.get("num_partitions")
        if num_partitions is None:
            num_partitions = max(1, num_rows // 50000)  # 50k rows per partition

        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, num_partitions).toDF("order_id")

        orders_df = base_df.select(
            col("order_id"),
            (col("order_id") % customer_count + 1).alias("customer_id"),
            # Order dates (last 2 years)
            date_add(
                lit("2022-01-01"),
                least(
                    greatest((col("order_id") % 730).cast("int"), lit(0)),
                    lit(2147483647),
                ),
            ).alias("order_date"),
            # Status
            expr(
                "CASE WHEN order_id % 100 < 15 THEN 'Pending' "
                + "WHEN order_id % 100 < 25 THEN 'Processing' "
                + "WHEN order_id % 100 < 45 THEN 'Shipped' "
                + "WHEN order_id % 100 < 80 THEN 'Delivered' "
                + "WHEN order_id % 100 < 90 THEN 'Cancelled' "
                + "ELSE 'Returned' END"
            ).alias("status"),
            # Payment methods
            expr(
                "CASE WHEN order_id % 5 = 0 THEN 'Credit Card' "
                + "WHEN order_id % 5 = 1 THEN 'Debit Card' "
                + "WHEN order_id % 5 = 2 THEN 'PayPal' "
                + "WHEN order_id % 5 = 3 THEN 'Bank Transfer' "
                + "ELSE 'Cash' END"
            ).alias("payment_method"),
            # Financial amounts
            ((col("order_id") % 9900 + 100) / 10.0).alias("order_total"),  # $10.00 to $1000.00
            ((col("order_id") % 250) / 10.0).alias("shipping_cost"),  # $0.00 to $25.00
            when(col("order_id") % 10 < 3, (col("order_id") % 500) / 10.0)
            .otherwise(0.0)
            .alias("discount_amount"),  # 30% chance of discount
            # Calculated shipping dates
            when(
                expr("status IN ('Shipped', 'Delivered')"),
                date_add(col("order_date"), (col("order_id") % 5 + 1).cast("int")),
            ).alias("shipped_date"),
            when(
                expr("status = 'Delivered'"),
                date_add(col("order_date"), (col("order_id") % 7 + 3).cast("int")),
            ).alias("delivered_date"),
        )

        return orders_df

    def generate_order_items_table(self, orders_df: DataFrame, products_df: DataFrame = None, **kwargs) -> DataFrame:
        """Generate an order items table linking orders to products using PySpark."""
        # Get product count from the DataFrame
        if products_df is not None:
            product_count = products_df.count()
        else:
            product_count = 100  # Default number of products

        # Generate order items based on orders
        # Each order gets 1-5 items
        order_items_df = (
            orders_df.select(
                col("order_id"),
                # Generate multiple items per order using array_repeat and explode
                expr("explode(sequence(1, cast((order_id % 5) + 1 as int)))").alias("item_sequence"),
            )
            .select(
                # Create unique order item IDs
                expr("row_number() OVER (ORDER BY order_id, item_sequence)").alias("order_item_id"),
                col("order_id"),
                # Random product selection
                ((col("order_id") * 13 + col("item_sequence") * 17) % product_count + 1).alias("product_id"),
                # Random quantities between 1-5
                ((col("order_id") * 7 + col("item_sequence") * 11) % 5 + 1).alias("quantity"),
                # Random unit prices between $5-$200
                (((col("order_id") * 23 + col("item_sequence") * 29) % 19500 + 500) / 100.0).alias("unit_price"),
            )
            .withColumn(
                # Calculate line total
                "line_total",
                col("quantity") * col("unit_price"),
            )
        )

        return order_items_df

    def generate_large_fact_table(
        self,
        num_rows: int,
        date_range_days: int = 1826,
        customer_count: int = 1000000,
        product_count: int = 100000,
        store_count: int = 1000,
        num_partitions: int = None,
    ) -> DataFrame:
        """Generate a large fact table for analytics using PySpark optimizations."""
        if num_partitions is None:
            num_partitions = max(200, num_rows // 1000000)  # 1M rows per partition for large datasets

        base_df = self.lakehouse_utils.create_range_dataframe(1, num_rows + 1, num_partitions).toDF("sales_fact_key")

        fact_df = (
            base_df.select(
                col("sales_fact_key"),
                # Generate date keys (YYYYMMDD format)
                expr(f"20200101 + (sales_fact_key % {date_range_days})").alias("date_key"),
                # Generate dimension keys
                (col("sales_fact_key") % customer_count + 1).alias("customer_key"),
                (col("sales_fact_key") % product_count + 1).alias("product_key"),
                (col("sales_fact_key") % store_count + 1).alias("store_key"),
                # Generate measures
                (col("sales_fact_key") % 10 + 1).alias("quantity_sold"),
                ((col("sales_fact_key") % 4950 + 50) / 10.0).alias("unit_price"),
                # Calculate derived measures
                (col("quantity_sold") * col("unit_price")).alias("sales_amount"),
            )
            .select(
                "*",
                (col("sales_amount") * (0.6 + (col("sales_fact_key") % 30) / 100.0)).alias("cost_amount"),
                (col("sales_amount") * ((col("sales_fact_key") % 15) / 100.0)).alias("discount_amount"),
                (col("sales_amount") * 0.08).alias("tax_amount"),
            )
            .select("*", (col("sales_amount") - col("cost_amount")).alias("profit_amount"))
        )

        return fact_df

    def generate_date_dimension(self, start_year: int = 2020, end_year: int = 2025) -> DataFrame:
        """Generate a comprehensive date dimension using PySpark."""
        # Calculate total days
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        total_days = (end_date - start_date).days + 1

        # Create base range
        base_df = self.lakehouse_utils.create_range_dataframe(0, total_days).toDF("day_offset")

        date_df = base_df.select(
            # Calculate actual date (cast BIGINT to INT for date_add compatibility)
            date_add(lit(f"{start_year}-01-01"), col("day_offset").cast("int")).alias("full_date")
        ).select(
            # Generate date key (YYYYMMDD)
            expr("CAST(date_format(full_date, 'yyyyMMdd') AS INT)").alias("date_key"),
            col("full_date"),
            # Date parts
            expr("dayofweek(full_date)").alias("day_of_week"),
            expr("date_format(full_date, 'EEEE')").alias("day_name"),
            expr("dayofmonth(full_date)").alias("day_of_month"),
            expr("dayofyear(full_date)").alias("day_of_year"),
            expr("weekofyear(full_date)").alias("week_of_year"),
            expr("month(full_date)").alias("month"),
            expr("date_format(full_date, 'MMMM')").alias("month_name"),
            expr("quarter(full_date)").alias("quarter"),
            expr("year(full_date)").alias("year"),
            # Calculated fields
            expr("dayofweek(full_date) IN (1, 7)").alias("is_weekend"),
            expr("month(full_date) = 12 AND dayofmonth(full_date) = 25").alias("is_holiday"),  # Simple holiday logic
        )

        return date_df

    def generate_fact_sales(self, num_rows: int, dimensions: Dict[str, DataFrame], **kwargs) -> DataFrame:
        """Generate a fact sales table using provided dimensions.

        Args:
            num_rows: Number of sales fact records to generate
            dimensions: Dictionary of dimension DataFrames (dim_customer, dim_product, etc.)
            **kwargs: Additional parameters

        Returns:
            DataFrame containing sales fact data
        """
        num_partitions = kwargs.get("num_partitions")
        if num_partitions is None:
            num_partitions = max(1, num_rows // 50000)  # 50k rows per partition

        # Get dimension counts
        customer_count = dimensions.get("dim_customer", {}).count() if "dim_customer" in dimensions else 50000
        product_count = dimensions.get("dim_product", {}).count() if "dim_product" in dimensions else 5000
        store_count = 100  # Default store count

        # Generate fact table using existing method
        return self.generate_large_fact_table(
            num_rows=num_rows,
            customer_count=customer_count,
            product_count=product_count,
            store_count=store_count,
            num_partitions=num_partitions,
        )

    def write_to_delta_table(
        self,
        df: DataFrame,
        table_name: str,
        partition_cols: List[str] = None,
        mode: str = "overwrite",
    ):
        """Write DataFrame to Delta table with optional partitioning."""
        self.lakehouse_utils.write_to_table(df, table_name, mode=mode, partition_by=partition_cols)
        self.logger.info(f"Written to Delta table {table_name}")

    def optimize_for_large_datasets(self, df: DataFrame, partition_col: str = None) -> DataFrame:
        """Apply optimizations for large dataset processing."""
        # Cache if dataset will be reused
        df = self.lakehouse_utils.cache_dataframe(df)

        # Repartition if specified
        if partition_col:
            df = self.lakehouse_utils.repartition_dataframe(df, partition_cols=[partition_col])

        # Enable adaptive query execution
        self.lakehouse_utils.set_spark_config("spark.sql.adaptive.enabled", "true")
        self.lakehouse_utils.set_spark_config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        return df

    def generate_chunked_dataset(self, total_rows: int, chunk_size: int, generator_func, **kwargs) -> List[DataFrame]:
        """Generate large datasets in chunks to manage memory."""
        chunks = []
        for start_id in range(1, total_rows + 1, chunk_size):
            end_id = min(start_id + chunk_size - 1, total_rows)
            chunk_rows = end_id - start_id + 1

            chunk_df = generator_func(chunk_rows, start_id=start_id, **kwargs)
            chunks.append(chunk_df)

            self.logger.info(f"Generated chunk {len(chunks)} with {chunk_rows} rows")

        return chunks

    def union_chunks(self, chunks: List[DataFrame]) -> DataFrame:
        """Union multiple DataFrame chunks into a single DataFrame."""
        return self.lakehouse_utils.union_dataframes(chunks)

    def export_to_csv(self, tables: Dict[str, DataFrame], output_path: str) -> None:
        """Export generated tables to CSV files."""
        for table_name, df in tables.items():
            file_path = f"{output_path}/{table_name}.csv"
            self.lakehouse_utils.write_file(df, file_path, "csv", options={"header": "true", "mode": "overwrite"})
            self.logger.info(f"Exported {table_name} to CSV: {file_path}")

    def export_to_parquet(self, tables: Dict[str, DataFrame], output_path: str) -> None:
        """Export generated tables to Parquet files."""
        for table_name, df in tables.items():
            file_path = f"{output_path}/{table_name}.parquet"
            self.lakehouse_utils.write_file(df, file_path, "parquet", options={"mode": "overwrite"})
            self.logger.info(f"Exported {table_name} to Parquet: {file_path}")


class PySparkDatasetBuilder:
    """High-level builder for creating complete synthetic datasets using PySpark."""

    def __init__(self, generator: PySparkSyntheticDataGenerator):
        self.generator = generator
        self.lakehouse_utils = generator.lakehouse_utils

    def build_billion_row_fact_table(self, target_rows: int = 1000000000, chunk_size: int = 10000000) -> DataFrame:
        """Build a billion-row fact table using chunked generation."""
        if target_rows <= chunk_size:
            return self.generator.generate_large_fact_table(target_rows)

        # Generate in chunks
        chunks = []
        for start_id in range(1, target_rows + 1, chunk_size):
            end_id = min(start_id + chunk_size - 1, target_rows)
            chunk_rows = end_id - start_id + 1

            chunk_df = self.generator.generate_large_fact_table(
                chunk_rows, num_partitions=max(10, chunk_rows // 100000)
            )
            chunks.append(chunk_df)

            self.generator.logger.info(f"Generated chunk with {chunk_rows} rows")

        # Union all chunks
        return self.generator.union_chunks(chunks)

    def build_complete_star_schema(
        self,
        fact_rows: int,
        customer_count: int = 1000000,
        product_count: int = 100000,
        store_count: int = 1000,
    ) -> Dict[str, DataFrame]:
        """Build a complete star schema with dimensions and fact table."""

        # Generate dimensions
        dim_date = self.generator.generate_date_dimension()
        dim_customer = self.generator.generate_customers_table(customer_count)
        dim_product = self.generator.generate_products_table(product_count)

        # Generate store dimension (simplified)
        dim_store = (
            self.generator.lakehouse_utils.create_range_dataframe(1, store_count + 1)
            .toDF("store_key")
            .select(
                col("store_key"),
                concat(lit("Store "), col("store_key").cast("string")).alias("store_name"),
                expr(
                    "CASE WHEN store_key % 4 = 0 THEN 'Flagship' "
                    + "WHEN store_key % 4 = 1 THEN 'Standard' "
                    + "WHEN store_key % 4 = 2 THEN 'Outlet' "
                    + "ELSE 'Online' END"
                ).alias("store_type"),
            )
        )

        # Generate fact table
        fact_sales = self.generator.generate_large_fact_table(
            fact_rows,
            customer_count=customer_count,
            product_count=product_count,
            store_count=store_count,
        )

        return {
            "dim_date": dim_date,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_store": dim_store,
            "fact_sales": fact_sales,
        }

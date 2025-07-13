# Flat File Ingestion Sample Project

This folder contains sample data files and configurations for testing the flat file ingestion package.

## Sample Files

### 1. sales_data.csv
- **Format**: CSV with headers
- **Records**: 12 sales transactions
- **Columns**: date, product_id, customer_id, quantity, unit_price, total_amount, region, sales_rep
- **Use Case**: Testing CSV file ingestion with date parsing and numeric columns

### 2. products.json
- **Format**: JSON array
- **Records**: 5 product records
- **Schema**: Nested JSON with timestamps, booleans, and various data types
- **Use Case**: Testing JSON file ingestion with schema inference and timestamp parsing

### 3. customers.parquet
- **Format**: Apache Parquet
- **Records**: 12 customer records
- **Schema**: Mixed data types including dates, booleans, strings, and numbers
- **Use Case**: Testing Parquet file ingestion with partitioning by region

## Configuration

The DDL scripts include sample configuration records that reference these files:

- `csv_test_001`: Points to sales_data.csv
- `json_test_002`: Points to products.json  
- `parquet_test_003`: Points to customers.parquet

## Usage

1. Run the DDL scripts to create tables and insert sample configurations
2. Copy the sample_data folder to your target lakehouse Files directory
3. Execute the flat file ingestion processor with execution_group=1 or 2
4. Verify the data has been loaded into the raw schema tables
#!/usr/bin/env python3
"""
Test script for Extract Generation package end-to-end functionality.
This script simulates the full workflow from DDL creation to extract generation.
"""

import os
import sys
import sqlite3
import pandas as pd
from pathlib import Path
import tempfile
import json
from datetime import datetime
import uuid

# Add the package to sys.path for testing
sys.path.insert(0, '/workspaces/ingen_fab')

def setup_test_database():
    """Create an in-memory SQLite database with sample data for testing"""
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    print("🗄️  Setting up test database with sample data...")
    
    # Create sample customers table
    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            country TEXT,
            created_date TEXT
        )
    """)
    
    # Insert sample customers data
    customers_data = [
        (1, 'Acme Corp', 'contact@acme.com', '555-0100', '123 Main St', 'New York', 'USA', '2024-01-15'),
        (2, 'Global Industries', 'info@global.com', '555-0200', '456 Oak Ave', 'London', 'UK', '2024-01-20'),
        (3, 'Tech Solutions', 'sales@techsol.com', '555-0300', '789 Pine Rd', 'Tokyo', 'Japan', '2024-02-01'),
        (4, 'Innovation Ltd', 'hello@innovate.com', '555-0400', '321 Elm St', 'Berlin', 'Germany', '2024-02-10'),
        (5, 'Future Systems', 'support@future.com', '555-0500', '654 Maple Dr', 'Sydney', 'Australia', '2024-02-15'),
    ]
    
    cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?)", customers_data)
    
    # Create sample transactions table
    cursor.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            amount REAL,
            quantity INTEGER,
            transaction_date TEXT,
            status TEXT
        )
    """)
    
    # Insert sample transactions
    transactions_data = [
        (1, 1, 101, 250.50, 2, '2024-07-20', 'completed'),
        (2, 2, 102, 450.75, 1, '2024-07-21', 'completed'),
        (3, 3, 103, 125.25, 3, '2024-07-22', 'pending'),
        (4, 1, 104, 750.00, 1, '2024-07-23', 'completed'),
        (5, 4, 105, 99.99, 5, '2024-07-24', 'cancelled'),
        (6, 5, 101, 180.25, 2, '2024-07-25', 'completed'),
        (7, 2, 106, 320.50, 1, '2024-07-25', 'completed'),
    ]
    
    cursor.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)", transactions_data)
    
    # Create reporting view
    cursor.execute("""
        CREATE VIEW v_sales_summary AS
        SELECT 
            substr(t.transaction_date, 1, 7) as year_month,
            c.country,
            COUNT(DISTINCT t.customer_id) as unique_customers,
            COUNT(t.transaction_id) as total_transactions,
            SUM(t.amount) as total_revenue,
            AVG(t.amount) as avg_transaction_value
        FROM transactions t
        INNER JOIN customers c ON t.customer_id = c.customer_id
        WHERE t.status = 'completed'
        GROUP BY substr(t.transaction_date, 1, 7), c.country
    """)
    
    # Create config and log schemas (tables)
    cursor.execute("""
        CREATE TABLE config_extract_generation (
            extract_name TEXT PRIMARY KEY,
            is_active INTEGER,
            extract_table_name TEXT,
            extract_table_schema TEXT,
            extract_view_name TEXT,
            extract_view_schema TEXT,
            extract_sp_name TEXT,
            extract_sp_schema TEXT,
            is_full_load INTEGER,
            execution_group TEXT,
            created_timestamp TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE config_extract_details (
            extract_name TEXT PRIMARY KEY,
            is_active INTEGER,
            extract_container TEXT,
            extract_directory TEXT,
            extract_file_name TEXT,
            extract_file_name_extension TEXT,
            file_properties_column_delimiter TEXT,
            file_properties_header INTEGER,
            output_format TEXT,
            is_compressed INTEGER,
            compressed_type TEXT,
            is_trigger_file INTEGER,
            trigger_file_extension TEXT,
            created_timestamp TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE log_extract_generation (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            extract_name TEXT,
            run_id TEXT,
            run_timestamp TEXT,
            run_status TEXT,
            rows_extracted INTEGER,
            rows_written INTEGER,
            files_generated INTEGER,
            output_file_path TEXT,
            created_timestamp TEXT
        )
    """)
    
    print("✅ Test database setup complete")
    return conn

def insert_sample_configurations(conn):
    """Insert sample extract configurations"""
    cursor = conn.cursor()
    
    print("📋 Inserting sample extract configurations...")
    
    # Sample configurations
    configs = [
        {
            'extract_name': 'SAMPLE_CUSTOMERS_DAILY',
            'is_active': 1,
            'extract_table_name': 'customers',
            'extract_table_schema': 'main',
            'is_full_load': 1,
            'execution_group': 'DAILY_EXTRACTS'
        },
        {
            'extract_name': 'SAMPLE_SALES_SUMMARY',
            'is_active': 1,
            'extract_view_name': 'v_sales_summary',
            'extract_view_schema': 'main',
            'is_full_load': 1,
            'execution_group': 'REPORTS'
        }
    ]
    
    details = [
        {
            'extract_name': 'SAMPLE_CUSTOMERS_DAILY',
            'is_active': 1,
            'extract_container': 'extracts',
            'extract_directory': 'customers',
            'extract_file_name': 'customers',
            'extract_file_name_extension': 'csv',
            'file_properties_column_delimiter': ',',
            'file_properties_header': 1,
            'output_format': 'csv',
            'is_compressed': 0,
            'is_trigger_file': 0
        },
        {
            'extract_name': 'SAMPLE_SALES_SUMMARY',
            'is_active': 1,
            'extract_container': 'extracts',
            'extract_directory': 'reports',
            'extract_file_name': 'sales_summary',
            'extract_file_name_extension': 'csv',
            'file_properties_column_delimiter': ',',
            'file_properties_header': 1,
            'output_format': 'csv',
            'is_compressed': 1,
            'compressed_type': 'ZIP',
            'is_trigger_file': 1,
            'trigger_file_extension': '.done'
        }
    ]
    
    # Insert configurations
    for config in configs:
        config['created_timestamp'] = datetime.utcnow().isoformat()
        placeholders = ', '.join(['?' for _ in config])
        columns = ', '.join(config.keys())
        cursor.execute(f"INSERT INTO config_extract_generation ({columns}) VALUES ({placeholders})", list(config.values()))
    
    for detail in details:
        detail['created_timestamp'] = datetime.utcnow().isoformat()
        placeholders = ', '.join(['?' for _ in detail])
        columns = ', '.join(detail.keys())
        cursor.execute(f"INSERT INTO config_extract_details ({columns}) VALUES ({placeholders})", list(detail.values()))
    
    conn.commit()
    print("✅ Sample configurations inserted")

def simulate_extract_generation(conn, extract_name: str, output_dir: Path):
    """Simulate the extract generation process"""
    cursor = conn.cursor()
    
    print(f"🔄 Processing extract: {extract_name}")
    
    # Get configuration
    cursor.execute("""
        SELECT cg.*, cd.* 
        FROM config_extract_generation cg
        JOIN config_extract_details cd ON cg.extract_name = cd.extract_name
        WHERE cg.extract_name = ? AND cg.is_active = 1
    """, (extract_name,))
    
    config_row = cursor.fetchone()
    if not config_row:
        print(f"❌ No configuration found for {extract_name}")
        return
    
    # Get column names for easier access
    columns = [description[0] for description in cursor.description]
    config = dict(zip(columns, config_row))
    
    run_id = str(uuid.uuid4())
    start_time = datetime.utcnow()
    
    try:
        # Determine source query
        if config['extract_table_name']:
            query = f"SELECT * FROM {config['extract_table_name']}"
            source_type = "TABLE"
        elif config['extract_view_name']:
            query = f"SELECT * FROM {config['extract_view_name']}"
            source_type = "VIEW"
        else:
            raise ValueError("No source defined")
        
        print(f"   📊 Executing query: {query}")
        
        # Execute query and get data
        df = pd.read_sql_query(query, conn)
        rows_extracted = len(df)
        
        print(f"   📈 Retrieved {rows_extracted} rows")
        
        # Generate file name
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        file_name = f"{config['extract_file_name']}_{timestamp}.{config['extract_file_name_extension']}"
        
        # Create output directory
        file_dir = output_dir / config['extract_container'] / config['extract_directory']
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / file_name
        
        # Write file
        if config['output_format'] == 'csv':
            df.to_csv(file_path, index=False, sep=config['file_properties_column_delimiter'])
        
        print(f"   💾 Created file: {file_path}")
        print(f"   📋 File size: {file_path.stat().st_size} bytes")
        
        files_generated = 1
        
        # Create trigger file if configured
        trigger_file_path = None
        if config['is_trigger_file']:
            trigger_file_name = f"{config['extract_file_name']}{config['trigger_file_extension']}"
            trigger_file_path = file_dir / trigger_file_name
            trigger_file_path.touch()
            print(f"   🎯 Created trigger file: {trigger_file_path}")
        
        # Simulate compression if configured
        if config['is_compressed']:
            import zipfile
            compressed_file = file_path.with_suffix('.zip')
            with zipfile.ZipFile(compressed_file, 'w') as zf:
                zf.write(file_path, file_path.name)
            file_path.unlink()  # Remove original
            print(f"   🗜️  Compressed to: {compressed_file}")
            file_path = compressed_file
        
        # Log success
        cursor.execute("""
            INSERT INTO log_extract_generation 
            (extract_name, run_id, run_timestamp, run_status, rows_extracted, rows_written, files_generated, output_file_path, created_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            extract_name, run_id, start_time.isoformat(), 'SUCCESS',
            rows_extracted, rows_extracted, files_generated, str(file_path), datetime.utcnow().isoformat()
        ))
        
        conn.commit()
        print(f"   ✅ Extract {extract_name} completed successfully")
        
        return {
            'success': True,
            'file_path': file_path,
            'rows': rows_extracted,
            'trigger_file': trigger_file_path
        }
        
    except Exception as e:
        # Log error
        cursor.execute("""
            INSERT INTO log_extract_generation 
            (extract_name, run_id, run_timestamp, run_status, created_timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (extract_name, run_id, start_time.isoformat(), f'FAILED: {str(e)}', datetime.utcnow().isoformat()))
        
        conn.commit()
        print(f"   ❌ Extract {extract_name} failed: {str(e)}")
        return {'success': False, 'error': str(e)}

def main():
    """Main test execution"""
    print("🚀 Starting Extract Generation End-to-End Test")
    print("=" * 60)
    
    # Create temporary directory for output files
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir)
        print(f"📁 Output directory: {output_dir}")
        
        # Setup test database
        conn = setup_test_database()
        
        try:
            # Insert sample configurations
            insert_sample_configurations(conn)
            
            print("\n🎯 Running Extract Generation Tests")
            print("-" * 40)
            
            # Test different extract types
            extracts = ['SAMPLE_CUSTOMERS_DAILY', 'SAMPLE_SALES_SUMMARY']
            results = {}
            
            for extract_name in extracts:
                result = simulate_extract_generation(conn, extract_name, output_dir)
                results[extract_name] = result
                print()
            
            # Display results summary
            print("📊 Test Results Summary")
            print("=" * 30)
            
            for extract_name, result in results.items():
                status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
                print(f"{extract_name}: {status}")
                if result['success']:
                    print(f"  📄 File: {result['file_path'].name}")
                    print(f"  📈 Rows: {result['rows']}")
                    if result.get('trigger_file'):
                        print(f"  🎯 Trigger: {result['trigger_file'].name}")
                else:
                    print(f"  ❌ Error: {result['error']}")
                print()
            
            # Show generated files
            print("📁 Generated Files:")
            for file_path in output_dir.rglob('*'):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    rel_path = file_path.relative_to(output_dir)
                    print(f"  {rel_path} ({size} bytes)")
            
            # Show sample file contents
            csv_files = list(output_dir.rglob('*.csv'))
            if csv_files:
                print(f"\n📋 Sample content from {csv_files[0].name}:")
                content = csv_files[0].read_text()[:300]
                print(content[:300] + "..." if len(content) > 300 else content)
            
            # Show extract log
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM log_extract_generation ORDER BY created_timestamp DESC")
            logs = cursor.fetchall()
            
            if logs:
                print("\n📊 Extract Execution Log:")
                print("-" * 50)
                for log in logs:
                    print(f"Extract: {log[1]} | Status: {log[4]} | Rows: {log[5]} | Files: {log[7]}")
            
        finally:
            conn.close()
    
    print("\n🎉 Extract Generation test completed!")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test SQL files against Microsoft Fabric SQL Warehouse
"""
import pyodbc
import subprocess
import json
import os
from pathlib import Path

def get_access_token():
    """Get Azure access token for SQL database"""
    result = subprocess.run([
        'az', 'account', 'get-access-token', 
        '--resource', 'https://database.windows.net/',
        '--query', 'accessToken',
        '-o', 'tsv'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Failed to get access token: {result.stderr}")
    
    return result.stdout.strip()

def test_sql_file(cursor, sql_file_path):
    """Test a SQL file against the database"""
    print(f"\n=== Testing {sql_file_path} ===")
    
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Split SQL content by GO statements and semicolons
    sql_statements = []
    current_statement = ""
    
    for line in sql_content.split('\n'):
        line = line.strip()
        if line.upper() == 'GO':
            if current_statement.strip():
                sql_statements.append(current_statement.strip())
                current_statement = ""
        elif line and not line.startswith('--'):
            current_statement += line + '\n'
    
    # Add the last statement if it exists
    if current_statement.strip():
        sql_statements.append(current_statement.strip())
    
    # Execute each statement
    for i, statement in enumerate(sql_statements):
        if not statement.strip():
            continue
            
        print(f"Statement {i+1}: {statement[:100]}...")
        try:
            cursor.execute(statement)
            cursor.commit()
            print(f"✓ Statement {i+1} executed successfully")
        except Exception as e:
            print(f"✗ Statement {i+1} failed: {str(e)}")
            return False
    
    return True

def main():
    server = '73ddz4sy6afudhw374ntwskot4-wdfpwucwpx3unehwqdhlacwinu.datawarehouse.fabric.microsoft.com'
    database = 'sample_wh'
    
    # Get access token
    try:
        token = get_access_token()
        print("✓ Successfully obtained access token")
    except Exception as e:
        print(f"✗ Failed to get access token: {e}")
        return
    
    # Connection string with access token
    connection_string = f"""
    Driver={{ODBC Driver 18 for SQL Server}};
    Server={server};
    Database={database};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
    """
    
    try:
        # Connect to database
        conn = pyodbc.connect(connection_string, attrs_before={1256: token})
        cursor = conn.cursor()
        print("✓ Successfully connected to Fabric warehouse")
        
        # Test connection
        cursor.execute("SELECT 'Connection test successful' as test_result")
        result = cursor.fetchone()
        print(f"✓ Connection test: {result[0]}")
        
        # Test each SQL file
        sql_files = [
            'ingen_fab/packages/extract_generation/ddl_scripts/warehouse/000_schema_creation.sql',
            'ingen_fab/packages/extract_generation/ddl_scripts/warehouse/001_config_extract_details_create.sql',
            'ingen_fab/packages/extract_generation/ddl_scripts/warehouse/002_config_extract_generation_create.sql',
            'ingen_fab/packages/extract_generation/ddl_scripts/warehouse/003_log_extract_generation_create.sql',
            'ingen_fab/packages/extract_generation/ddl_scripts/warehouse/004_vw_extract_generation_latest_runs.sql',
            'ingen_fab/packages/extract_generation/ddl_scripts/warehouse/005_sample_data_insert.sql'
        ]
        
        results = {}
        for sql_file in sql_files:
            if Path(sql_file).exists():
                results[sql_file] = test_sql_file(cursor, sql_file)
            else:
                print(f"✗ File not found: {sql_file}")
                results[sql_file] = False
        
        # Summary
        print("\n=== SUMMARY ===")
        for file_path, success in results.items():
            status = "✓ PASSED" if success else "✗ FAILED"
            print(f"{status}: {Path(file_path).name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Database connection failed: {str(e)}")

if __name__ == "__main__":
    main()
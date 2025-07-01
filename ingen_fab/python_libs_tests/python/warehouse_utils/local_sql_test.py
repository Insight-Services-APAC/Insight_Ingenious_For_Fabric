from __future__ import annotations

import os
import sys
import traceback
import types
from pathlib import Path

import pandas as pd
import pyodbc

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Check if we're in a Fabric environment, if not mock the module
try:
    import notebookutils  # type: ignore # noqa: F401
except ImportError:
    # Mock notebookutils module since it's only available in Fabric notebooks
    from unittest.mock import MagicMock
    mock_notebookutils = types.ModuleType('notebookutils')
    mock_notebookutils.data = types.ModuleType('data')
    mock_notebookutils.data.connect_to_artifact = MagicMock()
    sys.modules['notebookutils'] = mock_notebookutils

from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


def connect_to_local_sql_server() -> pyodbc.Connection | None:
    """Connect to local SQL Server instance."""
    try:
        # Define connection parameters
        password = os.getenv('SQL_SERVER_PASSWORD', 'default_password')
        connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "UID=sa;"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(connection_string)
        print("✅ Connected to local SQL Server instance.")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to local SQL Server instance: {e}")
        return None


def test_warehouse_utils_initialization() -> warehouse_utils | None:
    """Test warehouse_utils initialization with SQL Server."""
    print("\n🧪 Testing warehouse_utils initialization...")
    try:
        # Get connection string for SQL Server dialect
        password = os.getenv('SQL_SERVER_PASSWORD', 'default_password')
        connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "UID=sa;"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )
        
        # Initialize warehouse_utils with SQL Server dialect
        utils = warehouse_utils(
            target_workspace_id="test_workspace",
            target_warehouse_id="test_warehouse",
            dialect="sqlserver",
            connection_string=connection_string
        )
        
        print("✅ warehouse_utils initialized successfully")
        print(f"   - Workspace ID: {utils.target_workspace_id}")
        print(f"   - Warehouse ID: {utils.target_store_id}")
        print(f"   - Dialect: {utils.dialect}")
        
        return utils
    except Exception as e:
        print(f"❌ warehouse_utils initialization failed: {e}")
        traceback.print_exc()
        return None


def test_connection(utils: warehouse_utils) -> bool:
    """Test warehouse_utils connection."""
    print("\n🧪 Testing warehouse_utils connection...")
    try:
        conn = utils.get_connection()
        if conn:
            print("✅ Connection established successfully")
            # Test basic query
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION;")
            row = cursor.fetchone()
            print(f"   - SQL Server Version: {row[0][:50]}...")
            cursor.close()
            return True
        else:
            print("❌ Connection failed")
            return False
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        traceback.print_exc()
        return False


def test_schema_operations(utils: warehouse_utils) -> bool:
    """Test schema creation and management."""
    print("\n🧪 Testing schema operations...")
    try:
        # Test schema creation
        test_schema = "test_schema"
        utils.create_schema_if_not_exists(test_schema)
        print(f"✅ Schema '{test_schema}' created or verified")
        
        # Test schema creation again (should not fail)
        utils.create_schema_if_not_exists(test_schema)
        print(f"✅ Schema '{test_schema}' creation idempotent")
        
        return True
    except Exception as e:
        print(f"❌ Schema operations failed: {e}")
        traceback.print_exc()
        return False


def test_table_existence_check(utils: warehouse_utils) -> bool:
    """Test table existence checking."""
    print("\n🧪 Testing table existence checks...")
    try:
        # Test non-existent table
        table_name = "non_existent_table"
        exists = utils.check_if_table_exists(table_name, "dbo")
        assert not exists, f"Table {table_name} should not exist"
        print(f"✅ Non-existent table correctly identified: {table_name}")
        
        # Test with custom schema
        exists_in_schema = utils.check_if_table_exists(table_name, "test_schema")
        assert not exists_in_schema, f"Table {table_name} should not exist in test_schema"
        print("✅ Non-existent table in custom schema correctly identified")
        
        return True
    except Exception as e:
        print(f"❌ Table existence check failed: {e}")
        traceback.print_exc()
        return False


def test_write_operations(utils: warehouse_utils) -> bool:
    """Test table writing operations."""
    print("\n🧪 Testing table write operations...")
    try:
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'department': ['Engineering', 'Sales', 'Marketing']
        })
        
        # Test overwrite mode (default)
        table_name = "test_employees"
        schema_name = "test_schema"
        
        utils.write_to_table(test_data, table_name, schema_name, mode="overwrite")
        print(f"✅ Table written successfully: {schema_name}.{table_name} (overwrite)")
        
        # Verify table exists now
        exists = utils.check_if_table_exists(table_name, schema_name)
        assert exists, f"Table {table_name} should exist after writing"
        print("✅ Table existence verified after write")
        
        # Test append mode
        additional_data = pd.DataFrame({
            'id': [4, 5],
            'name': ['David', 'Eve'],
            'age': [28, 32],
            'department': ['HR', 'Finance']
        })
        
        utils.write_to_table(additional_data, table_name, schema_name, mode="append")
        print(f"✅ Data appended successfully to {schema_name}.{table_name}")
        
        # Test error mode with existing table
        try:
            utils.write_to_table(test_data, table_name, schema_name, mode="error")
            print("❌ Should have raised error for existing table")
            return False
        except ValueError:
            print("✅ Error mode correctly raised exception for existing table")
        
        # Test ignore mode with existing table
        utils.write_to_table(test_data, table_name, schema_name, mode="ignore")
        print("✅ Ignore mode handled existing table correctly")
        
        return True
    except Exception as e:
        print(f"❌ Write operations failed: {e}")
        traceback.print_exc()
        return False


def test_query_execution(utils: warehouse_utils) -> bool:
    """Test direct query execution."""
    print("\n🧪 Testing query execution...")
    try:
        conn = utils.get_connection()
        
        # Test SELECT query
        query = "SELECT name, age FROM test_schema.test_employees WHERE age > 30"
        result = utils.execute_query(conn, query)
        
        if result is not None and len(result) > 0:
            print(f"✅ SELECT query executed successfully, returned {len(result)} rows")
            print(f"   - Sample result: {result.iloc[0].to_dict() if not result.empty else 'No data'}")
        else:
            print("✅ SELECT query executed (no results)")
        
        # Test DDL query (no results expected)
        ddl_query = "CREATE TABLE test_schema.temp_test (id INT, value VARCHAR(50))"
        result = utils.execute_query(conn, ddl_query)
        print("✅ DDL query executed successfully")
        
        # Clean up
        cleanup_query = "DROP TABLE test_schema.temp_test"
        utils.execute_query(conn, cleanup_query)
        print("✅ Cleanup query executed successfully")
        
        return True
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        traceback.print_exc()
        return False


def test_drop_operations(utils: warehouse_utils) -> bool:
    """Test table dropping operations."""
    print("\n🧪 Testing drop operations...")
    try:
        # Create additional test tables
        test_data = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
        
        utils.write_to_table(test_data, "test_table_1", "test_schema")
        utils.write_to_table(test_data, "test_table_2", "test_schema")
        utils.write_to_table(test_data, "other_table", "test_schema")
        print("✅ Created multiple test tables")
        
        # Test dropping tables with prefix
        utils.drop_all_tables(schema_name="test_schema", table_prefix="test_")
        print("✅ Dropped tables with prefix 'test_'")
        
        # Verify specific tables are gone
        exists_1 = utils.check_if_table_exists("test_table_1", "test_schema")
        exists_2 = utils.check_if_table_exists("test_table_2", "test_schema")
        
        assert not exists_1, "test_table_1 should be dropped"
        assert not exists_2, "test_table_2 should be dropped"
        print("✅ Prefix-based table dropping verified")
        
        return True
    except Exception as e:
        print(f"❌ Drop operations failed: {e}")
        traceback.print_exc()
        return False


def test_error_handling(utils: warehouse_utils) -> bool:
    """Test error handling scenarios."""
    print("\n🧪 Testing error handling...")
    try:
        # Test invalid query
        try:
            conn = utils.get_connection()
            utils.execute_query(conn, "INVALID SQL SYNTAX")
            print("❌ Should have raised error for invalid SQL")
            return False
        except Exception:
            print("✅ Invalid SQL properly rejected")
        
        # Test writing invalid data type
        try:
            invalid_data = pd.DataFrame({'col': [object()]})  # Non-serializable object
            utils.write_to_table(invalid_data, "invalid_table", "test_schema")
            print("❌ Should have raised error for invalid data")
            return False
        except Exception:
            print("✅ Invalid data properly rejected")
        
        return True
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        traceback.print_exc()
        return False


def cleanup_test_data(utils: warehouse_utils) -> None:
    """Clean up test data and schemas."""
    print("\n🧹 Cleaning up test data...")
    try:
        conn = utils.get_connection()
        
        # Drop test tables if they exist
        test_tables = ["test_employees", "test_table_1", "test_table_2", "other_table"]
        for table in test_tables:
            try:
                if utils.check_if_table_exists(table, "test_schema"):
                    utils.execute_query(conn, f"DROP TABLE test_schema.{table}")
                    print(f"✅ Dropped table: test_schema.{table}")
            except Exception:
                pass  # Ignore cleanup errors
        
        # Drop test schema
        try:
            utils.execute_query(conn, "DROP SCHEMA test_schema")
            print("✅ Dropped schema: test_schema")
        except Exception:
            pass  # Ignore cleanup errors
            
    except Exception as e:
        print(f"⚠️ Cleanup had issues (not critical): {e}")


def check_sql_server_availability() -> bool:
    """Check if SQL Server is available and accessible."""
    try:
        password = os.getenv('SQL_SERVER_PASSWORD', 'default_password')
        connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "UID=sa;"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(connection_string)
        conn.close()
        return True
    except Exception:
        return False


def run_comprehensive_tests() -> None:
    """Run comprehensive tests of warehouse_utils against local SQL Server."""
    print("🚀 Starting comprehensive warehouse_utils tests against local SQL Server")
    print("=" * 80)
    
    # Check if SQL Server is available
    sql_server_available = check_sql_server_availability()
    
    if not sql_server_available:
        print("⚠️  SQL Server is not available. Running demo with mocked connections.")
        print("   To test with real SQL Server:")
        print("   1. Ensure SQL Server is running on localhost:1433")
        print("   2. Set SQL_SERVER_PASSWORD environment variable")
        print("   3. Re-run this script")
        print("=" * 80)
        
        # Import the demo function and run it
        from local_sql_test_demo import run_comprehensive_tests_demo
        run_comprehensive_tests_demo()
        return
    
    print("✅ SQL Server detected! Running tests against real database.")
    print("=" * 80)
    
    # Test basic connection first
    conn = connect_to_local_sql_server()
    if not conn:
        print("❌ Cannot connect to SQL Server. Ensure SQL Server is running and accessible.")
        return
    conn.close()
    
    # Initialize warehouse_utils
    utils = test_warehouse_utils_initialization()
    if not utils:
        print("❌ Cannot initialize warehouse_utils. Aborting tests.")
        return
    
    # Run test suite
    tests = [
        ("Connection Test", lambda: test_connection(utils)),
        ("Schema Operations", lambda: test_schema_operations(utils)),
        ("Table Existence Check", lambda: test_table_existence_check(utils)),
        ("Write Operations", lambda: test_write_operations(utils)),
        ("Query Execution", lambda: test_query_execution(utils)),
        ("Drop Operations", lambda: test_drop_operations(utils)),
        ("Error Handling", lambda: test_error_handling(utils)),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {e}")
    
    # Cleanup
    cleanup_test_data(utils)
    
    # Final summary
    print(f"\n{'='*80}")
    print("🎯 TEST SUMMARY")
    print(f"{'='*80}")
    print(f"✅ Passed: {passed}/{total}")
    print(f"❌ Failed: {total - passed}/{total}")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! warehouse_utils is working correctly with local SQL Server.")
    else:
        print("⚠️ Some tests failed. Check the output above for details.")
    
    # Initialize warehouse_utils
    utils = test_warehouse_utils_initialization()
    if not utils:
        print("❌ Cannot initialize warehouse_utils. Aborting tests.")
        return
    
    # Run test suite
    tests = [
        ("Connection Test", lambda: test_connection(utils)),
        ("Schema Operations", lambda: test_schema_operations(utils)),
        ("Table Existence Check", lambda: test_table_existence_check(utils)),
        ("Write Operations", lambda: test_write_operations(utils)),
        ("Query Execution", lambda: test_query_execution(utils)),
        ("Drop Operations", lambda: test_drop_operations(utils)),
        ("Error Handling", lambda: test_error_handling(utils)),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {e}")
    
    # Cleanup
    cleanup_test_data(utils)
    
    # Final summary
    print(f"\n{'='*80}")
    print("🎯 TEST SUMMARY")
    print(f"{'='*80}")
    print(f"✅ Passed: {passed}/{total}")
    print(f"❌ Failed: {total - passed}/{total}")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! warehouse_utils is working correctly with local SQL Server.")
    else:
        print("⚠️ Some tests failed. Check the output above for details.")


if __name__ == "__main__":
    run_comprehensive_tests()
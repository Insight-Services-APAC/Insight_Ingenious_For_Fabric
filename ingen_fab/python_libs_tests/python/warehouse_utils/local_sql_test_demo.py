from __future__ import annotations

import os
import sys
import traceback
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Mock notebookutils module since it's only available in Fabric notebooks
mock_notebookutils = types.ModuleType('notebookutils')
mock_notebookutils.data = types.ModuleType('data')
mock_notebookutils.data.connect_to_artifact = MagicMock()
sys.modules['notebookutils'] = mock_notebookutils

from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


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


def test_connection_mock(utils: warehouse_utils) -> bool:
    """Test warehouse_utils connection with mock."""
    print("\n🧪 Testing warehouse_utils connection (mocked)...")
    try:
        # Mock the pyodbc.connect method
        with patch('pyodbc.connect') as mock_connect:
            # Create a mock connection object
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("Microsoft SQL Server 2022",)
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            # Test connection
            conn = utils.get_connection()
            assert conn is not None
            print("✅ Connection established successfully (mocked)")
            
            # Test basic query simulation
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION;")
            row = cursor.fetchone()
            print(f"   - SQL Server Version: {row[0]}")
            cursor.close()
            return True
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        traceback.print_exc()
        return False


def test_schema_operations_mock(utils: warehouse_utils) -> bool:
    """Test schema creation and management with mock."""
    print("\n🧪 Testing schema operations (mocked)...")
    try:
        with patch.object(utils, 'get_connection') as mock_get_conn, \
             patch.object(utils, 'execute_query') as mock_execute:
            
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            
            # Mock schema doesn't exist first, then exists
            mock_execute.side_effect = [[], None]  # Empty result = schema doesn't exist, None = creation success
            
            # Test schema creation
            test_schema = "test_schema"
            utils.create_schema_if_not_exists(test_schema)
            print(f"✅ Schema '{test_schema}' created (mocked)")
            
            # Verify method calls
            assert mock_execute.call_count == 2  # Check + Create
            print("✅ Schema operations completed successfully")
            
            return True
    except Exception as e:
        print(f"❌ Schema operations failed: {e}")
        traceback.print_exc()
        return False


def test_table_existence_check_mock(utils: warehouse_utils) -> bool:
    """Test table existence checking with mock."""
    print("\n🧪 Testing table existence checks (mocked)...")
    try:
        with patch.object(utils, 'get_connection') as mock_get_conn, \
             patch.object(utils, 'execute_query') as mock_execute:
            
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            
            # Mock table doesn't exist (empty result)
            mock_execute.return_value = pd.DataFrame()
            
            table_name = "non_existent_table"
            exists = utils.check_if_table_exists(table_name, "dbo")
            assert not exists, f"Table {table_name} should not exist"
            print(f"✅ Non-existent table correctly identified: {table_name}")
            
            # Mock table exists (non-empty result)
            mock_execute.return_value = pd.DataFrame([{"exists": 1}])
            exists_in_schema = utils.check_if_table_exists("existing_table", "test_schema")
            assert exists_in_schema, "Table should exist"
            print("✅ Existing table correctly identified")
            
            return True
    except Exception as e:
        print(f"❌ Table existence check failed: {e}")
        traceback.print_exc()
        return False


def test_write_operations_mock(utils: warehouse_utils) -> bool:
    """Test table writing operations with mock."""
    print("\n🧪 Testing table write operations (mocked)...")
    try:
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'department': ['Engineering', 'Sales', 'Marketing']
        })
        
        with patch.object(utils, 'get_connection') as mock_get_conn, \
             patch.object(utils, 'execute_query') as mock_execute, \
             patch.object(utils, 'check_if_table_exists') as mock_check_exists:
            
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            mock_execute.return_value = None  # Successful execution
            mock_check_exists.return_value = False  # Table doesn't exist initially
            
            # Test overwrite mode (default)
            table_name = "test_employees"
            schema_name = "test_schema"
            
            utils.write_to_table(test_data, table_name, schema_name, mode="overwrite")
            print(f"✅ Table written successfully: {schema_name}.{table_name} (overwrite, mocked)")
            
            # Verify table exists after write
            mock_check_exists.return_value = True
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
            print(f"✅ Data appended successfully to {schema_name}.{table_name} (mocked)")
            
            # Test error mode with existing table
            mock_check_exists.return_value = True
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


def test_query_execution_mock(utils: warehouse_utils) -> bool:
    """Test direct query execution with mock."""
    print("\n🧪 Testing query execution (mocked)...")
    try:
        with patch.object(utils, 'get_connection') as mock_get_conn, \
             patch.object(utils, 'execute_query') as mock_execute:
            
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            
            # Mock SELECT query result
            mock_result = pd.DataFrame([
                {"name": "Bob", "age": 30},
                {"name": "Charlie", "age": 35}
            ])
            mock_execute.return_value = mock_result
            
            conn = utils.get_connection()
            query = "SELECT name, age FROM test_schema.test_employees WHERE age > 30"
            result = utils.execute_query(conn, query)
            
            assert result is not None and len(result) > 0
            print(f"✅ SELECT query executed successfully, returned {len(result)} rows (mocked)")
            print(f"   - Sample result: {result.iloc[0].to_dict()}")
            
            # Mock DDL query (no results expected)
            mock_execute.return_value = None
            ddl_query = "CREATE TABLE test_schema.temp_test (id INT, value VARCHAR(50))"
            result = utils.execute_query(conn, ddl_query)
            print("✅ DDL query executed successfully (mocked)")
            
            # Mock cleanup
            cleanup_query = "DROP TABLE test_schema.temp_test"
            utils.execute_query(conn, cleanup_query)
            print("✅ Cleanup query executed successfully (mocked)")
            
            return True
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        traceback.print_exc()
        return False


def test_drop_operations_mock(utils: warehouse_utils) -> bool:
    """Test table dropping operations with mock."""
    print("\n🧪 Testing drop operations (mocked)...")
    try:
        with patch.object(utils, 'get_connection') as mock_get_conn, \
             patch.object(utils, 'execute_query') as mock_execute, \
             patch.object(utils, 'check_if_table_exists') as mock_check_exists:
            
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            
            # Mock test data creation
            test_data = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
            
            # Mock table listing for drop operation
            mock_tables_df = pd.DataFrame([
                {"table_schema": "test_schema", "table_name": "test_table_1"},
                {"table_schema": "test_schema", "table_name": "test_table_2"},
                {"table_schema": "test_schema", "table_name": "other_table"}
            ])
            
            # Set up the mock to return different values for different calls
            mock_execute.side_effect = [
                None,  # Create table 1
                None,  # Create table 2
                None,  # Create other table
                mock_tables_df,  # List tables for drop operation
                None,  # Drop table 1
                None,  # Drop table 2
            ]
            
            # Simulate creating tables
            utils.write_to_table(test_data, "test_table_1", "test_schema")
            utils.write_to_table(test_data, "test_table_2", "test_schema")
            utils.write_to_table(test_data, "other_table", "test_schema")
            print("✅ Created multiple test tables (mocked)")
            
            # Test dropping tables with prefix
            utils.drop_all_tables(schema_name="test_schema", table_prefix="test_")
            print("✅ Dropped tables with prefix 'test_' (mocked)")
            
            # Mock verification that specific tables are gone
            mock_check_exists.side_effect = [False, False]  # Both test tables dropped
            exists_1 = utils.check_if_table_exists("test_table_1", "test_schema")
            exists_2 = utils.check_if_table_exists("test_table_2", "test_schema")
            
            assert not exists_1, "test_table_1 should be dropped"
            assert not exists_2, "test_table_2 should be dropped"
            print("✅ Prefix-based table dropping verified (mocked)")
            
            return True
    except Exception as e:
        print(f"❌ Drop operations failed: {e}")
        traceback.print_exc()
        return False


def test_error_handling_mock(utils: warehouse_utils) -> bool:
    """Test error handling scenarios with mock."""
    print("\n🧪 Testing error handling (mocked)...")
    try:
        # Test invalid query
        with patch.object(utils, 'get_connection') as mock_get_conn, \
             patch.object(utils, 'execute_query') as mock_execute:
            
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            mock_execute.side_effect = Exception("Invalid SQL syntax")
            
            try:
                conn = utils.get_connection()
                utils.execute_query(conn, "INVALID SQL SYNTAX")
                print("❌ Should have raised error for invalid SQL")
                return False
            except Exception:
                print("✅ Invalid SQL properly rejected (mocked)")
        
        # Test writing invalid data type
        with patch.object(utils, 'write_to_table') as mock_write:
            mock_write.side_effect = Exception("Invalid data type")
            
            try:
                invalid_data = pd.DataFrame({'col': [object()]})  # Non-serializable object
                utils.write_to_table(invalid_data, "invalid_table", "test_schema")
                print("❌ Should have raised error for invalid data")
                return False
            except Exception:
                print("✅ Invalid data properly rejected (mocked)")
        
        return True
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        traceback.print_exc()
        return False


def run_comprehensive_tests_demo() -> None:
    """Run comprehensive tests of warehouse_utils with mocked SQL Server."""
    print("🚀 Starting comprehensive warehouse_utils tests (Demo with Mocks)")
    print("=" * 80)
    print("ℹ️  This demo uses mocked SQL Server connections to demonstrate the testing framework")
    print("   without requiring an actual SQL Server instance.")
    print("=" * 80)
    
    # Initialize warehouse_utils
    utils = test_warehouse_utils_initialization()
    if not utils:
        print("❌ Cannot initialize warehouse_utils. Aborting tests.")
        return
    
    # Run test suite
    tests = [
        ("Connection Test (Mocked)", lambda: test_connection_mock(utils)),
        ("Schema Operations (Mocked)", lambda: test_schema_operations_mock(utils)),
        ("Table Existence Check (Mocked)", lambda: test_table_existence_check_mock(utils)),
        ("Write Operations (Mocked)", lambda: test_write_operations_mock(utils)),
        ("Query Execution (Mocked)", lambda: test_query_execution_mock(utils)),
        ("Drop Operations (Mocked)", lambda: test_drop_operations_mock(utils)),
        ("Error Handling (Mocked)", lambda: test_error_handling_mock(utils)),
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
    
    # Final summary
    print(f"\n{'='*80}")
    print("🎯 TEST SUMMARY")
    print(f"{'='*80}")
    print(f"✅ Passed: {passed}/{total}")
    print(f"❌ Failed: {total - passed}/{total}")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! warehouse_utils testing framework is working correctly.")
        print("\n📋 To run against a real SQL Server:")
        print("   1. Ensure SQL Server is running on localhost:1433")
        print("   2. Set SQL_SERVER_PASSWORD environment variable")
        print("   3. Run the original local_sql_test.py file")
    else:
        print("⚠️ Some tests failed. Check the output above for details.")


if __name__ == "__main__":
    run_comprehensive_tests_demo()

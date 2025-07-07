import datetime
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys

# Mock all required modules before importing warehouse_utils
sys.modules['notebookutils'] = Mock()
sys.modules['pyodbc'] = Mock()
sys.modules['sqlparse'] = Mock(format=Mock(return_value="formatted_sql"))

# Mock the sql_templates module
mock_sql_templates = Mock()
mock_sql_templates.SQLTemplates = Mock(return_value=Mock(render=Mock(return_value="MOCKED SQL")))
sys.modules['ingen_fab.python_libs.python.sql_templates'] = mock_sql_templates

from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


@pytest.fixture(scope="module")
def utils():
    return warehouse_utils("your-workspace-id", "your-warehouse-id", dialect="fabric")


@pytest.fixture
def local_utils():
    return warehouse_utils(
        dialect="local", 
        connection_string="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;PWD=test;TrustServerCertificate=yes;"
    )


@pytest.fixture
def sales_data():
    return pd.DataFrame([
        {"sale_id": 1, "product_name": "Laptop", "customer_name": "John Doe", "amount": 1200, "sale_date": "2024-06-01 10:00:00"},
        {"sale_id": 2, "product_name": "Mouse", "customer_name": "Jane Smith", "amount": 25, "sale_date": "2024-06-02 14:30:00"},
        {"sale_id": 3, "product_name": "Keyboard", "customer_name": "Bob Johnson", "amount": 75, "sale_date": "2024-06-03 09:15:00"},
        {"sale_id": 4, "product_name": "Monitor", "customer_name": "Alice Brown", "amount": 300, "sale_date": "2024-06-04 16:45:00"},
        {"sale_id": 5, "product_name": "Headphones", "customer_name": "Carol Wilson", "amount": 150, "sale_date": "2024-06-05 11:20:00"},
    ])


@pytest.fixture
def customers_data():
    return pd.DataFrame([
        {"customer_id": 1, "customer_name": "John Doe", "email": "john.doe@example.com", "signup_date": datetime.datetime(2023, 1, 15, 10, 0)},
        {"customer_id": 2, "customer_name": "Jane Smith", "email": "jane.smith@example.com", "signup_date": datetime.datetime(2023, 2, 20, 14, 30)},
        {"customer_id": 3, "customer_name": "Alice Brown", "email": "alice.brown@example.com", "signup_date": datetime.datetime(2023, 3, 25, 9, 15)},
    ])


@pytest.fixture
def orders_data():
    return pd.DataFrame([
        {"order_id": 101, "customer_id": 1, "order_date": datetime.datetime(2023, 4, 10, 11, 0), "total_amount": 1200},
        {"order_id": 102, "customer_id": 2, "order_date": datetime.datetime(2023, 4, 15, 16, 45), "total_amount": 300},
        {"order_id": 103, "customer_id": 3, "order_date": datetime.datetime(2023, 4, 20, 13, 20), "total_amount": 450},
    ])


@pytest.fixture
def products_data():
    return pd.DataFrame([
        {"product_id": 1, "product_name": "Laptop", "category": "Electronics", "price": 1200},
        {"product_id": 2, "product_name": "Mouse", "category": "Accessories", "price": 25},
        {"product_id": 3, "product_name": "Keyboard", "category": "Accessories", "price": 75},
    ])


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_fabric_connection(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    conn = utils.get_connection()
    assert conn == mock_conn
    mock_notebookutils.data.connect_to_artifact.assert_called_once_with(
        utils.target_store_id, utils.target_workspace_id
    )


@patch('ingen_fab.python_libs.python.warehouse_utils.pyodbc')
def test_local_connection(mock_pyodbc, local_utils):
    mock_conn = Mock()
    mock_pyodbc.connect.return_value = mock_conn
    
    conn = local_utils.get_connection()
    assert conn == mock_conn
    mock_pyodbc.connect.assert_called_once_with(local_utils.connection_string)


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_check_if_table_exists(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([{"exists": 1}])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT 1"):
        exists = utils.check_if_table_exists("test_table", "dbo")
        assert exists is True


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_write_and_read_table(mock_notebookutils, utils, sales_data):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame()  # Empty for table doesn't exist
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render') as mock_render:
        mock_render.return_value = "MOCKED SQL"
        utils.write_to_table(sales_data, "sales_data", mode="overwrite")
        
        # Verify SQL render was called for drop and create operations
        assert mock_render.call_count >= 2
        mock_conn.query.assert_called()


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_write_with_different_modes(mock_notebookutils, utils, sales_data):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame()
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="MOCKED SQL"):
        # Test overwrite mode
        utils.write_to_table(sales_data, "test_table", mode="overwrite")
        
        # Test append mode
        utils.write_to_table(sales_data, "test_table", mode="append")
        
        # Test ignore mode
        utils.write_to_table(sales_data, "test_table", mode="ignore")


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_error_mode_with_existing_table(mock_notebookutils, utils, sales_data):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([{"exists": 1}])  # Table exists
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT 1"):
        with pytest.raises(ValueError, match="Table test_table already exists"):
            utils.write_to_table(sales_data, "test_table", mode="error")


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_create_schema_if_not_exists(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame()  # Schema doesn't exist
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT 1"):
        utils.create_schema_if_not_exists("test_schema")
        assert mock_conn.query.call_count >= 2  # Check + Create


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_list_tables(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([
        {"table_name": "sales_data"},
        {"table_name": "customers"},
        {"table_name": "orders"}
    ])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SHOW TABLES"):
        tables = utils.list_tables()
        assert isinstance(tables, list)
        assert "sales_data" in tables
        assert len(tables) == 3


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_multiple_table_management(mock_notebookutils, utils, customers_data, orders_data, products_data):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame()
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="MOCKED SQL"):
        utils.write_to_table(customers_data, "customers", mode="overwrite")
        utils.write_to_table(orders_data, "orders", mode="overwrite")
        utils.write_to_table(products_data, "products", mode="overwrite")
        
        # Verify multiple calls were made
        assert mock_conn.query.call_count >= 6  # 3 tables * 2 operations each


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_get_table_row_count(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([{"count": 100}])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT COUNT(*) as count FROM table"):
        count = utils.get_table_row_count("test_table")
        assert count == 100


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_get_table_schema(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([{
        "column_name": "id",
        "data_type": "int",
        "is_nullable": "NO"
    }])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="DESCRIBE table"):
        schema = utils.get_table_schema("test_table")
        assert isinstance(schema, dict)
        assert "column_name" in schema


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_read_table_with_filters(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([{"id": 1, "name": "test"}])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT * FROM table WHERE id = 1"):
        result = utils.read_table(
            "test_table", 
            columns=["id", "name"], 
            limit=10, 
            filters={"id": 1}
        )
        assert isinstance(result, pd.DataFrame)


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_delete_from_table(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_result = Mock()
    mock_result.rowcount = 5
    mock_conn.query.return_value = mock_result
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="DELETE FROM table WHERE id = 1"):
        deleted_count = utils.delete_from_table("test_table", filters={"id": 1})
        assert deleted_count == 5  # Default return when rowcount not available


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_drop_all_tables(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.side_effect = [
        pd.DataFrame([
            {"table_schema": "dbo", "table_name": "table1"},
            {"table_schema": "dbo", "table_name": "table2"}
        ]),
        None,  # Drop table1
        None   # Drop table2
    ]
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="MOCKED SQL"):
        utils.drop_all_tables(table_prefix="test_")
        assert mock_conn.query.call_count == 3  # List + 2 drops


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_rename_table(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = None
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="ALTER TABLE old_name RENAME TO new_name"):
        utils.rename_table("old_table", "new_table")
        mock_conn.query.assert_called_once()


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_create_table(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = None
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    schema = {"id": "int", "name": "varchar(50)"}
    with patch.object(utils.sql, 'render', return_value="CREATE TABLE test (id int, name varchar(50))"):
        utils.create_table("test_table", schema=schema)
        mock_conn.query.assert_called_once()


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_drop_table(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = None
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="DROP TABLE test_table"):
        utils.drop_table("test_table")
        mock_conn.query.assert_called_once()


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_list_schemas(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([
        {"schema_name": "dbo"},
        {"schema_name": "test_schema"}
    ])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT schema_name FROM schemas"):
        schemas = utils.list_schemas()
        assert isinstance(schemas, list)
        assert "dbo" in schemas
        assert len(schemas) == 2


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_get_table_metadata(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.return_value = pd.DataFrame([{
        "table_name": "test_table",
        "created_date": "2024-01-01",
        "row_count": 1000
    }])
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with patch.object(utils.sql, 'render', return_value="SELECT metadata FROM table"):
        metadata = utils.get_table_metadata("test_table")
        assert isinstance(metadata, dict)
        assert "table_name" in metadata


def test_vacuum_table(utils):
    # vacuum_table is a no-op for SQL warehouses
    utils.vacuum_table("test_table")  # Should not raise any exception


def test_properties(utils):
    assert utils.target_workspace_id == "your-workspace-id"
    assert utils.target_store_id == "your-warehouse-id"


def test_properties_not_set():
    utils = warehouse_utils()
    with pytest.raises(ValueError, match="target_workspace_id is not set"):
        _ = utils.target_workspace_id
    
    with pytest.raises(ValueError, match="target_warehouse_id is not set"):
        _ = utils.target_store_id


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_execute_query_error_handling(mock_notebookutils, utils):
    mock_conn = Mock()
    mock_conn.query.side_effect = Exception("Database error")
    mock_notebookutils.data.connect_to_artifact.return_value = mock_conn
    
    with pytest.raises(Exception, match="Database error"):
        utils.execute_query(mock_conn, "SELECT * FROM invalid_table")


@patch('ingen_fab.python_libs.python.warehouse_utils.notebookutils')
def test_connection_error_handling(mock_notebookutils, utils):
    mock_notebookutils.data.connect_to_artifact.side_effect = Exception("Connection failed")
    
    with pytest.raises(Exception, match="Connection failed"):
        utils.get_connection()
import pathlib
import sys
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


def test_get_connection_fabric():
    wu = warehouse_utils("ws", "wh")
    mock_conn = object()
    with mock.patch(
        "ingen_fab.python_libs.python.warehouse_utils.notebookutils.data.connect_to_artifact",
        return_value=mock_conn,
    ):
        assert wu.get_connection() is mock_conn


def test_get_connection_sqlserver():
    wu = warehouse_utils(None, None, dialect="sql_server", connection_string="dsn")
    mock_conn = object()
    with mock.patch(
        "ingen_fab.python_libs.python.warehouse_utils.pyodbc.connect",
        return_value=mock_conn,
    ):
        assert wu.get_connection() is mock_conn


def test_execute_query_fabric():
    wu = warehouse_utils("ws", "wh")  # noqa: F841

    def test_create_schema_if_not_exists_schema_exists():
        wu = warehouse_utils("ws", "wh")
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query", return_value=[(1,)]),
        ):
            wu.create_schema_if_not_exists("myschema")  # Should not attempt to create schema if exists

    def test_create_schema_if_not_exists_schema_missing():
        wu = warehouse_utils("ws", "wh")
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query", side_effect=[[], None]) as exec_mock,
        ):
            wu.create_schema_if_not_exists("myschema")
            # Should call execute_query twice: check and create
            assert exec_mock.call_count == 2

    def test_create_schema_if_not_exists_raises():
        wu = warehouse_utils("ws", "wh")
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query", side_effect=Exception("fail")),
        ):
            try:
                wu.create_schema_if_not_exists("myschema")
            except Exception as e:
                assert str(e) == "fail"

    def test_write_to_warehouse_table_overwrite():
        wu = warehouse_utils("ws", "wh")
        df = type(
            "DF",
            (),
            {
                "iterrows": lambda self: iter([(0, ["a", 1])]),
                "columns": ["col1", "col2"],
            },
        )()
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query") as exec_mock,
            mock.patch.object(wu.sql, "render", side_effect=["DROP", "CREATE"]),
        ):
            wu.write_to_warehouse_table(df, "table", mode="overwrite")
            assert exec_mock.call_count == 2

    def test_write_to_warehouse_table_append():
        wu = warehouse_utils("ws", "wh")
        df = type(
            "DF",
            (),
            {
                "iterrows": lambda self: iter([(0, ["a", 1])]),
                "columns": ["col1", "col2"],
            },
        )()
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query") as exec_mock,
            mock.patch.object(wu.sql, "render", return_value="INSERT"),
        ):
            wu.write_to_warehouse_table(df, "table", mode="append")
            assert exec_mock.call_count == 1

    def test_write_to_warehouse_table_errorifexists_exists():
        wu = warehouse_utils("ws", "wh")
        df = type(
            "DF",
            (),
            {
                "iterrows": lambda self: iter([(0, ["a", 1])]),
                "columns": ["col1", "col2"],
            },
        )()
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "check_if_table_exists", return_value=True),
        ):
            try:
                wu.write_to_warehouse_table(df, "table", mode="errorifexists")
            except ValueError as e:
                assert "already exists" in str(e)

    def test_write_to_warehouse_table_errorifexists_not_exists():
        wu = warehouse_utils("ws", "wh")
        df = type(
            "DF",
            (),
            {
                "iterrows": lambda self: iter([(0, ["a", 1])]),
                "columns": ["col1", "col2"],
            },
        )()
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "check_if_table_exists", return_value=False),
            mock.patch.object(wu, "execute_query") as exec_mock,
            mock.patch.object(wu.sql, "render", return_value="CREATE"),
        ):
            wu.write_to_warehouse_table(df, "table", mode="errorifexists")
            assert exec_mock.call_count == 1

    def test_write_to_warehouse_table_ignore_exists():
        wu = warehouse_utils("ws", "wh")
        df = type(
            "DF",
            (),
            {
                "iterrows": lambda self: iter([(0, ["a", 1])]),
                "columns": ["col1", "col2"],
            },
        )()
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "check_if_table_exists", return_value=True),
            mock.patch.object(wu, "execute_query") as exec_mock,
        ):
            wu.write_to_warehouse_table(df, "table", mode="ignore")
            exec_mock.assert_not_called()

    def test_write_to_warehouse_table_ignore_not_exists():
        wu = warehouse_utils("ws", "wh")
        df = type(
            "DF",
            (),
            {
                "iterrows": lambda self: iter([(0, ["a", 1])]),
                "columns": ["col1", "col2"],
            },
        )()
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "check_if_table_exists", return_value=False),
            mock.patch.object(wu, "execute_query") as exec_mock,
            mock.patch.object(wu.sql, "render", return_value="CREATE"),
        ):
            wu.write_to_warehouse_table(df, "table", mode="ignore")
            assert exec_mock.call_count == 1

    def test_drop_all_tables_handles_empty():
        wu = warehouse_utils("ws", "wh")
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query", return_value=[]),
            mock.patch.object(wu.sql, "render", return_value="LIST"),
        ):
            wu.drop_all_tables()

    def test_drop_all_tables_handles_dicts():
        wu = warehouse_utils("ws", "wh")
        with (
            mock.patch.object(wu, "get_connection", return_value="conn"),
            mock.patch.object(wu, "execute_query", side_effect=[[{"name": "t1"}], None]),
            mock.patch.object(wu.sql, "render", side_effect=["LIST", "DROP"]),
        ):
            wu.drop_all_tables()

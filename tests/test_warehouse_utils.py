import pathlib
import sys
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


class DummyCursor:
    def __init__(self):
        self.executed = None
        self.description = [("col",)]

    def execute(self, query):
        self.executed = query

    def fetchall(self):
        return [(1,)]


class DummyConn:
    def __init__(self):
        self.cursor_obj = DummyCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True


class DummyPD:
    class DataFrame:
        @classmethod
        def from_records(cls, rows, columns):
            return {"rows": rows, "columns": columns}


class DummyFabricConn:
    def __init__(self):
        self.last_query = None

    def query(self, q):
        self.last_query = q
        return [1]


def test_get_connection_fabric():
    wu = warehouse_utils("ws", "wh")
    mock_conn = object()
    with mock.patch(
        "ingen_fab.python_libs.python.warehouse_utils.notebookutils.data.connect_to_artifact",
        return_value=mock_conn,
    ):
        assert wu.get_connection() is mock_conn


def test_get_connection_sqlserver():
    wu = warehouse_utils(None, None, dialect="sqlserver", connection_string="dsn")
    mock_conn = object()
    with mock.patch("ingen_fab.python_libs.python.warehouse_utils.pyodbc.connect", return_value=mock_conn):
        assert wu.get_connection() is mock_conn


def test_execute_query_fabric():
    wu = warehouse_utils("ws", "wh")
    conn = DummyFabricConn()
    result = wu.execute_query(conn, "SELECT 1")
    assert result == [1]
    assert conn.last_query == "SELECT 1"


def test_execute_query_sqlserver():
    wu = warehouse_utils(None, None, dialect="sqlserver", connection_string="dsn")
    conn = DummyConn()
    with mock.patch("ingen_fab.python_libs.python.warehouse_utils.pd", DummyPD()):
        result = wu.execute_query(conn, "SELECT 1")
    assert result == {"rows": [(1,)], "columns": ["col"]}
    assert conn.cursor_obj.executed == "SELECT 1"


def test_check_if_table_exists_success():
    wu = warehouse_utils("ws", "wh")
    with mock.patch.object(wu, "get_connection", return_value="conn"), \
         mock.patch.object(wu, "execute_query", return_value=None):
        assert wu.check_if_table_exists("table") is True


def test_check_if_table_exists_failure():
    wu = warehouse_utils("ws", "wh")
    with mock.patch.object(wu, "get_connection", return_value="conn"), \
         mock.patch.object(wu, "execute_query", side_effect=Exception("boom")):
        assert wu.check_if_table_exists("table") is False


def test_drop_all_tables():
    wu = warehouse_utils("ws", "wh")
    with mock.patch.object(wu, "get_connection", return_value="conn"), \
         mock.patch.object(wu, "execute_query") as exec_mock, \
         mock.patch.object(wu.sql, "render", side_effect=["LIST", "DROP"]):
        wu.drop_all_tables()
        assert exec_mock.call_count >= 1

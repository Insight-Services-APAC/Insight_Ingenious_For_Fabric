import pathlib
import sys
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from ingen_fab.python_libs.python.sql_templates import SQLTemplates


def test_render_drop_table_fabric():
    tmpl = SQLTemplates(dialect="fabric")
    sql = tmpl.render("drop_table", table_name="my_table")
    assert sql == "DROP TABLE IF EXISTS my_table"


def test_render_list_tables_sqlserver():
    tmpl = SQLTemplates(dialect="sqlserver")
    sql = tmpl.render("list_tables", prefix="abc")
    expected = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'abc%'"
    assert sql == expected

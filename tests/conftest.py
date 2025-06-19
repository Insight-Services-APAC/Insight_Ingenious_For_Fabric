import sys
import types
import re

# Minimal jinja2 substitute
jinja2 = types.ModuleType("jinja2")
class Template:
    def __init__(self, s: str):
        self.s = s
    def render(self, **kwargs):
        result = self.s
        def repl_cond(match):
            var = match.group(1).strip()
            content = match.group(2)
            return content if kwargs.get(var) else ""
        result = re.sub(r"{% if ([^%]+)%}(.*?){% endif %}", repl_cond, result)
        for k, v in kwargs.items():
            result = result.replace(f"{{{{ {k} }}}}", str(v))
        return result
jinja2.Template = Template
sys.modules.setdefault("jinja2", jinja2)

# Minimal notebookutils substitute
notebookutils = types.ModuleType("notebookutils")
class DummySession:
    def query(self, _):
        return types.SimpleNamespace(collect=lambda: [])
    def execute(self, _):
        pass
notebookutils.mssparkutils = types.SimpleNamespace(session=DummySession(), notebook=types.SimpleNamespace(exit=lambda x: None))
notebookutils.data = types.SimpleNamespace(connect_to_artifact=lambda *_: None)
sys.modules.setdefault("notebookutils", notebookutils)

# Minimal pandas substitute
pandas = types.ModuleType("pandas")
class DataFrame:
    @classmethod
    def from_records(cls, rows, columns):
        return {"rows": rows, "columns": columns}
pandas.DataFrame = DataFrame
sys.modules.setdefault("pandas", pandas)

# Minimal pyodbc substitute
pyodbc = types.ModuleType("pyodbc")
def connect(*args, **kwargs):
    return types.SimpleNamespace(cursor=lambda: None)
pyodbc.connect = connect
sys.modules.setdefault("pyodbc", pyodbc)

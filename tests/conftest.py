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
    def query(self, *_, **__):
        return types.SimpleNamespace(collect=lambda: [])

    def execute(self, *_, **__):
        pass


notebookutils.mssparkutils = types.SimpleNamespace(
    session=DummySession(), notebook=types.SimpleNamespace(exit=lambda x: None)
)
notebookutils.data = types.SimpleNamespace(connect_to_artifact=lambda *_, **__: None)
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

# Minimal azure.identity substitute
azure = types.ModuleType("azure")
identity = types.ModuleType("identity")


class DefaultAzureCredential:
    def get_token(self, _):
        return types.SimpleNamespace(token="token")


identity.DefaultAzureCredential = DefaultAzureCredential
azure.identity = identity
sys.modules.setdefault("azure", azure)
sys.modules.setdefault("azure.identity", identity)

# Minimal fabric_cicd substitute
fabric_cicd = types.ModuleType("fabric_cicd")


class FabricWorkspace:
    def __init__(self, **_):
        pass


def publish_all_items(_):
    pass


def unpublish_all_orphan_items(_):
    pass


class Constants:
    ACCEPTED_ITEM_TYPES_UPN = []


fabric_cicd.FabricWorkspace = FabricWorkspace
fabric_cicd.publish_all_items = publish_all_items
fabric_cicd.unpublish_all_orphan_items = unpublish_all_orphan_items
fabric_cicd.constants = Constants

sys.modules.setdefault("fabric_cicd", fabric_cicd)

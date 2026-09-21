"""Stand-ins for modules that are not importable on a plain developer machine.

Each stub is installed only when the real module is missing, so these tests can run
without Fabric while the python_libs_tests tree (which needs real pandas, pyodbc and
fabric_cicd) is unaffected when both trees run in one pytest session.
"""

import importlib.util
import sys
import types


def _missing(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is None


# Minimal notebookutils substitute (never installed outside Fabric)
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

# Minimal pandas substitute (only if pandas is unavailable)
if _missing("pandas"):
    pandas = types.ModuleType("pandas")

    class DataFrame:
        @classmethod
        def from_records(cls, rows, columns):
            return {"rows": rows, "columns": columns}

    pandas.DataFrame = DataFrame
    sys.modules.setdefault("pandas", pandas)

# Minimal pyodbc substitute (only if pyodbc is unavailable)
if _missing("pyodbc"):
    pyodbc = types.ModuleType("pyodbc")

    def connect(*args, **kwargs):
        return types.SimpleNamespace(cursor=lambda: None)

    pyodbc.connect = connect
    sys.modules.setdefault("pyodbc", pyodbc)

# Minimal azure.identity substitute (only if azure is unavailable)
try:
    import azure.identity  # type: ignore # noqa: F401
except Exception:
    azure = types.ModuleType("azure")
    identity = types.ModuleType("identity")

    class DefaultAzureCredential:
        def get_token(self, _):
            return types.SimpleNamespace(token="token")

    identity.DefaultAzureCredential = DefaultAzureCredential
    azure.identity = identity
    sys.modules.setdefault("azure", azure)
    sys.modules.setdefault("azure.identity", identity)

# Minimal fabric_cicd substitute (only if fabric_cicd is unavailable)
if _missing("fabric_cicd"):
    fabric_cicd = types.ModuleType("fabric_cicd")

    FEATURE_FLAG = set()

    class FabricWorkspace:
        def __init__(self, **_):
            pass

    def publish_all_items(*args, **kwargs):
        return []

    def unpublish_all_orphan_items(*args, **kwargs):
        return None

    def append_feature_flag(feature):
        FEATURE_FLAG.add(feature)

    class Constants:
        ACCEPTED_ITEM_TYPES_UPN = []

    fabric_cicd.FabricWorkspace = FabricWorkspace
    fabric_cicd.publish_all_items = publish_all_items
    fabric_cicd.unpublish_all_orphan_items = unpublish_all_orphan_items
    fabric_cicd.append_feature_flag = append_feature_flag
    fabric_cicd.constants = Constants
    fabric_cicd.constants.FEATURE_FLAG = FEATURE_FLAG

    sys.modules.setdefault("fabric_cicd", fabric_cicd)

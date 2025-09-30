import sys
import types
from typing import Any

# Minimal notebookutils substitute
notebookutils = types.ModuleType("notebookutils")


class DummySession:
    def query(self, *_: Any, **__: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(collect=lambda: [])

    def execute(self, *_: Any, **__: Any) -> None:
        pass


notebookutils.mssparkutils = types.SimpleNamespace(  # type: ignore[attr-defined]
    session=DummySession(), notebook=types.SimpleNamespace(exit=lambda x: None)
)
notebookutils.data = types.SimpleNamespace(connect_to_artifact=lambda *_, **__: None)  # type: ignore[attr-defined]
sys.modules.setdefault("notebookutils", notebookutils)

# Minimal pandas substitute
pandas = types.ModuleType("pandas")


class DataFrame:
    @classmethod
    def from_records(cls, rows: Any, columns: Any) -> dict[str, Any]:
        return {"rows": rows, "columns": columns}


pandas.DataFrame = DataFrame  # type: ignore[attr-defined]
sys.modules.setdefault("pandas", pandas)

# Minimal pyodbc substitute
pyodbc = types.ModuleType("pyodbc")


def connect(*args: Any, **kwargs: Any) -> types.SimpleNamespace:
    return types.SimpleNamespace(cursor=lambda: None)


pyodbc.connect = connect  # type: ignore[attr-defined]
sys.modules.setdefault("pyodbc", pyodbc)

# Minimal azure.identity substitute
azure = types.ModuleType("azure")
identity = types.ModuleType("identity")


class DefaultAzureCredential:
    def get_token(self, _: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(token="token")


identity.DefaultAzureCredential = DefaultAzureCredential  # type: ignore[attr-defined]
azure.identity = identity  # type: ignore[attr-defined]
sys.modules.setdefault("azure", azure)
sys.modules.setdefault("azure.identity", identity)

# Minimal fabric_cicd substitute
fabric_cicd = types.ModuleType("fabric_cicd")


class FabricWorkspace:
    def __init__(self, **_: Any) -> None:
        pass


def publish_all_items(_: Any) -> None:
    pass


def unpublish_all_orphan_items(_: Any) -> None:
    pass


class Constants:
    ACCEPTED_ITEM_TYPES_UPN: list[str] = []


fabric_cicd.FabricWorkspace = FabricWorkspace  # type: ignore[attr-defined]
fabric_cicd.publish_all_items = publish_all_items  # type: ignore[attr-defined]
fabric_cicd.unpublish_all_orphan_items = unpublish_all_orphan_items  # type: ignore[attr-defined]
fabric_cicd.constants = Constants  # type: ignore[attr-defined]

sys.modules.setdefault("fabric_cicd", fabric_cicd)

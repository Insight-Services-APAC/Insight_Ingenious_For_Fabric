"""
Azure CLI module for interacting with Azure Data Lake Storage and OneLake.

``OneLakeUtils`` is resolved lazily: ``onelake_utils`` imports ``fabric_api.utils``, which
imports ``az_cli.credentials``; an eager import here would close that loop whenever
``fabric_api.utils`` is imported first.
"""

from typing import Any

__all__ = ["OneLakeUtils"]


def __getattr__(name: str) -> Any:
    if name == "OneLakeUtils":
        from .onelake_utils import OneLakeUtils

        return OneLakeUtils
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

"""Logging utilities for the ingestion framework"""

import logging
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass
class ResourceContext:
    """Context for resource-level logging."""
    source_name: str
    resource_name: str


# Context variable - set by orchestrator, read by filter
resource_context: ContextVar[ResourceContext | None] = ContextVar('resource_context', default=None)


class ResourceContextFilter(logging.Filter):
    """Logging filter that adds [source_name] [resource_name] prefix from context."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = resource_context.get()
        if ctx:
            record.msg = f"[{ctx.source_name}] [{ctx.resource_name}] {record.msg}"
        return True


def _install_context_filter():
    """Install ResourceContextFilter on root handlers (idempotent).

    Note: Uses class name check instead of isinstance() because exec() loading
    in Fabric notebooks creates new class definitions each time, breaking isinstance().
    """
    for handler in logging.getLogger().handlers:
        # Check by class name to handle exec() reloading creating new class definitions
        if not any(type(f).__name__ == "ResourceContextFilter" for f in handler.filters):
            handler.addFilter(ResourceContextFilter())


_install_context_filter()

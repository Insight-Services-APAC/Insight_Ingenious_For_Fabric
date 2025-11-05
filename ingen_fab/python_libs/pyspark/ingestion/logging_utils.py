"""Logging utilities for the ingestion framework"""

import logging


class ConfigLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that prepends source name and config name to messages"""

    def process(self, msg, kwargs):
        source_name = self.extra.get("source_name", "")
        config_name = self.extra.get("config_name", "")

        prefix_parts = []
        if source_name:
            prefix_parts.append(source_name)
        if config_name:
            prefix_parts.append(config_name)

        if prefix_parts:
            prefix = "[" + "] [".join(prefix_parts) + "]"
            return f"{prefix} {msg}", kwargs
        return msg, kwargs

# Variable Resolver
# Resolves ${var_lib.VariableName} references from Microsoft Fabric Variable Library

import logging
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class VariableResolver:
    """
    Resolves variable references in the format ${var_lib.VariableName}.

    Uses Microsoft Fabric's Variable Library (notebookutils.variableLibrary)
    when running in Fabric environment, or returns literal values in local mode.
    """

    # Pattern to match ${var_lib.VariableName}
    VARIABLE_PATTERN = re.compile(r'\$\{var_lib\.([a-zA-Z_][a-zA-Z0-9_]*)\}')

    def __init__(self, spark_version: str = "fabric"):
        """
        Initialize the variable resolver.

        Args:
            spark_version: Environment type - "fabric" or "local"
        """
        self.spark_version = spark_version
        self._library_cache: Dict[str, Any] = {}
        self._library_loaded = False

    def _load_variable_library(self) -> Optional[Any]:
        """
        Load the Variable Library from Fabric (lazy loading).

        Returns:
            Variable Library object or None if not available
        """
        if self._library_loaded:
            return self._library_cache.get("var_lib")

        self._library_loaded = True

        if self.spark_version == "local":
            logger.debug("Variable Library not available in local mode")
            return None

        try:
            # Import notebookutils (only available in Fabric)
            from notebookutils import variableLibrary

            var_lib = variableLibrary.getLibrary("var_lib")
            self._library_cache["var_lib"] = var_lib
            logger.debug("Successfully loaded Variable Library 'var_lib'")
            return var_lib

        except ImportError:
            logger.warning("notebookutils not available - cannot load Variable Library")
            return None
        except Exception as e:
            logger.warning(f"Failed to load Variable Library 'var_lib': {e}")
            return None

    def resolve(self, value: Optional[str]) -> Optional[str]:
        """
        Resolve variable references in a string value.

        Replaces ${var_lib.VariableName} with the actual variable value from
        the Fabric Variable Library. Returns the original value if:
        - Value is None or not a string
        - No variable pattern is found
        - Running in local mode
        - Variable Library is not available
        - Variable is not found in library

        Args:
            value: String potentially containing variable references

        Returns:
            Resolved string value or original value
        """
        # Return as-is for None or non-string values
        if not value or not isinstance(value, str):
            return value

        # Check if value contains variable pattern
        match = self.VARIABLE_PATTERN.search(value)
        if not match:
            return value

        # In local mode, return original value
        if self.spark_version == "local":
            logger.debug(f"Cannot resolve variables in local mode: {value}")
            return value

        # Load Variable Library
        var_lib = self._load_variable_library()
        if var_lib is None:
            logger.warning(f"Variable Library not available, returning literal value: {value}")
            return value

        # Replace all variable references
        def replace_variable(match_obj):
            var_name = match_obj.group(1)
            try:
                # Access variable from library
                var_value = getattr(var_lib, var_name, None)
                if var_value is None:
                    logger.warning(f"Variable '{var_name}' not found in var_lib, keeping literal")
                    return match_obj.group(0)  # Return original ${var_lib.VarName}

                logger.debug(f"Resolved ${{{var_name}}} = {var_value}")
                return str(var_value)

            except Exception as e:
                logger.warning(f"Error accessing variable '{var_name}': {e}, keeping literal")
                return match_obj.group(0)

        resolved_value = self.VARIABLE_PATTERN.sub(replace_variable, value)

        if resolved_value != value:
            logger.debug(f"Resolved variable reference: {value} -> {resolved_value}")

        return resolved_value

    def resolve_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve variable references in all string values within a dictionary.

        Args:
            data: Dictionary potentially containing variable references

        Returns:
            Dictionary with resolved values
        """
        resolved = {}
        for key, value in data.items():
            if isinstance(value, str):
                resolved[key] = self.resolve(value)
            elif isinstance(value, dict):
                resolved[key] = self.resolve_dict(value)
            elif isinstance(value, list):
                resolved[key] = [
                    self.resolve(item) if isinstance(item, str) else item
                    for item in value
                ]
            else:
                resolved[key] = value

        return resolved

    def clear_cache(self):
        """Clear the cached Variable Library - useful for testing."""
        self._library_cache.clear()
        self._library_loaded = False

    def is_variable_reference(self, value: Optional[str]) -> bool:
        """
        Check if a string value contains a variable reference.

        Args:
            value: String to check

        Returns:
            True if value contains ${var_lib.VariableName} pattern
        """
        if not value or not isinstance(value, str):
            return False

        return bool(self.VARIABLE_PATTERN.search(value))

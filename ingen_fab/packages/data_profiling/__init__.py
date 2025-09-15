"""Data Profiling Package for Fabric Accelerator"""

from .compilation.configuration_builder import (
    ConfigurationBuilder,
    create_config,
    create_default_config,
)
from .data_profiling import DataProfilingCompiler, compile_data_profiling_package
from .compilation.ddl_manager import DDLScriptManager
from .compilation.modular_compiler import ModularDataProfilingCompiler
from .compilation.modular_compiler import compile_data_profiling_package as compile_modular
# ProfilingTemplateCompiler - check if it exists in compilation/
try:
    from .compilation.compiler import ProfilingTemplateCompiler
except ImportError:
    # Fallback for backward compatibility
    ProfilingTemplateCompiler = None

# Keep backward compatibility
__all__ = [
    "DataProfilingCompiler", 
    "compile_data_profiling_package",
    # New modular components
    "ModularDataProfilingCompiler",
    "compile_modular",
    "ConfigurationBuilder",
    "create_config",
    "create_default_config",
    "ProfilingTemplateCompiler",
    "DDLScriptManager",
]
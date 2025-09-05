"""Data Profiling Package for Fabric Accelerator"""

from .data_profiling import DataProfilingCompiler, compile_data_profiling_package
from .modular_compiler import ModularDataProfilingCompiler, compile_data_profiling_package as compile_modular
from .configuration_builder import ConfigurationBuilder, create_config, create_default_config
from .template_compiler import ProfilingTemplateCompiler
from .ddl_manager import DDLScriptManager

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
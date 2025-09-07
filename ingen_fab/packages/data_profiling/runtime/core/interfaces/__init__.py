"""Core interfaces for data profiling."""

from .profiling_interface import DataProfilingInterface
from .persistence_interface import PersistenceInterface

__all__ = [
    'DataProfilingInterface',
    'PersistenceInterface',
]
# Extractor Factory
# Creates the appropriate extractor based on source type

from typing import Any, Callable, Dict

from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.interface import (
    ConfigurationError,
    ResourceExtractorInterface,
)


class ExtractorFactory:
    """
    Factory for creating extractors based on source type.

    Uses a registry pattern for extensibility - new source types can be registered
    without modifying this factory.
    """

    # Registry mapping source_type -> extractor class
    _registry: Dict[str, Callable] = {}

    @classmethod
    def register(cls, source_type: str, extractor_class: Callable) -> None:
        """
        Register an extractor class for a source type.

        Args:
            source_type: The source type (e.g., 'filesystem', 'database')
            extractor_class: The extractor class constructor

        Example:
            ExtractorFactory.register('filesystem', FileSystemExtractor)
        """
        cls._registry[source_type] = extractor_class

    @classmethod
    def create(
        cls,
        resource_config: ResourceConfig,
        spark: Any,
        location_resolver: Any,
    ) -> ResourceExtractorInterface:
        """
        Create an extractor instance for the given resource configuration.

        Args:
            resource_config: Resource configuration
            spark: Spark session
            location_resolver: Location resolver for source/target resolution

        Returns:
            Extractor instance implementing ResourceExtractorInterface

        Raises:
            ConfigurationError: If source type is not registered
        """
        source_type = resource_config.source_config.source_type

        if source_type not in cls._registry:
            raise ConfigurationError(
                f"No extractor registered for source_type '{source_type}'. "
                f"Available types: {list(cls._registry.keys())}"
            )

        # Get the extractor class
        extractor_class = cls._registry[source_type]

        # Create and return the extractor instance
        # Each extractor decides what constructor args it needs
        return extractor_class(
            resource_config=resource_config,
            spark=spark,
            location_resolver=location_resolver,
        )

    @classmethod
    def get_registered_types(cls) -> list:
        """Get list of registered source types"""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, source_type: str) -> bool:
        """Check if a source type is registered"""
        return source_type in cls._registry


# ============================================================================
# AUTO-REGISTRATION OF EXTRACTORS
# ============================================================================
# Extractors will auto-register themselves when imported

# Import extractors here to register them
# (This will be populated as we implement each extractor)

try:
    from ingen_fab.python_libs.pyspark.ingestion.extractors.filesystem_extractor import (
        FileSystemExtractor,
    )
    ExtractorFactory.register('filesystem', FileSystemExtractor)
except ImportError:
    # FileSystemExtractor not yet implemented
    pass

try:
    from ingen_fab.python_libs.pyspark.ingestion.extractors.database_extractor import (
        DatabaseExtractor,
    )
    ExtractorFactory.register('database', DatabaseExtractor)
except ImportError:
    # DatabaseExtractor not yet implemented
    pass

try:
    from ingen_fab.python_libs.pyspark.ingestion.extractors.api_extractor import (
        APIExtractor,
    )
    ExtractorFactory.register('api', APIExtractor)
except ImportError:
    # APIExtractor not yet implemented
    pass

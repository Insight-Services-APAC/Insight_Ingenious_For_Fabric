"""Factory for creating Spark sessions with different providers.

This module provides a factory pattern for creating Spark sessions,
supporting both native Spark and Lakesail/PySail providers.
"""

from __future__ import annotations

import atexit
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """Factory for creating Spark sessions with different providers."""

    _lakesail_server = None  # Class variable to store server instance

    @staticmethod
    def create_local_spark_session(
        provider: str = "native",
        app_name: str = "LocalSparkApp",
        lakesail_port: int = 50051,
    ) -> SparkSession:
        """Create a local Spark session using the specified provider.

        Args:
            provider: The Spark provider to use ("native" or "lakesail")
            app_name: Name of the Spark application
            lakesail_port: Port for Lakesail server (only used if provider="lakesail")

        Returns:
            SparkSession: Configured Spark session

        Raises:
            ImportError: If lakesail provider is requested but pysail is not installed
            ValueError: If an unknown provider is specified
        """
        provider = provider.lower()

        # Check for environment variable override
        env_provider = os.getenv("LOCAL_SPARK_PROVIDER", "").lower()
        if env_provider in ["native", "lakesail"]:
            logger.info(f"Using LOCAL_SPARK_PROVIDER environment variable: {env_provider}")
            provider = env_provider

        if provider == "lakesail":
            return SparkSessionFactory._create_lakesail_session(app_name, lakesail_port)
        elif provider == "native":
            return SparkSessionFactory._create_native_session(app_name)
        else:
            raise ValueError(
                f"Unknown Spark provider: {provider}. Supported providers are 'native' and 'lakesail'."
            )

    @staticmethod
    def _create_native_session(app_name: str) -> SparkSession:
        """Create a native local Spark session with Delta support.

        Args:
            app_name: Name of the Spark application

        Returns:
            SparkSession: Native Spark session with Delta Lake support
        """
        logger.info("Creating native local Spark session with Delta support")

        # Check if there's already an active Spark session
        try:
            existing_spark = SparkSession.getActiveSession()
            if existing_spark is not None:
                logger.info("Found existing native Spark session, reusing it.")
                return existing_spark
        except Exception as e:
            logger.debug(f"No active Spark session found: {e}")

        # Create new Spark session with Delta support
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.defaultCatalog", "spark_catalog")           # ensure Delta-backed catalog is default
            .config("spark.sql.defaultTableFormat", "delta")               # CTAS/CREATE default to Delta
            .config("spark.sql.legacy.createHiveTableByDefault", "false")  # avoid Hive SerDe tables
     
        )

        # Import and configure Delta
        from delta import configure_spark_with_delta_pip

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Successfully created native Spark session with Delta support")
        return spark

    @staticmethod
    def _create_lakesail_session(app_name: str, port: int = 50051) -> SparkSession:
        """Create a Spark session connected to a Lakesail/PySail server.

        Args:
            app_name: Name of the Spark application
            port: Port for the Lakesail server

        Returns:
            SparkSession: Spark session connected to Lakesail server

        Raises:
            ImportError: If pysail is not installed
        """
        try:
            from pysail.spark import SparkConnectServer
        except ImportError as e:
            logger.error(
                "pysail is not installed. Install it with: pip install pysail"
            )
            raise ImportError(
                "pysail is required for lakesail provider. Install with: pip install pysail"
            ) from e

        # Check if there's already an active Spark session
        try:
            existing_spark = SparkSession.getActiveSession()
            if existing_spark is not None:
                # Check if it's already a remote session
                if hasattr(existing_spark, "_client"):
                    logger.info("Found existing Lakesail Spark session, reusing it.")
                    return existing_spark
                else:
                    logger.warning(
                        "Found existing native Spark session, stopping it to create Lakesail session"
                    )
                    existing_spark.stop()
        except Exception as e:
            logger.debug(f"No active Spark session found: {e}")

        # Start Lakesail server if not already running
        if SparkSessionFactory._lakesail_server is None:
            logger.info(f"Starting Lakesail server on port {port}")
            try:
                server = SparkConnectServer(port=port)
                server.start(background=True)
                SparkSessionFactory._lakesail_server = server

                # Register cleanup function
                atexit.register(SparkSessionFactory._cleanup_lakesail_server)

                # Get the actual listening address
                _, actual_port = server.listening_address
                logger.info(f"Lakesail server started successfully on port {actual_port}")
            except Exception as e:
                logger.error(f"Failed to start Lakesail server: {e}")
                raise

        # Create Spark session connected to Lakesail server
        logger.info(f"Creating Spark session connected to Lakesail server at localhost:{port}")
        spark = SparkSession.builder.appName(app_name).remote(f"sc://localhost:{port}").getOrCreate()

        # Test the connection
        try:
            spark.sql("SELECT 1").collect()
            logger.info("Successfully connected to Lakesail server")
        except Exception as e:
            logger.error(f"Failed to connect to Lakesail server: {e}")
            raise

        return spark

    @staticmethod
    def _cleanup_lakesail_server():
        """Clean up the Lakesail server on exit."""
        if SparkSessionFactory._lakesail_server is not None:
            try:
                logger.info("Stopping Lakesail server")
                SparkSessionFactory._lakesail_server.stop()
                SparkSessionFactory._lakesail_server = None
            except Exception as e:
                logger.warning(f"Error stopping Lakesail server: {e}")

    @staticmethod
    def get_or_create_spark_session(
        provider: Optional[str] = None,
        app_name: str = "LocalSparkApp",
        lakesail_port: int = 50051,
    ) -> SparkSession:
        """Get existing Spark session or create a new one.

        This method tries to reuse existing sessions when possible,
        and only creates new ones when necessary.

        Args:
            provider: The Spark provider to use (None = auto-detect from config)
            app_name: Name of the Spark application
            lakesail_port: Port for Lakesail server (only used if provider="lakesail")

        Returns:
            SparkSession: Active or newly created Spark session
        """
        # Auto-detect provider from config if not specified
        if provider is None:
            try:
                # Get  provider from Environment Variable
                provider = os.environ.get("LOCAL_SPARK_PROVIDER", "native").lower()
                print(f"Using Spark provider: {provider}")
            except Exception:
                provider = "native"

        print(f"Using Spark provider: {provider}")

        return SparkSessionFactory.create_local_spark_session(
            provider=provider,
            app_name=app_name,
            lakesail_port=lakesail_port,
        )
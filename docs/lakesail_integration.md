# Lakesail/PySail Integration Guide

This guide explains how to use Lakesail/PySail as an alternative Spark provider for local development in the Ingenious Fabric Accelerator.

## Overview

Lakesail's PySail is a drop-in replacement for Apache Spark that offers improved performance for single-process settings. It's built in Rust and provides compatibility with Spark SQL and DataFrame APIs while unifying batch, stream, and AI workloads.

## Installation

To use Lakesail, install it as an optional dependency:

```bash
# Using pip
pip install "insight-ingenious-for-fabric[lakesail]"

# Or install directly
pip install pysail>=0.3.3

# Using uv
uv pip install pysail
```

## Configuration

### Method 1: Configuration File

Set the `local_spark_provider` in your configuration:

```python
# In config_utils.py or your variable library
configs_dict = {
    "fabric_environment": "local",
    "local_spark_provider": "lakesail",  # Options: "native" (default) or "lakesail"
    # ... other configs
}
```

### Method 2: Environment Variable

Override the provider using an environment variable:

```bash
export LOCAL_SPARK_PROVIDER=lakesail
```

The environment variable takes precedence over the configuration file.

## Usage

Once configured, the abstraction libraries will automatically use Lakesail when creating Spark sessions in local environments:

```python
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# This will use Lakesail if configured
lh_utils = lakehouse_utils(
    target_workspace_id="your-workspace-id",
    target_lakehouse_id="your-lakehouse-id"
)

# Use as normal
df = lh_utils.spark.sql("SELECT * FROM your_table")
df.show()
```

## Direct Usage

You can also directly use the SparkSessionFactory:

```python
from ingen_fab.python_libs.common.spark_session_factory import SparkSessionFactory

# Create a Lakesail session
spark = SparkSessionFactory.create_local_spark_session(
    provider="lakesail",
    app_name="MyApp",
    lakesail_port=50051  # Optional: specify port
)

# Use the Spark session
df = spark.sql("SELECT 1 as test")
df.show()

# Clean up
spark.stop()
SparkSessionFactory._cleanup_lakesail_server()
```

## How It Works

1. **Server Management**: When using Lakesail, a PySail server is automatically started in the background on the specified port (default: 50051).

2. **Session Creation**: A Spark session is created that connects to the PySail server using the Spark Connect protocol.

3. **Automatic Cleanup**: The server is automatically stopped when the Python process exits.

4. **Session Reuse**: Existing sessions are reused when possible to avoid unnecessary overhead.

## Comparison: Native vs Lakesail

| Feature | Native Spark | Lakesail |
|---------|-------------|----------|
| Performance (single-process) | Good | Excellent |
| Memory Usage | Higher | Lower |
| Startup Time | Slower | Faster |
| Compatibility | 100% Spark | Spark SQL & DataFrame API |
| Delta Lake Support | Yes | Yes (via Spark compatibility) |
| Best For | Production, full Spark features | Local development, testing |

## Troubleshooting

### Import Error

If you see `ImportError: pysail is required for lakesail provider`, install pysail:

```bash
pip install pysail
```

### Port Already in Use

If the default port (50051) is in use, specify a different port:

```python
spark = SparkSessionFactory.create_local_spark_session(
    provider="lakesail",
    lakesail_port=50052
)
```

### Server Not Stopping

If the Lakesail server doesn't stop properly, you can manually clean it up:

```python
from ingen_fab.python_libs.common.spark_session_factory import SparkSessionFactory
SparkSessionFactory._cleanup_lakesail_server()
```

## Testing

Run the integration test to verify everything is working:

```bash
python test_lakesail_integration.py
```

This will test both native and Lakesail providers.

## Performance Tips

1. **Use for Local Development**: Lakesail excels in single-process scenarios, making it ideal for local development and testing.

2. **Batch Operations**: Group operations together to minimize round-trips to the server.

3. **Data Size**: Lakesail performs best with small to medium datasets typical in development environments.

## Limitations

- Lakesail implements a subset of Spark's functionality focused on SQL and DataFrame operations
- Some advanced Spark features may not be available
- Not recommended for production Fabric deployments (use native Spark in Fabric)

## Future Enhancements

- Support for additional Lakesail configuration options
- Performance benchmarking tools
- Integration with Fabric's local development tools
- Support for Lakesail's streaming capabilities
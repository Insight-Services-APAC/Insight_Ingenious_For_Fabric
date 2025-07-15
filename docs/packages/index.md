# Packages

Welcome to the Packages documentation! This section provides detailed information about available packages and how to use them in your Microsoft Fabric projects.

## What are Packages?

Packages are reusable workload extensions that provide specialized functionality for common data processing scenarios. They include pre-built templates, configurations, and processing logic for specific use cases.

## Available Packages

### [Flat File Ingestion](flat_file_ingestion.md)

A comprehensive package for processing various file formats (CSV, JSON, Parquet, Avro, XML) and loading them into Delta tables based on metadata configuration.

**Key Features:**
- Multi-format file support
- Configurable parsing options
- Data validation and error handling
- Comprehensive execution logging
- Merge, append, and overwrite strategies

**Quick Start:**
```bash
# Compile the package
ingen_fab run package flat-file-ingestion compile

# Run ingestion
ingen_fab run package flat-file-ingestion run --config-id=my-config
```

## Package Development

Learn how to create your own packages:

- [Package Architecture](../developer_guide/packages.md) - Understanding package structure
- [Development Guide](../developer_guide/packages.md#creating-a-new-package) - Step-by-step package creation
- [Best Practices](../developer_guide/packages.md#best-practices) - Package development guidelines

## Getting Started

1. **Choose a Package** - Select from available packages
2. **Compile Templates** - Generate notebooks and DDL scripts
3. **Configure Metadata** - Set up configuration tables
4. **Execute Workloads** - Run your data processing jobs

## Package Structure

All packages follow a consistent structure:

```
packages/package_name/
├── __init__.py                 # Package initialization
├── package_name.py             # Main package module
├── templates/                  # Jinja2 templates
│   ├── notebook_template.py.jinja
│   └── config_template.json.jinja
├── ddl_scripts/               # DDL script templates
│   ├── lakehouse_config.py
│   ├── lakehouse_log.py
│   ├── warehouse_config.sql
│   └── warehouse_log.sql
└── README.md                  # Package documentation
```

## CLI Integration

Packages integrate seamlessly with the CLI:

```bash
# General command structure
ingen_fab run package <package-name> <command> [options]

# Common commands
ingen_fab run package <package-name> compile    # Compile templates
ingen_fab run package <package-name> run       # Execute workload
```

## Next Steps

- **Use a Package**: Start with [Flat File Ingestion](flat_file_ingestion.md)
- **Create a Package**: Read the [Development Guide](../developer_guide/packages.md)
- **Contribute**: See [Contributing Guidelines](../developer_guide/index.md#contributing)

Ready to accelerate your data processing with packages? Choose a package from the list above or create your own!
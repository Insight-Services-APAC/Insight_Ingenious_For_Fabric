# Compilation Code

This directory contains code that runs ONLY during the compilation phase when generating notebooks for Microsoft Fabric.

## Structure

- **compiler.py** - Main notebook compiler orchestrator
- **ddl_manager.py** - DDL script generation and management
- **configuration_builder.py** - Build configuration from manifest files
- **modular_compiler.py** - Modular compilation logic
- **validators/** - Template and configuration validators
- **utils/** - Compilation utilities (path resolution, etc.)

## Usage

This code is executed when running:

```bash
ingen_fab package data_profiling compile
```

## Important Notes

- This code NEVER gets deployed to Fabric
- Runs only locally during compilation
- Handles template processing, DDL generation, configuration building
- Uses Jinja2 templates from the `templates/` directory
- Generates notebooks that import from the `runtime/` package
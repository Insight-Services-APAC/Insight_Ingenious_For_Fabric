# Fabric Workspace Items

This directory contains template Fabric artifacts that will be deployed to Microsoft Fabric workspaces.

## Structure

- **`config/`** - Variable library template with environment-specific configurations
- **`extract/`** - Data extraction notebook templates
- **`load/`** - Data loading notebook templates
- **`lakehouses/`** - Lakehouse definition templates
- **`warehouses/`** - Warehouse definition templates

## Usage

These templates are used when initializing new projects with:

```bash
ingen_fab init solution --project-name "My Project"
```

The templates will be copied to your project and customized with your project-specific settings.

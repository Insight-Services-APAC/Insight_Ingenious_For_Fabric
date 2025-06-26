# {project_name}

Microsoft Fabric workspace project.

## Structure

- `ddl_scripts/` - DDL scripts for creating tables and loading configuration data
- `fabric_workspace_items/` - Fabric artifacts (notebooks, pipelines, etc.)
- `var_lib/` - Environment-specific configuration
- `diagrams/` - Architecture diagrams

## Setup

1. Update variable files in `var_lib/` with your workspace and lakehouse IDs
2. Create DDL scripts in `ddl_scripts/`
3. Generate DDL notebooks: `ingen_fab compile-ddl-notebooks --output-mode fabric --generation-mode warehouse`
4. Deploy: `ingen_fab deploy-to-environment --fabric-workspace-repo-dir . --fabric-environment development`

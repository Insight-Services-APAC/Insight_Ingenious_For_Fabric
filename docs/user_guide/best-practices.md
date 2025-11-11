# Best Practices

[Home](../index.md) > [User Guide](index.md) > Best Practices

This guide covers best practices for using the Ingenious Fabric Accelerator effectively in your projects.

## dbt Best Practices

### Model Organization

- Use layers: `staging/`, `marts/`, `metrics/`
- Follow naming conventions: `stg_`, `fct_`, `dim_`
- Document models with descriptions and tests

### Testing Strategy

- Add tests in schema.yml files
- Use `relationships` tests for referential integrity
- Implement custom data quality tests

### Incremental Models

- Use incremental materialization for large tables
- Define appropriate `unique_key` and `incremental_strategy`
- Test incremental logic thoroughly

### Environment Configuration

- Use separate profiles for each environment
- Configure different target lakehouses per environment
- Manage secrets securely (Azure Key Vault)

## Code Organization

- **Separate concerns**: Keep DDL scripts focused on single responsibilities
- **Version control**: Use descriptive commit messages and branch strategies
- **Documentation**: Include README files in each major directory

## Development Practices

- **Deploy and validate**: Test changes directly in Fabric development workspace
- **Use version control**: Commit changes frequently with meaningful messages
- **Environment parity**: Keep environments as similar as possible
- **Leverage dbt**: Use dbt for analytics transformations and data modeling

## Deployment Practices

- **Gradual rollout**: Deploy to development first, then test, then production
- **Backup strategy**: Ensure you can rollback changes if needed
- **Monitoring**: Monitor execution logs in Fabric after deployment

## Security Practices

- **Secret management**: Use Azure Key Vault or environment variables
- **Access control**: Implement proper RBAC in Fabric workspaces
- **Audit logging**: Enable audit logs for all environments

## Performance Optimization

### Notebook Generation

```bash
# Generate notebooks for multiple types sequentially
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Generate dbt notebooks
ingen_fab dbt create-notebooks --dbt-project analytics_dbt
```

### Deployment

```bash
# Clean up old items before deployment if needed
ingen_fab deploy delete-all --environment development --force

# Deploy fresh
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development
```

## Related Topics

- [Workflows](workflows.md) - Development workflows and common tasks
- [DDL Script Organization](ddl-organization.md) - Organizing DDL scripts
- [DBT Integration](dbt_integration.md) - Working with dbt
- [Deploy Guide](deploy_guide.md) - Deployment strategies

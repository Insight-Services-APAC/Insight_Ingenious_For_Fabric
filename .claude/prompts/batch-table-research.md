# Batch Table Research Prompt

Research and update documentation for the following tables systematically. For each table:

## Process:
1. Read current metadata: `/workspaces/i4f/database-docs/tracking/tables/{table}/metadata.yml`
2. Research official Microsoft documentation using web search
3. Update metadata with findings
4. Move to next table

## Tables to Research:
{TABLES_LIST}

## Research Focus:
- **Dataverse entity reference**: Base entity documentation
- **D365 module documentation**: Sales, Service, Field Service, Marketing
- **F&O integration**: Dual-write, data entities if applicable
- **Cross-module usage**: Which D365 apps use this entity

## Update Format:
```yaml
documentation:
  d365_doc_url: 'primary_d365_module_url'
  dataverse_doc_url: 'dataverse_entity_reference_url'
  fo_module_doc_url: 'finance_operations_url_if_applicable'
  docs_verified: verified
  verification_notes: |
    - Research findings and URL verification details
    - Cross-module usage notes
    - Additional resources found
  last_verified: '{current_date}'
```

Please process each table systematically and provide a summary of findings.
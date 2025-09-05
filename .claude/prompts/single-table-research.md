# Single Table Research Prompt

Research documentation for table: **{TABLE_NAME}**

## Current Status:
- Metadata file: `/workspaces/i4f/database-docs/tracking/tables/{TABLE_NAME}/metadata.yml`

## Research Tasks:
1. **Read current metadata** to understand the table's module classification
2. **Web search** for official Microsoft documentation:
   - Primary Dataverse entity reference
   - D365 module-specific docs (Sales, Service, Marketing, Field Service)
   - F&O integration docs if applicable
3. **Verify URLs** and check for correct Microsoft Learn patterns
4. **Update metadata** with comprehensive findings

## Search Strategy:
Based on the table's `module_guess` and `prefix_family`, search for:
- `"Microsoft Dataverse {table_name} entity reference site:learn.microsoft.com"`
- `"Dynamics 365 {relevant_module} {table_name} entity documentation site:learn.microsoft.com"`
- Integration and dual-write documentation if F&O related

## Update Requirements:
- Correct URL formats (include `/en-us/` paths)
- Comprehensive verification notes
- Cross-module usage documentation
- Additional relevant resources
- Current date verification

Please research this table thoroughly and update its metadata with verified documentation URLs.
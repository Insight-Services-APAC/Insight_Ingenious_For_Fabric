# Claude Prompts Library

This folder contains reusable prompts and templates for consistent analysis and documentation of the Fabric Data Warehouse.

## Available Prompts

### 📊 Database Table Analysis

#### `comprehensive_table_analysis.md`
**Purpose**: Complete systematic analysis and documentation of any database table  
**Usage**: Replace `{table_name}` and `{schema_name}` variables, execute all SQL queries  
**Outputs**: Comprehensive markdown documentation with business context, data quality analysis, and practical examples

**Key Features:**
- 6-step analysis process with specific SQL queries
- Column categorization (Core/System/Relationship/Custom)
- Data quality assessment with completeness metrics
- Business context using Dynamics 365 knowledge
- Sample data analysis and pattern identification
- Relationship mapping and integration points
- Performance considerations and query examples

#### `table_analysis_quick_start.md`
**Purpose**: Quick reference guide for table analysis  
**Usage**: Essential SQL queries and template structure for rapid analysis  
**Outputs**: Streamlined analysis following the same quality standards

**Key Features:**
- Essential SQL queries only
- Column categorization guide
- D365 pattern recognition
- Quality indicator guidelines

## Usage Examples

### Analyze a Customer Table
```bash
# Use comprehensive_table_analysis.md
# Replace: {table_name} = 'account', {schema_name} = 'dbo'
# Execute all SQL queries and follow documentation template
```

### Quick Table Assessment
```bash
# Use table_analysis_quick_start.md
# Focus on essential queries for rapid insights
```

## Template Standards

All prompts follow these standards:
- **Show all SQL queries**: Display every query executed to the user
- **Use actual data**: Include real sample data with business context  
- **Provide completeness metrics**: Calculate data quality percentages
- **Include D365 context**: Leverage Dynamics 365 knowledge for business descriptions
- **Visual indicators**: Use ✅, ⚠️, ❌ for quality assessments
- **Consistent formatting**: Markdown tables, code blocks, structured output

## Quality Checklist

Before completing any analysis:
- [ ] All SQL queries executed and documented
- [ ] Sample data analyzed for business patterns
- [ ] Data quality metrics calculated
- [ ] Business context and D365 integration explained
- [ ] Practical examples provided
- [ ] Recommendations included

## Integration with Tools

These prompts work with:
- **FabricWarehouseQueryTool**: SQL execution and data retrieval
- **MkDocs**: Documentation output format
- **Database-docs**: Structured documentation repository

## Future Enhancements

Planned additions:
- Schema-level analysis prompts
- Cross-table relationship mapping
- Data lineage documentation
- Performance optimization guides
- Data governance templates
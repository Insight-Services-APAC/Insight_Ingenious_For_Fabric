---
name: "Comprehensive Table Analysis"
description: "Systematic analysis and documentation of any database table with SQL execution logging"
version: "1.0"
author: "Claude Code Database Documentation System"
created: "2025-08-22"
updated: "2025-08-22"

# Input Parameters
parameters:
  table_name:
    type: string
    required: true
    description: "Name of the table to analyze (e.g., 'account', 'contact', 'custtable')"
    example: "account"
  
  schema_name:
    type: string
    required: false
    default: "dbo"
    description: "Database schema name"
    example: "dbo"
  
  analysis_depth:
    type: string
    required: false
    default: "comprehensive"
    options: ["basic", "standard", "comprehensive"]
    description: "Level of analysis detail"
  
  sample_size:
    type: integer
    required: false
    default: 1000
    description: "Number of records to sample for analysis"
    min: 10
    max: 10000

# Usage Instructions
usage: |
  1. Set parameters: table_name="your_table" schema_name="dbo"
  2. Execute all SQL queries in sequence. 
  3. Show each query and its results to the user
  4. Follow the documentation template structure
  5. Provide business context using Dynamics 365 knowledge

# Expected Outputs
outputs:
  - comprehensive_documentation: "Complete markdown documentation file"
  - sql_execution_log: "All queries executed with results"
  - data_quality_report: "Completeness metrics and recommendations" 
  - business_context: "D365 integration and usage patterns"
  - sample_data: "Representative data samples with patterns"

# Quality Standards
quality_requirements:
  - show_all_queries: "Display every SQL query before execution"
  - use_actual_data: "Include real sample data with business context"
  - calculate_metrics: "Provide completeness percentages and quality scores"
  - d365_context: "Apply Dynamics 365 knowledge for business descriptions"
  - visual_indicators: "Use ✅ ⚠️ ❌ for data quality assessments"
---

# Comprehensive Table Analysis and Documentation

## Purpose
This prompt provides a systematic approach to analyze and document any single database table from the Fabric Data Warehouse, ensuring consistent, thorough documentation across all tables.

**Target Table**: `{schema_name}.{table_name}`  
**Analysis Depth**: {analysis_depth}  
**Sample Size**: {sample_size} records

## Usage Instructions
1. **Replace variables**: Substitute `{table_name}` and `{schema_name}` with actual values
2. **Execute all SQL queries**: Show each query and its results to the user
3. **Follow the structure**: Use the exact documentation template provided
4. **Analyze sample data**: Always get 1000+ records for pattern analysis
5. **Provide business context**: Use Dynamics 365 knowledge for context

## Step-by-Step Analysis Process

### Step 0: Setup
- Load the virtual environment and set environment variables (see claude.md)
- Import FabricWarehouseQueryTool from fabric_sql_query_tool.py in order to execute queries.
- Assume you are already logged in via az login 
- Create a temporary working folder in ./tmp/scripts/{schema}/{table} to house any scripts or temporary files you need to create

### Step 1: Basic Table Information
Execute and document these queries:

```sql
-- Get complete table structure
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
ORDER BY ORDINAL_POSITION;

-- Get total row count
SELECT COUNT(*) as [row_count] FROM [{schema_name}].[{table_name}];

-- Get table size (if sys tables accessible)
SELECT 
    SUM(a.total_pages) * 8 / 1024 as [size_mb],
    SUM(a.used_pages) * 8 / 1024 as [used_mb]
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.name = '{table_name}';
```

**Analysis Tasks:**
- Categorize columns into: Core Business, System Fields, Relationships, Custom/Extended
- Count columns in each category
- Identify primary key and main business fields

### Step 2: Sample Data Analysis
```sql
-- Get recent representative sample
SELECT TOP 1000 * FROM [{schema_name}].[{table_name}]
ORDER BY SinkCreatedOn DESC; -- or other timestamp field

-- Get random sample for pattern analysis
SELECT TOP 100 * FROM [{schema_name}].[{table_name}]
ORDER BY NEWID();

-- Get specific key fields sample
SELECT TOP 10 
    Id, SinkCreatedOn, SinkModifiedOn, statecode, statuscode,
    [key_field_1], [key_field_2], [key_field_3]
FROM [{schema_name}].[{table_name}]
ORDER BY SinkCreatedOn DESC;
```

**Analysis Tasks:**
- Extract sample records showing business patterns
- Identify data formats and conventions
- Note any business-specific patterns (D365, etc.)

### Step 3: Data Quality Assessment
```sql
-- Analyze completeness for key fields
SELECT 
    COUNT(*) as total_rows,
    COUNT([field1]) as field1_non_null,
    COUNT([field2]) as field2_non_null,
    COUNT([field3]) as field3_non_null,
    CAST((COUNT([field1]) * 100.0 / COUNT(*)) as DECIMAL(5,2)) as field1_completeness_pct,
    CAST((COUNT([field2]) * 100.0 / COUNT(*)) as DECIMAL(5,2)) as field2_completeness_pct
FROM [{schema_name}].[{table_name}];

-- Check for duplicates (if ID exists)
SELECT Id, COUNT(*) as duplicate_count
FROM [{schema_name}].[{table_name}]
GROUP BY Id
HAVING COUNT(*) > 1;

-- Analyze text field characteristics
SELECT 
    MIN(LEN([text_field])) as min_length,
    MAX(LEN([text_field])) as max_length,
    AVG(LEN([text_field])) as avg_length
FROM [{schema_name}].[{table_name}]
WHERE [text_field] IS NOT NULL;
```

**Analysis Tasks:**
- Calculate completeness percentages for key fields
- Identify data quality issues
- Check for duplicates and data consistency

### Step 4: Data Distribution Analysis
```sql
-- Analyze state/status patterns (D365 standard)
SELECT 
    statecode, statuscode, 
    COUNT(*) as record_count,
    CAST((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [{schema_name}].[{table_name}])) as DECIMAL(5,2)) as percentage
FROM [{schema_name}].[{table_name}]
GROUP BY statecode, statuscode
ORDER BY record_count DESC;

-- Analyze categorical field distributions
SELECT 
    [categorical_field],
    COUNT(*) as count,
    CAST((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [{schema_name}].[{table_name}])) as DECIMAL(5,2)) as percentage
FROM [{schema_name}].[{table_name}]
GROUP BY [categorical_field]
ORDER BY count DESC;
```

### Step 5: Data Freshness Analysis
```sql
-- Analyze loading patterns
SELECT 
    MIN(SinkCreatedOn) as earliest_record,
    MAX(SinkCreatedOn) as latest_record,
    COUNT(DISTINCT CAST(SinkCreatedOn as DATE)) as distinct_loading_days,
    COUNT(DISTINCT CAST(SinkModifiedOn as DATE)) as distinct_modification_days
FROM [{schema_name}].[{table_name}]
WHERE SinkCreatedOn IS NOT NULL;

-- Recent activity analysis
SELECT 
    CAST(SinkCreatedOn as DATE) as load_date,
    COUNT(*) as records_loaded
FROM [{schema_name}].[{table_name}]
WHERE SinkCreatedOn >= DATEADD(day, -30, GETDATE())
GROUP BY CAST(SinkCreatedOn as DATE)
ORDER BY load_date DESC;
```

### Step 6: Relationship Analysis
```sql
-- Identify potential relationship fields
-- Look for fields ending in 'id', 'accountid', 'contactid', etc.

-- Sample relationship data
SELECT TOP 5 Id, [relationship_field1], [relationship_field2]
FROM [{schema_name}].[{table_name}]
WHERE [relationship_field1] IS NOT NULL;
```

**Analysis Tasks:**
- Identify foreign key patterns
- Find lookup and reference fields
- Map relationships to other known tables

## Documentation Template Structure

### Header Section
```markdown
# {table_name} Table

**{Dynamics 365 Context} - {Business Purpose}**

## Table Overview

| Property | Value |
|----------|-------|
| **Table Name** | `{table_name}` |
| **Schema** | `{schema_name}` |
| **Business Purpose** | {determined from analysis} |
| **System Source** | {D365 CE/F&O/Power Platform/CDM} |
| **Total Columns** | {column_count} |
| **Total Records** | {row_count:,} |
| **Primary Key** | `{primary_key}` |
| **Last Updated** | {latest_timestamp} |
```

### Business Context Section
```markdown
## Business Context

{Detailed explanation of business purpose and D365 integration}

### Dynamics 365 Entity Classification
- **Entity Type**: {Standard/Custom Dataverse entity}
- **Business Area**: {CRM/ERP/Power Platform}
- **Process Integration**: {Sales/Marketing/Service/Finance}
- **Ownership Model**: {User/Team owned}
```

### Column Analysis Section
```markdown
## Column Analysis

### Core Business Fields ({count} columns)
| Column | Data Type | Description | Sample Data |
|--------|-----------|-------------|-------------|
{table of core fields with samples}

### System Fields ({count} columns)
| Column | Data Type | Description | Business Purpose |
|--------|-----------|-------------|------------------|
{D365 system fields}

### Relationship Fields ({count} columns)
{List key relationship fields with purposes}

### Custom/Extended Fields ({count} columns)
{Summary of customization areas}
```

### Data Quality Section
```markdown
## Data Quality Analysis

### Completeness Assessment
| Field | Complete Records | Completeness % | Data Quality |
|-------|------------------|----------------|--------------|
{completeness table with visual indicators}

### State/Status Distribution
{distribution analysis}

### Data Characteristics
{patterns, lengths, formats, business insights}
```

### SQL Documentation Section
```markdown
## SQL Queries Executed

### Column Structure Analysis
```sql
{actual query executed}
```

### Data Volume and Distribution
```sql
{actual queries with results}
```

### Sample Records
```json
{actual sample data in JSON format}
```

### Business Process Integration Section
```markdown
## Business Process Integration

### Primary Use Cases
1. {use case 1}
2. {use case 2}

### Integration Points
- **{related_entity}**: {relationship description}

### Common Business Scenarios
{real-world usage patterns}
```

### Technical Specifications Section
```markdown
## Performance Characteristics
{size, growth, optimization notes}

## Sample Query Patterns
{practical SQL examples}

## Data Quality Recommendations
{actionable improvements}
```

### Footer Metadata
```markdown
---

**Analysis Date**: {current_date}  
**SQL Queries Executed**: {count} comprehensive queries  
**Sample Size Analyzed**: {sample_size} records  
**Data Quality Score**: {score}/10 ({rationale})  
**Confidence Level**: {High/Medium/Low} ({reason})
```

## Quality Checklist

Before completing documentation:

- [ ] All SQL queries executed and results documented
- [ ] Column categorization completed (Core/System/Relationship/Custom)
- [ ] Sample data analyzed for business patterns
- [ ] Data quality metrics calculated and interpreted
- [ ] Business context and D365 integration explained
- [ ] Relationship mappings identified
- [ ] Performance considerations noted
- [ ] Practical query examples provided
- [ ] Data quality recommendations included
- [ ] Completeness percentages calculated
- [ ] Sample records formatted as JSON
- [ ] All {variables} replaced with actual values
- [ ] Clean up any temporary files that you created

## Expected Outcomes

The completed analysis should provide:

1. **Technical Reference**: Complete column definitions and data types
2. **Business Context**: Clear explanation of purpose and D365 integration
3. **Data Quality Assessment**: Actionable insights on data completeness and issues
4. **Usage Examples**: Practical SQL queries for common scenarios
5. **Integration Guide**: Relationships and dependencies with other tables
6. **Performance Insights**: Size, growth patterns, and optimization notes

## Notes for Consistent Application

- **Always show SQL queries**: Display every query executed to the user
- **Use actual sample data**: Include real data samples with business context
- **Maintain D365 context**: Leverage Dynamics 365 knowledge for accurate business descriptions
- **Provide completeness metrics**: Calculate and display data quality percentages
- **Include visual indicators**: Use ✅, ⚠️, ❌ for data quality assessments
- **Format consistently**: Use markdown tables, code blocks, and info boxes appropriately

## Output Location, Format and Documentation Framework
- Output is to be in mardown format and stored in ./database-docs
- Output should adhere to the mkdocs framework. The relevant mkdocs file is database-docs/mkdocs.yml

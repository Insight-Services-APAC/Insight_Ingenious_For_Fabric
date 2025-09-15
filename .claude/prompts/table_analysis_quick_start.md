# Table Analysis Quick Start Guide

## Quick Usage
```bash
# Use this prompt to analyze any table:
# 1. Replace {table_name} and {schema_name} with actual values
# 2. Execute all SQL queries in order
# 3. Follow the documentation template structure
# 4. Show all queries and results to the user
```

## Essential SQL Queries

### 1. Basic Structure (ALWAYS START HERE)
```sql
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION,
       CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
ORDER BY ORDINAL_POSITION;

SELECT COUNT(*) as [row_count] FROM [{schema_name}].[{table_name}];
```

### 2. Sample Data (GET BUSINESS CONTEXT)
```sql
SELECT TOP 10 * FROM [{schema_name}].[{table_name}]
ORDER BY SinkCreatedOn DESC;
```

### 3. Data Quality (KEY INSIGHTS)
```sql
-- Customize field list based on table structure
SELECT 
    COUNT(*) as total_rows,
    COUNT([key_field1]) as field1_complete,
    COUNT([key_field2]) as field2_complete,
    CAST((COUNT([key_field1]) * 100.0 / COUNT(*)) as DECIMAL(5,2)) as field1_pct
FROM [{schema_name}].[{table_name}];
```

### 4. State Distribution (D365 TABLES)
```sql
SELECT statecode, statuscode, COUNT(*) as count,
       CAST((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [{schema_name}].[{table_name}])) as DECIMAL(5,2)) as pct
FROM [{schema_name}].[{table_name}]
GROUP BY statecode, statuscode;
```

## Documentation Template Order

1. **Table Overview** - Basic stats and purpose
2. **Business Context** - D365 integration and purpose  
3. **Column Analysis** - Categorized field breakdown
4. **Data Quality Analysis** - Completeness and patterns
5. **SQL Queries Executed** - All queries with results
6. **Sample Records** - JSON formatted examples
7. **Business Process Integration** - Use cases and relationships
8. **Technical Specifications** - Performance and queries
9. **Recommendations** - Actionable improvements

## Column Categorization

- **Core Business**: Primary identifiers, names, key business attributes
- **System Fields**: statecode, statuscode, Sink*, createdon, modifiedon, ownerid
- **Relationship Fields**: Fields ending in 'id', lookups, foreign keys
- **Custom/Extended**: Everything else, customizations, localizations

## Common D365 Patterns

- **Customer Engagement**: account, contact, lead, opportunity, incident
- **Finance & Operations**: custtable, vendtable, inventtable, ledger*
- **Power Platform**: canvas*, flow*, bot*, aiplugin*
- **CDM Entities**: cdm_* prefix for Common Data Model

## Quality Indicators

- ✅ **Excellent**: >95% completeness
- ⚠️ **Moderate**: 50-95% completeness  
- ❌ **Poor**: <50% completeness or not used

## Remember to Always:

1. Show every SQL query to the user
2. Include actual sample data with business context
3. Calculate and display completeness percentages
4. Provide D365 business context
5. Include practical query examples
6. Give actionable recommendations
# Synapse Sync Package

[Home](../index.md) > [Packages](index.md) > Synapse Sync

## At a Glance

| Item | Summary |
|------|---------|
| Purpose | Synchronize data between Fabric (lakehouse/warehouse) and Azure Synapse. |
| Inputs | Sync object configs (tables/views), connection details, optional transforms. |
| Outputs | Target objects updated; run history in log tables. |
| Core commands | `ingen_fab package synapse compile`, `ingen_fab package synapse run`. |
| When to use | You need one-way or bidirectional sync with incremental patterns and monitoring. |

Note: For detailed CLI flags, see User Guide → CLI Reference. Use Deploy Guide for environment setup.

The Synapse Sync package enables bidirectional data synchronization between Microsoft Fabric and Azure Synapse Analytics workspaces. It provides configurable sync patterns, incremental updates, and comprehensive monitoring.

## Overview

The Synapse Sync package allows you to:
- Synchronize data between Fabric and Synapse workspaces
- Configure table-level sync patterns and schedules
- Handle incremental updates with change tracking
- Map schemas and data types between platforms
- Monitor sync operations and performance
- Handle conflicts and data reconciliation

## Installation & Setup

### 1. Compile the Package

```bash
# Compile synapse sync package with sample configurations
ingen_fab package synapse compile --include-samples

# Compile for specific target
ingen_fab package synapse compile --target-datastore warehouse
```

### 2. Deploy DDL Scripts

The package creates configuration tables in both lakehouse and warehouse:

**Lakehouse Tables:**
- `config_synapse_extract_objects` - Sync object definitions
- `log_synapse_extract_run_log` - Execution history

**Warehouse Tables:**
- `config.config_synapse_extract_objects` - Sync object definitions
- `config.log_synapse_extract_run_log` - Execution history

### 3. Configure Connection

Set up Synapse workspace connection details in your environment configuration.

## Configuration

### Sync Object Configuration

Define objects to sync in `config_synapse_extract_objects`:

```python
# For lakehouse (Python)
sync_config = {
    "object_id": "CUSTOMER_SYNC",
    "object_name": "Customer Data Sync",
    "active_yn": "Y",
    "sync_direction": "FABRIC_TO_SYNAPSE",
    "source_type": "TABLE",
    "source_schema": "dbo",
    "source_object": "customers",
    "target_schema": "staging",
    "target_object": "stg_customers",
    "sync_type": "INCREMENTAL",
    "sync_column": "modified_date",
    "sync_frequency": "HOURLY",
    "filter_condition": "active_flag = 1",
    "column_mappings": {
        "customer_id": "customer_id",
        "customer_name": "customer_name",
        "email": "email_address"
    }
}
```

```sql
-- For warehouse (SQL)
INSERT INTO config.config_synapse_extract_objects (
    object_id,
    object_name,
    active_yn,
    sync_direction,
    source_type,
    source_schema,
    source_object,
    target_schema,
    target_object,
    sync_type,
    sync_column,
    sync_frequency,
    filter_condition,
    column_mappings
) VALUES (
    'CUSTOMER_SYNC',
    'Customer Data Sync',
    'Y',
    'FABRIC_TO_SYNAPSE',
    'TABLE',
    'dbo',
    'customers',
    'staging',
    'stg_customers',
    'INCREMENTAL',
    'modified_date',
    'HOURLY',
    'active_flag = 1',
    '{"customer_id": "customer_id", "customer_name": "customer_name"}'
);
```

## Usage

### Running Sync Jobs

```bash
# Sync a specific object
ingen_fab package synapse run --object-id CUSTOMER_SYNC

# Sync all active objects
ingen_fab package synapse run --sync-all

# Sync with specific direction
ingen_fab package synapse run --sync-direction FABRIC_TO_SYNAPSE

# Dry run to preview changes
ingen_fab package synapse run --object-id CUSTOMER_SYNC --dry-run
```

### Sync Directions

#### 1. Fabric to Synapse
Export data from Fabric to Synapse Analytics:
```python
sync_direction = "FABRIC_TO_SYNAPSE"
```

#### 2. Synapse to Fabric
Import data from Synapse Analytics to Fabric:
```python
sync_direction = "SYNAPSE_TO_FABRIC"
```

#### 3. Bidirectional
Sync changes in both directions with conflict resolution:
```python
sync_direction = "BIDIRECTIONAL"
conflict_resolution = "LAST_WRITE_WINS"
```

## Sync Patterns

### Full Sync

Replace entire target table with source data:

```python
sync_type = "FULL"
# Truncates target and loads all source data
```

### Incremental Sync

Sync only changed records based on a tracking column:

```python
sync_type = "INCREMENTAL"
sync_column = "last_modified"
# Only syncs records where last_modified > last_sync_timestamp
```

### Merge Sync

Upsert pattern with insert/update/delete operations:

```python
sync_type = "MERGE"
merge_keys = ["customer_id", "order_id"]
# Performs full merge based on key columns
```

### Append Only

Add new records without updating existing:

```python
sync_type = "APPEND_ONLY"
sync_column = "created_date"
# Only inserts new records
```

## Advanced Features

### Column Mappings

Map columns between source and target with transformations:

```json
{
    "column_mappings": {
        "customer_id": "cust_id",
        "first_name || ' ' || last_name": "full_name",
        "UPPER(country)": "country_code",
        "order_date": "CONVERT(DATE, order_date)"
    }
}
```

### Filter Conditions

Apply filters during sync:

```python
filter_condition = """
    active_flag = 1
    AND created_date >= DATEADD(day, -30, GETDATE())
    AND region IN ('US', 'CA', 'MX')
"""
```

### Sync Scheduling

Configure sync frequency:

- `CONTINUOUS` - Near real-time sync (every 5 minutes)
- `HOURLY` - Run at the top of each hour
- `DAILY` - Run once per day at specified time
- `WEEKLY` - Run on specified day(s) of week
- `MONTHLY` - Run on specified day of month
- `CUSTOM` - Cron expression for complex schedules

### Data Type Mapping

Automatic mapping between Fabric and Synapse types:

| Fabric Type | Synapse Type | Notes |
|------------|--------------|-------|
| STRING | NVARCHAR(MAX) | Length preserved if specified |
| INT | INT | Direct mapping |
| BIGINT | BIGINT | Direct mapping |
| DECIMAL(p,s) | DECIMAL(p,s) | Precision preserved |
| DATE | DATE | Direct mapping |
| TIMESTAMP | DATETIME2 | Microsecond precision |
| BOOLEAN | BIT | True=1, False=0 |
| ARRAY | NVARCHAR(MAX) | JSON serialized |
| STRUCT | NVARCHAR(MAX) | JSON serialized |

## Performance Optimization

### Batch Processing

Configure batch sizes for large datasets:

```python
batch_size = 50000  # Records per batch
parallel_threads = 4  # Concurrent batches
```

### Compression

Enable compression for network transfer:

```python
enable_compression = True
compression_type = "GZIP"
```

### Partitioning

Sync partitioned tables efficiently:

```python
partition_column = "order_date"
partition_strategy = "MONTHLY"
sync_latest_partitions = 3  # Sync last 3 months
```

## Monitoring & Logging

### Execution Logs

Query sync history:

```sql
-- View recent sync runs
SELECT
    object_id,
    run_id,
    start_time,
    end_time,
    sync_direction,
    records_read,
    records_inserted,
    records_updated,
    records_deleted,
    status,
    error_message
FROM config.log_synapse_extract_run_log
WHERE start_time >= DATEADD(day, -7, GETDATE())
ORDER BY start_time DESC;
```

### Performance Metrics

Monitor sync performance:

```sql
-- Average sync duration by object
SELECT
    object_id,
    AVG(DATEDIFF(SECOND, start_time, end_time)) as avg_duration_seconds,
    AVG(records_read + records_inserted + records_updated) as avg_records_processed,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs
FROM config.log_synapse_extract_run_log
GROUP BY object_id;
```

### Data Validation

Verify sync accuracy:

```sql
-- Compare record counts
WITH fabric_counts AS (
    SELECT COUNT(*) as fabric_count FROM dbo.customers
),
synapse_counts AS (
    SELECT COUNT(*) as synapse_count FROM SYNAPSE_SERVER.database.staging.stg_customers
)
SELECT
    f.fabric_count,
    s.synapse_count,
    ABS(f.fabric_count - s.synapse_count) as difference
FROM fabric_counts f, synapse_counts s;
```

## Conflict Resolution

### Last Write Wins

Default strategy using timestamp comparison:

```python
conflict_resolution = "LAST_WRITE_WINS"
conflict_column = "last_modified"
```

### Source Wins

Always prefer source system data:

```python
conflict_resolution = "SOURCE_WINS"
```

### Manual Resolution

Flag conflicts for manual review:

```python
conflict_resolution = "MANUAL"
conflict_table = "sync_conflicts"
```

### Custom Logic

Implement business rules for conflict resolution:

```python
conflict_resolution = "CUSTOM"
conflict_procedure = "dbo.sp_resolve_customer_conflicts"
```

## Best Practices

### Design Considerations

1. **Choose appropriate sync direction**
   - One-way for reporting/analytics
   - Bidirectional for operational sync
   - Consider data ownership

2. **Select optimal sync type**
   - Full sync for small reference data
   - Incremental for large transactional data
   - Merge for complex update patterns

3. **Plan for failures**
   - Implement retry logic
   - Set up monitoring alerts
   - Design recovery procedures

### Performance Guidelines

1. **Optimize source queries**
   - Add indexes on sync columns
   - Use filtered indexes for conditions
   - Consider indexed views

2. **Batch large datasets**
   - Prevents memory issues
   - Enables parallel processing
   - Improves failure recovery

3. **Schedule strategically**
   - Avoid peak usage times
   - Consider time zones
   - Balance frequency vs. load

### Data Integrity

1. **Maintain referential integrity**
   - Sync parent tables first
   - Handle cascading updates
   - Validate foreign keys

2. **Handle data types carefully**
   - Test type conversions
   - Handle NULL values
   - Preserve precision

3. **Implement validation**
   - Row count checks
   - Checksum validation
   - Business rule verification

## Troubleshooting

### Common Issues

#### Connection Failures
```bash
# Test connection
ingen_fab package synapse test-connection

# Check credentials
ingen_fab package synapse validate-config
```

#### Slow Performance
- Check network bandwidth
- Optimize batch sizes
- Add missing indexes
- Enable query statistics

#### Data Mismatches
- Verify column mappings
- Check filter conditions
- Review type conversions
- Compare schemas

#### Lock Conflicts
- Adjust isolation levels
- Implement retry logic
- Schedule during low usage
- Use partition switching

## Examples

### Example 1: Daily Customer Sync

Sync customer data from Fabric to Synapse daily:

```sql
INSERT INTO config.config_synapse_extract_objects VALUES (
    'CUST_DAILY_SYNC',
    'Daily Customer Sync to Synapse',
    'Y',
    'FABRIC_TO_SYNAPSE',
    'TABLE',
    'dbo',
    'dim_customer',
    'datamart',
    'dim_customer',
    'FULL',
    NULL,
    'DAILY',
    'active_flag = 1',
    NULL
);
```

### Example 2: Real-time Order Sync

Continuous bidirectional sync of orders:

```sql
INSERT INTO config.config_synapse_extract_objects VALUES (
    'ORDER_REALTIME_SYNC',
    'Real-time Order Synchronization',
    'Y',
    'BIDIRECTIONAL',
    'TABLE',
    'sales',
    'orders',
    'sales',
    'orders',
    'INCREMENTAL',
    'last_updated',
    'CONTINUOUS',
    NULL,
    '{"order_id": "order_id", "customer_id": "customer_id", "order_total": "order_total"}'
);
```

### Example 3: Complex ETL Pattern

Multi-table sync with transformations:

```python
# Configure product dimension sync with lookups
sync_config = {
    "object_id": "PRODUCT_DIM_SYNC",
    "sync_type": "MERGE",
    "source_query": """
        SELECT
            p.product_id,
            p.product_name,
            c.category_name,
            s.supplier_name,
            p.unit_price,
            p.units_in_stock
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN suppliers s ON p.supplier_id = s.supplier_id
        WHERE p.discontinued = 0
    """,
    "target_schema": "warehouse",
    "target_object": "dim_product",
    "merge_keys": ["product_id"],
    "sync_frequency": "HOURLY"
}
```

## Integration

### With Data Factory

Trigger sync from Azure Data Factory:

```json
{
    "type": "WebActivity",
    "name": "TriggerSynapseSync",
    "typeProperties": {
        "url": "https://fabric-api/sync/trigger",
        "method": "POST",
        "body": {
            "object_id": "CUSTOMER_SYNC",
            "run_mode": "INCREMENTAL"
        }
    }
}
```

### With Logic Apps

Orchestrate sync workflows:

```json
{
    "definition": {
        "$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
        "triggers": {
            "Recurrence": {
                "type": "Recurrence",
                "recurrence": {
                    "frequency": "Hour",
                    "interval": 1
                }
            }
        },
        "actions": {
            "Run_Sync": {
                "type": "Http",
                "inputs": {
                    "method": "POST",
                    "uri": "https://fabric-api/package/synapse/run",
                    "body": {
                        "sync_all": true
                    }
                }
            }
        }
    }
}
```

## Next Steps

- Review [sample configurations](https://github.com/your-repo/tree/main/packages/synapse_sync/sample_project)
- Explore [examples and templates](../examples/index.md) for common patterns

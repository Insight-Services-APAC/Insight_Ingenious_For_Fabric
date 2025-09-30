-- Add primary key constraint for config_extract_details table
-- Note: Must be done after table creation in Microsoft Fabric SQL Warehouse
-- This step may fail in Fabric due to transaction isolation but is included for completeness
-- For PostgreSQL: This becomes a standard PRIMARY KEY constraint

-- Drop constraint if it exists (for re-run scenarios)
ALTER TABLE [config].[config_extract_details] DROP CONSTRAINT IF EXISTS PK_config_extract_details;

-- Add the primary key constraint
ALTER TABLE [config].[config_extract_details]
ADD CONSTRAINT PK_config_extract_details
PRIMARY KEY NONCLUSTERED (extract_name) NOT ENFORCED;

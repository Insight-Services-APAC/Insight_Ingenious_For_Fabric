-- Add primary key constraint for log_extract_generation table
-- Note: Must be done after table creation in Microsoft Fabric SQL Warehouse
-- This step may fail in Fabric due to transaction isolation but is included for completeness
-- For PostgreSQL: This becomes a standard PRIMARY KEY constraint

-- Drop constraint if it exists (for re-run scenarios)
ALTER TABLE [log].[log_extract_generation] DROP CONSTRAINT IF EXISTS PK_log_extract_generation;

-- Add the primary key constraint
ALTER TABLE [log].[log_extract_generation] 
ADD CONSTRAINT PK_log_extract_generation 
PRIMARY KEY NONCLUSTERED (log_id) NOT ENFORCED;
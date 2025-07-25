-- Create schemas for Extract Generation package
-- This script creates the necessary schemas if they don't exist

-- Create config schema for configuration tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'config')
BEGIN
    EXEC('CREATE SCHEMA config')
END;

-- Create log schema for logging tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
BEGIN
    EXEC('CREATE SCHEMA log')
END;
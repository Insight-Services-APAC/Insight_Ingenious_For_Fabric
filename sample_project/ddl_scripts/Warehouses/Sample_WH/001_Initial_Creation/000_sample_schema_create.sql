-- Sample DDL script for creating a customer table in warehouse
-- This demonstrates the basic pattern for creating T-SQL tables

-- Create the sample schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sample')
    exec('CREATE SCHEMA sample;')

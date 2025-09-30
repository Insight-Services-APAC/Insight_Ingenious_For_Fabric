-- Sample DDL script for creating a customer table in warehouse
-- This demonstrates the basic pattern for creating T-SQL tables

-- Create the sample customer table
CREATE TABLE sample.customers (
    customer_id BIGINT,
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    email NVARCHAR(255),
    created_date DATETIME2,
    is_active BIT
);

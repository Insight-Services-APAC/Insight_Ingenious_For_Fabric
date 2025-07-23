-- Sample DDL script for creating a customer table in warehouse
-- This demonstrates the basic pattern for creating T-SQL tables

-- Add some sample data
INSERT INTO sample.customers VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '2024-01-01 10:00:00', 1),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2024-01-02 11:00:00', 1),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2024-01-03 12:00:00', 0);

PRINT '✓ Inserted sample data into customers table';
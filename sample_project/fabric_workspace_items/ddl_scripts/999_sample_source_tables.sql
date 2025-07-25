-- Sample source tables for Extract Generation testing

-- Create sample customers table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'customers')
BEGIN
    CREATE TABLE [dbo].[customers] (
        customer_id INT IDENTITY(1,1) PRIMARY KEY,
        customer_name NVARCHAR(100),
        email NVARCHAR(100),
        phone NVARCHAR(20),
        address NVARCHAR(200),
        city NVARCHAR(50),
        country NVARCHAR(50),
        created_date DATETIME2(7) DEFAULT GETUTCDATE()
    );
    
    -- Insert sample data
    INSERT INTO [dbo].[customers] (customer_name, email, phone, address, city, country) VALUES
    ('Acme Corp', 'contact@acme.com', '555-0100', '123 Main St', 'New York', 'USA'),
    ('Global Industries', 'info@global.com', '555-0200', '456 Oak Ave', 'London', 'UK'),
    ('Tech Solutions', 'sales@techsol.com', '555-0300', '789 Pine Rd', 'Tokyo', 'Japan'),
    ('Innovation Ltd', 'hello@innovate.com', '555-0400', '321 Elm St', 'Berlin', 'Germany'),
    ('Future Systems', 'support@future.com', '555-0500', '654 Maple Dr', 'Sydney', 'Australia');
END;

-- Create sample transactions table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'transactions')
BEGIN
    CREATE TABLE [dbo].[transactions] (
        transaction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT,
        product_id INT,
        amount DECIMAL(10,2),
        quantity INT,
        transaction_date DATETIME2(7),
        status VARCHAR(20)
    );
    
    -- Generate sample transactions (this would be larger in production)
    DECLARE @i INT = 1;
    WHILE @i <= 100
    BEGIN
        INSERT INTO [dbo].[transactions] (customer_id, product_id, amount, quantity, transaction_date, status)
        VALUES (
            ABS(CHECKSUM(NEWID())) % 5 + 1,
            ABS(CHECKSUM(NEWID())) % 10 + 1,
            ROUND(RAND() * 1000, 2),
            ABS(CHECKSUM(NEWID())) % 10 + 1,
            DATEADD(day, -ABS(CHECKSUM(NEWID())) % 365, GETUTCDATE()),
            CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN 'pending' WHEN 1 THEN 'completed' ELSE 'cancelled' END
        );
        SET @i = @i + 1;
    END;
END;

-- Create sample orders table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'orders')
BEGIN
    CREATE TABLE [dbo].[orders] (
        order_id INT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT,
        order_date DATETIME2(7),
        total_amount DECIMAL(10,2),
        status VARCHAR(20),
        modified_date DATETIME2(7) DEFAULT GETUTCDATE()
    );
END;

-- Create sales summary view
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[reporting].[v_sales_summary]'))
    DROP VIEW [reporting].[v_sales_summary];
GO

-- Create reporting schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting')
BEGIN
    EXEC('CREATE SCHEMA reporting')
END;
GO

CREATE VIEW [reporting].[v_sales_summary] AS
SELECT 
    YEAR(t.transaction_date) as year,
    MONTH(t.transaction_date) as month,
    c.country,
    COUNT(DISTINCT t.customer_id) as unique_customers,
    COUNT(t.transaction_id) as total_transactions,
    SUM(t.amount) as total_revenue,
    AVG(t.amount) as avg_transaction_value
FROM [dbo].[transactions] t
INNER JOIN [dbo].[customers] c ON t.customer_id = c.customer_id
WHERE t.status = 'completed'
GROUP BY YEAR(t.transaction_date), MONTH(t.transaction_date), c.country;
GO

-- Create finance schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'finance')
BEGIN
    EXEC('CREATE SCHEMA finance')
END;
GO

-- Create sample stored procedure
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[finance].[sp_generate_financial_report]'))
    DROP PROCEDURE [finance].[sp_generate_financial_report];
GO

CREATE PROCEDURE [finance].[sp_generate_financial_report]
AS
BEGIN
    SELECT 
        'Q' + CAST(DATEPART(QUARTER, t.transaction_date) AS VARCHAR(1)) + ' ' + 
        CAST(YEAR(t.transaction_date) AS VARCHAR(4)) as reporting_period,
        c.country as region,
        COUNT(DISTINCT t.customer_id) as active_customers,
        SUM(t.amount) as gross_revenue,
        SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) as net_revenue,
        SUM(CASE WHEN t.status = 'cancelled' THEN t.amount ELSE 0 END) as cancelled_revenue,
        COUNT(t.transaction_id) as total_transactions,
        AVG(t.amount) as avg_transaction_size
    FROM [dbo].[transactions] t
    INNER JOIN [dbo].[customers] c ON t.customer_id = c.customer_id
    WHERE t.transaction_date >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY 
        DATEPART(QUARTER, t.transaction_date),
        YEAR(t.transaction_date),
        c.country
    ORDER BY 
        YEAR(t.transaction_date) DESC,
        DATEPART(QUARTER, t.transaction_date) DESC,
        c.country;
END;
GO

-- Create validation stored procedure
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[dbo].[sp_validate_orders_extract]'))
    DROP PROCEDURE [dbo].[sp_validate_orders_extract];
GO

CREATE PROCEDURE [dbo].[sp_validate_orders_extract]
AS
BEGIN
    -- Simple validation: ensure we have recent orders
    DECLARE @recent_count INT;
    SELECT @recent_count = COUNT(*) 
    FROM [dbo].[orders] 
    WHERE order_date >= DATEADD(DAY, -7, GETDATE());
    
    IF @recent_count = 0
    BEGIN
        RAISERROR('No recent orders found for extraction', 16, 1);
    END;
END;
GO

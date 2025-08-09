-- Views to align synthetic data tables with extract generation expectations
-- These views map the synthetic data schema to what the extract samples expect

-- Create reporting schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting')
BEGIN
    EXEC('CREATE SCHEMA reporting')
END;
GO

-- Create finance schema if not exists  
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'finance')
BEGIN
    EXEC('CREATE SCHEMA finance')
END;
GO

-- View: v_customers_mapped
-- Maps synthetic data customers table to expected format
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[v_customers_mapped]'))
    DROP VIEW [dbo].[v_customers_mapped];
GO

CREATE VIEW [dbo].[v_customers_mapped] AS
SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) as customer_name,
    email,
    phone,
    address_line1 as address,
    city,
    country,
    registration_date as created_date
FROM [dbo].[customers];
GO

-- View: v_transactions
-- Creates a transaction-like view from orders and order_items
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[v_transactions]'))
    DROP VIEW [dbo].[v_transactions];
GO

CREATE VIEW [dbo].[v_transactions] AS
SELECT 
    oi.order_item_id as transaction_id,
    o.customer_id,
    oi.product_id,
    oi.line_total as amount,
    oi.quantity,
    o.order_date as transaction_date,
    o.status
FROM [dbo].[orders] o
INNER JOIN [dbo].[order_items] oi ON o.order_id = oi.order_id;
GO

-- View: v_sales_summary 
-- Sales summary for reporting based on synthetic data
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[reporting].[v_sales_summary]'))
    DROP VIEW [reporting].[v_sales_summary];
GO

CREATE VIEW [reporting].[v_sales_summary] AS
SELECT 
    YEAR(o.order_date) as year,
    MONTH(o.order_date) as month,
    c.country,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    COUNT(DISTINCT o.order_id) as total_transactions,
    SUM(o.order_total) as total_revenue,
    AVG(o.order_total) as avg_transaction_value
FROM [dbo].[orders] o
INNER JOIN [dbo].[customers] c ON o.customer_id = c.customer_id
WHERE o.status IN ('Shipped', 'Delivered')
GROUP BY YEAR(o.order_date), MONTH(o.order_date), c.country;
GO

-- View: v_product_sales_summary
-- Product-based sales analysis
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[reporting].[v_product_sales_summary]'))
    DROP VIEW [reporting].[v_product_sales_summary];
GO

CREATE VIEW [reporting].[v_product_sales_summary] AS
SELECT 
    p.category as product_category,
    p.brand,
    COUNT(DISTINCT oi.order_id) as order_count,
    SUM(oi.quantity) as units_sold,
    SUM(oi.line_total) as total_revenue,
    AVG(oi.unit_price) as avg_unit_price
FROM [dbo].[order_items] oi
INNER JOIN [dbo].[products] p ON oi.product_id = p.product_id
INNER JOIN [dbo].[orders] o ON oi.order_id = o.order_id
WHERE o.status IN ('Shipped', 'Delivered')
GROUP BY p.category, p.brand;
GO

-- Stored Procedure: sp_generate_financial_report
-- Financial report based on synthetic data
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[finance].[sp_generate_financial_report]'))
    DROP PROCEDURE [finance].[sp_generate_financial_report];
GO

CREATE PROCEDURE [finance].[sp_generate_financial_report]
AS
BEGIN
    SELECT 
        'Q' + CAST(DATEPART(QUARTER, o.order_date) AS VARCHAR(1)) + ' ' + 
        CAST(YEAR(o.order_date) AS VARCHAR(4)) as reporting_period,
        c.country as region,
        COUNT(DISTINCT o.customer_id) as active_customers,
        SUM(o.order_total) as gross_revenue,
        SUM(CASE WHEN o.status IN ('Shipped', 'Delivered') THEN o.order_total ELSE 0 END) as net_revenue,
        SUM(CASE WHEN o.status = 'Cancelled' THEN o.order_total ELSE 0 END) as cancelled_revenue,
        COUNT(o.order_id) as total_orders,
        AVG(o.order_total) as avg_order_size
    FROM [dbo].[orders] o
    INNER JOIN [dbo].[customers] c ON o.customer_id = c.customer_id
    WHERE o.order_date >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY 
        DATEPART(QUARTER, o.order_date),
        YEAR(o.order_date),
        c.country
    ORDER BY 
        YEAR(o.order_date) DESC,
        DATEPART(QUARTER, o.order_date) DESC,
        c.country;
END;
GO

-- Stored Procedure: sp_validate_orders_extract
-- Validation procedure for orders extraction
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[dbo].[sp_validate_orders_extract]'))
    DROP PROCEDURE [dbo].[sp_validate_orders_extract];
GO

CREATE PROCEDURE [dbo].[sp_validate_orders_extract]
AS
BEGIN
    -- Validate we have recent orders
    DECLARE @recent_count INT;
    SELECT @recent_count = COUNT(*) 
    FROM [dbo].[orders] 
    WHERE order_date >= DATEADD(DAY, -7, GETDATE());
    
    IF @recent_count = 0
    BEGIN
        RAISERROR('No recent orders found for extraction', 16, 1);
    END;
    
    -- Additional validation: check for orphaned order items
    DECLARE @orphaned_items INT;
    SELECT @orphaned_items = COUNT(*)
    FROM [dbo].[order_items] oi
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[orders] o WHERE o.order_id = oi.order_id);
    
    IF @orphaned_items > 0
    BEGIN
        RAISERROR('Found orphaned order items without corresponding orders', 16, 1);
    END;
END;
GO

-- Stored Procedure: sp_generate_customer_segment_report
-- Customer segmentation analysis
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[reporting].[sp_generate_customer_segment_report]'))
    DROP PROCEDURE [reporting].[sp_generate_customer_segment_report];
GO

CREATE PROCEDURE [reporting].[sp_generate_customer_segment_report]
AS
BEGIN
    SELECT 
        c.customer_segment,
        c.country,
        COUNT(DISTINCT c.customer_id) as customer_count,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.order_total) as lifetime_value,
        AVG(o.order_total) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date
    FROM [dbo].[customers] c
    LEFT JOIN [dbo].[orders] o ON c.customer_id = o.customer_id
    WHERE c.is_active = 1
    GROUP BY c.customer_segment, c.country
    ORDER BY lifetime_value DESC;
END;
GO
CREATE OR ALTER VIEW Reporting.vDim_Supplier
AS
SELECT
    supplier_key,
    supplier_id,
    supplier_name,
    country,
    region,
    CAST(performance_rating AS DECIMAL(3, 1)) AS performance_rating,
    lead_time_days
FROM [lh_gold].[dbo].[dim_supplier]

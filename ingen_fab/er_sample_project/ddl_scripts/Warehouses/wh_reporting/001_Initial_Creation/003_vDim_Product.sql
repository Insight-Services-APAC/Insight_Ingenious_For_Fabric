CREATE OR ALTER VIEW Reporting.vDim_Product
AS
SELECT
    product_key,
    product_id,
    product_name,
    category,
    sub_category,
    CAST(unit_weight_kg AS DECIMAL(8, 2)) AS unit_weight_kg
FROM [lh_gold].[dbo].[dim_product]

CREATE OR ALTER VIEW Reporting.vDim_Region
AS
SELECT
    region_key,
    region_id,
    region_name,
    country,
    zone
FROM [lh_gold].[dbo].[dim_region]

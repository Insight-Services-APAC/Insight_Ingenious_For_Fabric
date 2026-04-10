CREATE OR ALTER VIEW Reporting.vDim_Date
AS
SELECT
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week,
    day_of_week
FROM [lh_gold].[dbo].[dim_date]

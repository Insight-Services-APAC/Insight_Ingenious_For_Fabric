CREATE OR ALTER VIEW Reporting.vFact_Shipments
AS
SELECT
    shipment_key,
    shipment_id,
    order_id,
    product_key,
    supplier_key,
    origin_region_key,
    destination_region_key,
    date_key,
    quantity,
    CAST(unit_cost AS DECIMAL(12, 2))   AS unit_cost,
    CAST(freight_cost AS DECIMAL(12, 2)) AS freight_cost,
    CAST(total_cost AS DECIMAL(14, 2))  AS total_cost,
    scheduled_days,
    actual_days,
    CAST(is_on_time AS BIT)             AS is_on_time,
    status,
    priority
FROM [lh_gold].[dbo].[fact_shipments]

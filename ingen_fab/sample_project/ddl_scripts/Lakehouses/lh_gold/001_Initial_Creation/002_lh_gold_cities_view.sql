-- Create a view for cities in the gold lakehouse
-- This SQL will be executed via the lakehouse SQL endpoint using PyODBC connection

CREATE OR ALTER VIEW dbo.vw_cities AS
SELECT
    city_id,
    city_name,
    state_code,
    country_code,
    population,
    latitude,
    longitude,
    created_date,
    modified_date
FROM dbo.cities
WHERE is_active = 1;

{{ config(materialized='incremental', unique_key='CityID') }}

-- One row per city with its state or province and its country.
select
    c.CityID,
    c.CityName,
    c.LatestRecordedPopulation as CityPopulation,
    s.StateProvinceID,
    s.StateProvinceCode,
    s.StateProvinceName,
    s.SalesTerritory,
    k.CountryID,
    k.CountryName,
    k.IsoAlpha3Code,
    k.Continent,
    k.Region,
    k.Subregion
from {{ ref('cities') }} c
inner join {{ ref('state_provinces') }} s on c.StateProvinceID = s.StateProvinceID
inner join {{ ref('countries') }} k on s.CountryID = k.CountryID

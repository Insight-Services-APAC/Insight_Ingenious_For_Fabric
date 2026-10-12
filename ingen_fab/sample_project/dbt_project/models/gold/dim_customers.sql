{{ config(materialized='incremental', unique_key='CustomerID') }}

-- One row per customer with the geography of its delivery city.
select
    cu.CustomerID,
    cu.CustomerName,
    cu.CustomerCategory,
    cu.BuyingGroup,
    cu.CustomerSegment,
    cu.CreditLimit,
    cu.AccountOpenedDate,
    cu.AccountOpenedYear,
    cu.IsOnCreditHold,
    cu.DeliveryCityID,
    g.CityName as DeliveryCityName,
    g.StateProvinceName as DeliveryStateProvinceName,
    g.SalesTerritory,
    g.CountryID,
    g.CountryName,
    g.Continent
from {{ ref('customers') }} cu
inner join {{ ref('dim_geography') }} g on cu.DeliveryCityID = g.CityID

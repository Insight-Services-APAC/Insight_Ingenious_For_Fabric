{{ config(materialized='table') }}

-- Sales per country and month, with the country's rank and share within the month and its
-- revenue year to date.
with monthly as (

    select
        g.CountryID,
        g.CountryName,
        f.OrderYear,
        f.OrderMonth,
        count(distinct f.OrderID) as OrderCount,
        count(distinct f.CustomerID) as CustomerCount,
        sum(f.Quantity) as Quantity,
        sum(f.LineAmount) as Revenue,
        sum(f.LineTotal) as RevenueIncTax
    from {{ ref('fct_sales') }} f
    inner join {{ ref('dim_geography') }} g on f.DeliveryCityID = g.CityID
    group by g.CountryID, g.CountryName, f.OrderYear, f.OrderMonth

)

select
    CountryID,
    CountryName,
    OrderYear,
    OrderMonth,
    OrderCount,
    CustomerCount,
    Quantity,
    Revenue,
    RevenueIncTax,
    rank() over (partition by OrderYear, OrderMonth order by Revenue desc) as RevenueRank,
    cast(
        Revenue / nullif(sum(Revenue) over (partition by OrderYear, OrderMonth), 0) as decimal(9, 4)
    ) as RevenueShare,
    sum(Revenue) over (
        partition by CountryID, OrderYear
        order by OrderMonth
        rows between unbounded preceding and current row
    ) as RevenueYearToDate
from monthly

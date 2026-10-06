{{ config(materialized='incremental', unique_key='StockItemID') }}

select
    StockItemID,
    StockItemName,
    Brand,
    ColorName,
    StockGroup,
    UnitPrice,
    TaxRate,
    cast(round(UnitPrice * (1 + TaxRate / 100), 2) as decimal(18, 2)) as UnitPriceIncTax,
    case
        when UnitPrice >= 100 then 'Premium'
        when UnitPrice >= 30 then 'Standard'
        else 'Budget'
    end as PriceBand,
    TypicalWeightPerUnit,
    IsChillerStock
from {{ source('bronze', 'stock_items') }}

{{ config(materialized='incremental', unique_key='OrderLineID') }}

select
    OrderLineID,
    OrderID,
    LineNumber,
    StockItemID,
    Quantity,
    UnitPrice,
    TaxRate,
    cast(Quantity * UnitPrice as decimal(18, 2)) as LineAmount,
    cast(round(Quantity * UnitPrice * TaxRate / 100, 2) as decimal(18, 2)) as TaxAmount,
    cast(round(Quantity * UnitPrice * (1 + TaxRate / 100), 2) as decimal(18, 2)) as LineTotal
from {{ source('bronze', 'order_lines') }}

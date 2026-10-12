{{ config(materialized='incremental', unique_key='OrderLineID') }}

-- One row per order line, with the order's date and customer and the line's amounts.
select
    l.OrderLineID,
    l.OrderID,
    l.LineNumber,
    o.CustomerID,
    cu.DeliveryCityID,
    l.StockItemID,
    o.SalespersonPersonID,
    o.OrderDate,
    o.OrderYear,
    o.OrderMonth,
    o.LeadDays,
    l.Quantity,
    l.UnitPrice,
    l.LineAmount,
    l.TaxAmount,
    l.LineTotal
from {{ ref('order_lines') }} l
inner join {{ ref('orders') }} o on l.OrderID = o.OrderID
inner join {{ ref('customers') }} cu on o.CustomerID = cu.CustomerID

{{ config(materialized='incremental', unique_key='OrderID') }}

select
    OrderID,
    CustomerID,
    SalespersonPersonID,
    OrderDate,
    ExpectedDeliveryDate,
    year(OrderDate) as OrderYear,
    month(OrderDate) as OrderMonth,
    {{ dbt.datediff('OrderDate', 'ExpectedDeliveryDate', 'day') }} as LeadDays,
    IsUndersupplyBackordered
from {{ source('bronze', 'orders') }}

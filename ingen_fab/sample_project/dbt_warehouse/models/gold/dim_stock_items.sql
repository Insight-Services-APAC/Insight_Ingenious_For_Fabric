{{ config(materialized='incremental', unique_key='StockItemID') }}

select * from {{ ref('stock_items') }}

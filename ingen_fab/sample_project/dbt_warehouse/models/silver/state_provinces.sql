{{ config(materialized='incremental', unique_key='StateProvinceID') }}

select * from {{ source('bronze', 'state_provinces') }}

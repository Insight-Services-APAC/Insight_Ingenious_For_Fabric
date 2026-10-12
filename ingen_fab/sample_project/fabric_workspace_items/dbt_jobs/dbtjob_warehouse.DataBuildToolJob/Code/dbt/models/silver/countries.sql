{{ config(materialized='incremental', unique_key='CountryID') }}

select * from {{ source('bronze', 'countries') }}

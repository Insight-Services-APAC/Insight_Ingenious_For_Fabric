{{ config(materialized='incremental', unique_key='CityID') }}

select * from {{ source('bronze', 'cities') }}

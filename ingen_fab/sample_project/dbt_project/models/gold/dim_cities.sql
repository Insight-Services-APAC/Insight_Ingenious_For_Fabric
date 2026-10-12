{{ config(materialized='incremental', unique_key='CityID') }}

select * from {{ ref('cities') }}

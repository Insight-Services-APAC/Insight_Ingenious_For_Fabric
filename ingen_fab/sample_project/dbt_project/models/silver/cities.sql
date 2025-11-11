{{ config(
        materialized='incremental',
        unique_key = 'CityID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM {{ source('bronze','cities') }} t1
)

select * from source_data
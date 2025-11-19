

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM lh_bronze.countries t1
)

select * from source_data


-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM lh_bronze.cities t1
)

select * from source_data
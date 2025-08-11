
CREATE VIEW [log].[vw_extract_generation_latest_runs] AS
WITH LatestRuns AS (
    SELECT 
        extract_name,
        MAX(run_timestamp) as latest_run_timestamp
    FROM [log].[log_extract_generation]
    GROUP BY extract_name
)
SELECT 
    l.*
FROM [log].[log_extract_generation] l
INNER JOIN LatestRuns lr 
    ON l.extract_name = lr.extract_name 
    AND l.run_timestamp = lr.latest_run_timestamp;
CREATE TABLE hr_silver
WITH (
    format = 'PARQUET',
    external_location = 's3://hr-data-lake-2026/silver/', 
    partitioned_by = ARRAY['department']
) AS
SELECT
    id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    COALESCE(NULLIF(email, ''), 'n/a') AS email,
    
    CASE 
        WHEN CAST(REGEXP_REPLACE(salary, '[^0-9-]', '') AS INTEGER) < 0 
            THEN CAST(REGEXP_REPLACE(salary, '[^0-9-]', '') AS INTEGER) * -1 
        ELSE CAST(REGEXP_REPLACE(salary, '[^0-9-]', '') AS INTEGER)
    END AS salary,
    
    CAST(DATE_PARSE(hire_date, '%Y-%m-%d') AS DATE) AS hire_date,
    current_date AS extraction_date,
    
    UPPER(TRIM(department)) AS department
FROM hr_bronze;
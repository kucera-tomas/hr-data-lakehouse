-- Optimization file that convert CSV to Parquet
-- Parquet is columnar (faster queries) and compressed (cheaper storage).
-- This uses CTAS (Create Table As Select) to transform the data.

CREATE TABLE IF NOT EXISTS hr_employees_optimized
WITH (
  format = 'PARQUET',
  external_location = 's3://hr-data-lake-2026/optimized/employees/',
  partitioned_by = ARRAY['department_id']
) AS
SELECT 
    employee_id,
    first_name,
    last_name,
    email,
    salary,
    department_id,
    CAST(hire_date AS DATE) as hire_date_fixed
FROM default.hr_employees;
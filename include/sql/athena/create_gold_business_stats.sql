CREATE TABLE hr_gold_department_stats
WITH (
    format = 'PARQUET',
    external_location = 's3://hr-data-lake-2026/gold/department_stats/'
) AS
SELECT
    department,
    COUNT(id) as total_employees,
    CAST(AVG(salary) AS INTEGER) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    -- Calculate average tenure in days
    CAST(AVG(date_diff('day', hire_date, current_date)) AS INTEGER) as avg_tenure_days,
    current_date as report_date
FROM hr_silver
GROUP BY department
ORDER BY avg_salary DESC;
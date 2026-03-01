-- Analysis: High Earner Check
SELECT 
    FIRST_NAME, 
    LAST_NAME, 
    SALARY 
FROM default.hr_employees
WHERE CAST(SALARY AS INT) > 2500;
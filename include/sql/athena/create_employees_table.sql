-- DDL: Create External Table for HR Data
-- Source: s3://hr-data-lake-2026/employees/
CREATE EXTERNAL TABLE IF NOT EXISTS default.hr_employees (
  `EMPLOYEE_ID` int,
  `FIRST_NAME` string,
  `LAST_NAME` string,
  `EMAIL` string,
  `PHONE_NUMBER` string,
  `HIRE_DATE` string,
  `JOB_ID` string,
  `SALARY` int,
  `COMMISSION_PCT` string,
  `MANAGER_ID` int,
  `DEPARTMENT_ID` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
LOCATION 's3://hr-data-lake-2026/employees/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);
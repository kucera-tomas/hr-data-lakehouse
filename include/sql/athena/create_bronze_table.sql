CREATE EXTERNAL TABLE IF NOT EXISTS hr_bronze (
    id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    salary STRING,
    department STRING,
    hire_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '\"'
)
LOCATION 's3://hr-data-lake-2026/bronze/'
TBLPROPERTIES ('skip.header.line.count'='1');
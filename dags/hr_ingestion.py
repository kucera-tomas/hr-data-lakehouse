import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime

# CONSTANTS
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
S3_BUCKET_NAME = "hr-data-lake-2026"
LOCAL_FILE_PATH = "/usr/local/airflow/include/dataset/hr_raw.csv"

with DAG(
    dag_id='hr_data_ingestion',
    start_date=datetime(2026, 3, 1),
    schedule='@daily',
    catchup=False,
    template_searchpath=[f"{AIRFLOW_HOME}/include/sql/athena/"],
    tags=['ingestion', 's3', 'bronze']
) as dag:

    # Task 1: Generate the Data (Simulating Source System)
    generate_data = BashOperator(
        task_id='generate_mock_data',
        bash_command=f'python /usr/local/airflow/src/generators/generate_people.py 1000 {LOCAL_FILE_PATH}'
    )

    # Task 2: Upload to S3 (The "Load" Step)
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename=LOCAL_FILE_PATH,              # laptop file (inside Docker)
        dest_key='bronze/hr_raw_{{ ds }}.csv', # File name in S3 (Dynamic Date!)
        dest_bucket=S3_BUCKET_NAME,            
        aws_conn_id='aws_default',             # AWS Connection
        replace=True                           # Overwrite if we re-run the DAG
    )

    create_bronze_table = AthenaOperator(
        task_id='create_bronze_table',
        query='create_bronze_table.sql',    # The file name we just created
        database='default',                 # The Athena database name
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/', # Where to save query logs
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    drop_bronze_table = AthenaOperator(
        task_id='drop_bronze_table',
        query='drop_bronze.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    drop_silver_table = AthenaOperator(
        task_id='drop_silver_table',
        query='drop_silver.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    transform_silver = AthenaOperator(
        task_id='transform_silver',
        query='transform_silver.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    # Task Dependency
    generate_data >> upload_to_s3 >> drop_bronze_table >> create_bronze_table >> drop_silver_table >> transform_silver
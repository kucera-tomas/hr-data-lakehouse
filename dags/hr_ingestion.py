import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator  # <--- NEW IMPORT
from airflow.operators.python import PythonOperator
from utils.data_quality import check_dq
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

    # Task 1: Generate the Data
    generate_data = BashOperator(
        task_id='generate_mock_data',
        bash_command=f'python /usr/local/airflow/src/generators/generate_people.py 1000 {LOCAL_FILE_PATH}'
    )

    # Task 2: Upload to S3
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename=LOCAL_FILE_PATH,
        dest_key='bronze/hr_raw_{{ ds }}.csv',
        dest_bucket=S3_BUCKET_NAME,
        aws_conn_id='aws_default',
        replace=True
    )

    # --- BRONZE LAYER ---
    drop_bronze_table = AthenaOperator(
        task_id='drop_bronze_table',
        query='drop_bronze.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    create_bronze_table = AthenaOperator(
        task_id='create_bronze_table',
        query='create_bronze_table.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    # --- SILVER LAYER ---
    drop_silver_table = AthenaOperator(
        task_id='drop_silver_table',
        query='drop_silver.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    clean_silver_s3 = S3DeleteObjectsOperator(
        task_id='clean_silver_s3',
        bucket=S3_BUCKET_NAME,
        prefix='silver/hr_cleaned/',
        aws_conn_id='aws_default'
    )

    transform_silver = AthenaOperator(
        task_id='transform_silver',
        query='transform_silver.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    check_silver_quality = PythonOperator(
        task_id='check_silver_data_quality',
        python_callable=check_dq,
        op_kwargs={
            'query': "SELECT COUNT(*) FROM hr_silver WHERE salary < 0",
            'bucket_name': S3_BUCKET_NAME,
            'region_name': 'eu-north-1'
        }
    )

    # --- GOLD LAYER ---
    drop_gold_table = AthenaOperator(
        task_id='drop_gold_table',
        query='drop_gold.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    clean_gold_s3 = S3DeleteObjectsOperator(
        task_id='clean_gold_s3',
        bucket=S3_BUCKET_NAME,
        prefix='gold/department_stats/',
        aws_conn_id='aws_default'
    )

    build_gold_layer = AthenaOperator(
        task_id='build_gold_layer',
        query='create_gold_business_stats.sql',
        database='default',
        output_location=f's3://{S3_BUCKET_NAME}/athena_results/',
        aws_conn_id='aws_default',
        region_name='eu-north-1'
    )

    # Task Dependency Chain
    (
        generate_data 
        >> upload_to_s3 
        >> drop_bronze_table 
        >> create_bronze_table 
        >> drop_silver_table 
        >> clean_silver_s3
        >> transform_silver 
        >> check_silver_quality 
        >> drop_gold_table 
        >> clean_gold_s3
        >> build_gold_layer
    )
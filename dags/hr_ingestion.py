from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='hr_data_ingestion',
    start_date=datetime(2026, 3, 1),
    schedule='@daily',
    catchup=False
) as dag:

    # Task 1: Generate the Data
    # We use BashOperator to run the script inside the Docker container
    generate_data = BashOperator(
        task_id='generate_mock_data',
        bash_command='python /usr/local/airflow/src/generators/generate_people.py'
    )

    # Task 2: (Coming Tomorrow) Upload to S3
    
    generate_data
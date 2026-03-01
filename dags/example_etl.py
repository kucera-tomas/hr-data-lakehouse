from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Set up logging
logger = logging.getLogger("airflow.task")

def start_extraction(ds, **kwargs):
    """
    Simulates the start of an extraction process.
    """
    logger.info(f"Starting extraction for date: {ds}")

    # 'ti' stands for Task Instance
    ti = kwargs['ti']
    ti.xcom_push(key='status', value='started')
    return "Extraction Started"

def complete_extraction(ds, **kwargs):
    """
    Simulates the completion of an extraction process.
    """
    ti = kwargs['ti']
    prev_status = ti.xcom_pull(key='status', task_ids='start_extraction')

    logger.info(f"Previous task status was: {prev_status}")
    logger.info(f"Completing extraction for date: {ds}")
    return "Extraction Complete"

with DAG(
    dag_id='extraction_example',
    description='A simple DAG to demonstrate PythonOperator and XComs',
    start_date=datetime(2026, 3, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='start_extraction',
        python_callable=start_extraction,
    )

    task2 = PythonOperator(
        task_id='complete_extraction',
        python_callable=complete_extraction,
    )

    # This ensures that task2 runs after task1
    task1 >> task2
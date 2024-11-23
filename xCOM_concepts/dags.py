from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import datetime
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 11, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'customer_data_processing',
    default_args=default_args,
    description='ETL pipeline for customer data',
    schedule_interval=timedelta(days=1),  # Runs every day
    catchup=False,
)



# Load function


# Task Definitions
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Task dependencies
extract_task >> transform_task >> load_task

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
from coincap_fetch import fetch_coincap_data
from coincap_data_process import process_coincap_data

# Define the default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Whether to depend on the previous run
    'start_date': datetime.today(),
    'email': ['adityarjkl@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),

}

# haracteristics and scheduling of the entire DAG
dag = DAG(
    'coincap_dag',  # Name of the DAG
    default_args=default_args,  # Apply the default arguments
    # Description of the DAG
    description='Workflow to pull Crypto Data daily and output to CSV',
    schedule_interval='@daily',
    catchup=False,
)


# Define the tasks using PythonOperator
fetch_data_task = PythonOperator(
    task_id='fetch_coincap_data',
    python_callable=fetch_coincap_data,
    dag=dag,  # Reference to the DAG
)

process_data_task = PythonOperator(
    task_id='process_coincap_data',
    python_callable=process_coincap_data,
    provide_context=True,
    dag=dag
)


fetch_data_task >> process_data_task

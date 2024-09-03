from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

def print_hello():
    print("Hello from PythonOperator!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'python_operator_dag',
    default_args=default_args,
    description='A DAG with PythonOperator',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

run_python_function = PythonOperator(
    task_id='run_python_function',
    python_callable=print_hello,
    dag=dag,
)

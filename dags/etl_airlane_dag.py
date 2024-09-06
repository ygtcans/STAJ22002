from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from data_io_manager import S3DataHandler, MySQLDataHandler
from data_cleaner import DataCleaner

# Define constants
S3_BUCKET_NAME = 'ygtcans-test-bucket'
S3_OBJECT_NAME = 'US_Airline_Flight_Routes_and_Fares_1993-2024.csv'
TMP_DIR = 'tmp'
MYSQL_TABLE_NAME = 'flight_data'
CLEANED_FILE_NAME = 'cleaned_US_Airline_Flight_Routes_and_Fares_1993-2024.csv'

# Define functions
def ensure_tmp_dir_exists():
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

def extract_from_s3():
    ensure_tmp_dir_exists()
    s3_handler = S3DataHandler()
    s3_handler.read(
        bucket_name=S3_BUCKET_NAME,
        destination_dir=TMP_DIR,
        object_name=S3_OBJECT_NAME
    )

def transform_data():
    file_path = os.path.join(TMP_DIR, S3_OBJECT_NAME)
    df = pd.read_csv(file_path)
    cleaner = DataCleaner(df)
    cleaned_df = cleaner.clean_data()
    cleaned_file_path = os.path.join(TMP_DIR, CLEANED_FILE_NAME)
    cleaned_df.to_csv(cleaned_file_path, index=False)

def load_to_mysql():
    # Load cleaned data from the temporary file to MySQL
    mysql_handler = MySQLDataHandler()
    cleaned_file_path = os.path.join(TMP_DIR, CLEANED_FILE_NAME)
    df = pd.read_csv(cleaned_file_path)
    mysql_handler.write(data=df, table_name=MYSQL_TABLE_NAME)

def cleanup_files():
    # Clean up temporary files
    if os.path.isdir(TMP_DIR):
        for file in os.listdir(TMP_DIR):
            os.remove(os.path.join(TMP_DIR, file))
        os.rmdir(TMP_DIR)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    's3_to_mysql_etl',
    default_args=default_args,
    description='ETL pipeline from S3 to MySQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract_from_s3',
        python_callable=extract_from_s3,
    )

    t2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    t3 = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql,
    )

    t4 = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files,
    )

    t1 >> t2 >> t3 >> t4

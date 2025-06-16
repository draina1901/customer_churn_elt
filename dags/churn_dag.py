from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime
from Staging_upload import ingestionFunc
from transform_data import transformFunc
import os


# Load environment variables from .env file
load_dotenv()
# Read the cron schedule from env
cron_schedule = os.getenv('CRON_SCHEDULE')

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2025,3,3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)

}

dag = DAG(
    'Job_dag',
    default_args=default_args,
    description = 'job etl',
    schedule_interval=cron_schedule,
    catchup=False                 

)

run_elt = PythonOperator(
    task_id = 'data_ingestion',
    python_callable=ingestionFunc,
    dag=dag

)

run_elt1 = PythonOperator(
    task_id = 'elt_transform',
    python_callable=transformFunc,
   # op_kwargs={'rows_ingested': 0},
    dag=dag

)


run_elt >> run_elt1
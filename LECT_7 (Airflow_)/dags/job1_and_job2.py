from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import os
import time
import requests

JOB1_PORT = 8081
JOB2_PORT = 8082

BASE_DIR = "C:\\Users\\Admin\\PycharmProjects\\Lect2_homework"

RAW_DIR = os.path.join(BASE_DIR, "raw", "sales")  # , "2022-08-09"
STG_DIR = os.path.join(BASE_DIR, "stg", "sales")  # , "2022-08-09"

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 20,
}

def run_job1():
    print("Starting job1:", f'http://host.docker.internal:{JOB1_PORT}/')
    resp = requests.post(
        url=f'http://host.docker.internal:{JOB1_PORT}/',
        json={
            "date": "2022-08-09",
            "raw_dir": RAW_DIR
        }
    )
#    assert resp.status_code == 201
    print("job1 completed!")


def run_job2():
    print("Starting job2:")
    resp = requests.post(
        url=f'http://host.docker.internal:{JOB2_PORT}/',
        json={
            "raw_dir": RAW_DIR,
            "stg_dir": STG_DIR
        }
    )
#    assert resp.status_code == 201
    print("job2 completed!")


dag = DAG(
    dag_id='extract_convert',
    start_date=datetime(2022, 9, 16),
    schedule_interval="0 0 1 * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

task1 = PythonOperator(
    dag=dag,
    task_id='extract_data_from_api',
    python_callable=run_job1,
    op_args=[],
    op_kwargs={}
)

task2 = PythonOperator(
    dag=dag,
    task_id='convert_to_avro',
    python_callable=run_job2,
    op_args=[],
    op_kwargs={}
)

start >> task1 >> task2
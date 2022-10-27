"""
Upload a file to GCS

Notes:
    - all examples can be found here:
    https://github.com/googleapis/python-storage/tree/main/samples/snippets
"""
from google.cloud import storage
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from google.oauth2 import service_account

import json
import os
import tempfile

def upload_blob(bucket_name, source_file_path, destination_blob_name):
    """
    Uploads a file to the bucket.
    """
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    
    storage_client = storage.Client.from_service_account_json('/opt/airflow/keys/my-project-my-marks-7b258129da57.json')
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)

    print(
        f"File {source_file_path} uploaded to {destination_blob_name}."
    )




DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 20,
}

def run_job1():
	upload_blob(
		bucket_name='de2022-paul_skyba_bucket_1',
		source_file_path='/opt/airflow/gcloud/src1_sales_2022-08-01__01.csv',
		destination_blob_name='src1/sales/v1/2022/08/01/src1_sales_2022-08-01__01.csv',
	)


def run_job2():
	upload_blob(
		bucket_name='de2022-paul_skyba_bucket_1',
		source_file_path='/opt/airflow/gcloud/src1_sales_2022-08-02__01.csv',
		destination_blob_name='src1/sales/v1/2022/08/02/src1_sales_2022-08-02__01.csv',
	)
	upload_blob(
		bucket_name='de2022-paul_skyba_bucket_1',
		source_file_path='/opt/airflow/gcloud/src1_sales_2022-08-02__02.csv',
		destination_blob_name='src1/sales/v1/2022/08/02/src1_sales_2022-08-02__02.csv',
	)


dag = DAG(
    dag_id='copy_gcloud_twice11',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    schedule_interval="0 5 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

task1 = PythonOperator(
    dag=dag,
    task_id='copy1',
    python_callable=run_job1,
    end_date=datetime(2022, 8, 2)
)

task2 = PythonOperator(
    dag=dag,
    task_id='copy2',
    python_callable=run_job2,
    start_date=datetime(2022, 8, 2),
    end_date=datetime(2022, 8, 3)
)

start >> [task1 , task2]
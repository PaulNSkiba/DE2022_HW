"""
 Process user_profiles in the final project
"""

import datetime as dt

from airflow import DAG

from google.cloud import bigquery
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 10
}

dag = DAG(
    dag_id='process_user_profiles',
    description='Finally project customers'' part',
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 8, 1),
    catchup=False,
    tags=['user profiles'],
    default_args=DEFAULT_ARGS
)


def load_job():
    client = bigquery.Client.from_service_account_json('/opt/airflow/keys/my-project-my-marks-7b258129da57.json')
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("birth_date", "DATE"),
            bigquery.SchemaField("phone_number", "STRING")
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
    )
    uri = "gs://de2022-paul_skyba_bucket_1/raw/user_profiles/user_profiles.jsonl"
    table_id = "my-project-my-marks.2_silver.user_profiles"

    client.load_table_from_uri(
        uri,
        table_id,
        location="EU",
        job_config=job_config
    )
    return 'Job done!'


transfer_user_profiles_from_raw_to_silver = PythonOperator(
    dag=dag,
    task_id='transfer_user_profiles_from_raw_to_silver',
    python_callable=load_job,
)

transfer_user_profiles_from_raw_to_silver


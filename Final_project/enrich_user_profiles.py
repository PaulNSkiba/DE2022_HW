"""
 Process sales in the final project
"""

import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 10
}

dag = DAG(
    dag_id='enrich_user_profiles',
    description='Finally project enrich user profiles part',
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 8, 1),
    catchup=False,
    tags=['enrich user profiles'],
    default_args=DEFAULT_ARGS
)

enrich_user_profiles = BigQueryInsertJobOperator(
    task_id='enrich_user_profiles',
    dag=dag,
    gcp_conn_id='gcloud-airflow-conn',
    configuration={
        "query": {
            "query": "{% include 'sql/enrich_user_profiles.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'dl_bucket': "de2022-paul_skyba_bucket_1",
        'project_id': "my-project-my-marks"
    }
)

enrich_user_profiles



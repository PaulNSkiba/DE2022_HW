"""
 Process sales in the final project
"""

import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from table_defs.customers_csv import customers_csv

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 10
}

dag = DAG(
    dag_id='process_customers',
    description='Finally project customers'' part',
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 8, 1),
    end_date=dt.datetime(2022, 8, 10),
    catchup=True,
    tags=['customers'],
    default_args=DEFAULT_ARGS
)

transfer_customers_from_raw_to_bronze = BigQueryInsertJobOperator(
    task_id='transfer_customers_from_raw_to_bronze',
    dag=dag,
    gcp_conn_id='gcloud-airflow-conn',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_from_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_csv
            }
        }
    },
    params={
        'dl_bucket': "de2022-paul_skyba_bucket_1",
        'project_id': "my-project-my-marks"
    }
)

transfer_customers_from_raw_to_bronze


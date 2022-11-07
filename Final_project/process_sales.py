"""
 Process sales in the final project
"""

import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from table_defs.sales_csv import sales_csv

DEFAULT_ARGS = {
    'depends_on_past' : True,
    'email_on_failure' : True,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': 10
}

dag = DAG(
    dag_id='process_sales',
    description='Finally project sales'' part',
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 9, 1),
    end_date=dt.datetime(2022, 10, 1),
    catchup=True,
    tags=['sales'],
    default_args=DEFAULT_ARGS
)

transfer_sales_from_raw_to_bronze = BigQueryInsertJobOperator(
    task_id='transfer_sales_from_raw_to_bronze',
    dag=dag,
    gcp_conn_id='gcloud-airflow-conn',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_sales_from_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "sales_csv": sales_csv
            }
        }
    },
    params={
        'dl_bucket': "de2022-paul_skyba_bucket_1",
        'project_id': "my-project-my-marks"
    }
)

transfer_sales_from_raw_to_bronze
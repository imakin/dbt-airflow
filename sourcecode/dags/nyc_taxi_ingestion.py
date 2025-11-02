from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import os
import duckdb
import pandas as pd

from scripts import ingestion, utils

# Default arguments
default_args = {
    'owner': 'izzulmakin',
    'depends_on_past': False,
    'email': ['dwib_uas_mkn@mailhog.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'nyc_taxi_ingestion',
    default_args=default_args,
    description='Download and ingest NYC Taxi trip data',
    schedule_interval='@monthly',  # Run monthly
    start_date=utils.get_simulation_start_date(),
    end_date=utils.get_simulation_end_date(),
    catchup=True, # backfill bulanan
    tags=['taxi', 'datawarehouse'],
) as dag:


    # Define tasks
    task_download = PythonOperator(
        task_id='download_data',
        python_callable=ingestion.download_taxi_data,
        dag=dag,
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=ingestion.validate_file,
        dag=dag,
    )

    task_load = PythonOperator(
        task_id='load_to_db',
        python_callable=ingestion.load_to_database,
        dag=dag,
    )

    task_email_ingestion_notification = EmailOperator(
        task_id='email_ingestion_notification',
        to=[
            os.environ.get('DWIB_UAS_EMAIL_TO', 'makin@mailhog.com'),
        ],
        subject='DAG Ingestion Data - monthly {{ ds }}',
        html_content="""
        <pre>{{ task_instance.xcom_pull(task_ids='load_to_db', key='notification_text') }}</pre>
        <p>time: {{ ts }}</p>
        """,
        dag=dag,
    )

    (
        task_download >> task_validate >> task_load
        >> task_email_ingestion_notification
    )

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

from scripts import utils

default_args = {
    'owner': 'izzulmakin',
    'depends_on_past': False,
    'email': ['dwib_uas_mkn@mailhog.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi_transform',
    default_args=default_args,
    description='dbt transformation',
    schedule='0 6 * * *',  # Daily at 6 AM
    start_date=utils.get_simulation_start_date(),
    end_date=None,  # Allow manual trigger anytime
    catchup=False,
    tags=['datawarehouse', 'dbt'],
) as dag:
    # Task 0 Bila pakai dbt seed untuk load taxi_zone_lookup.csv
    # dbt_seed = BashOperator(
    #     task_id='dbt_seed',
    #     bash_command='cd /opt/airflow/dbt_nyc_taxi && dbt seed',
    # )
    
    # Task 1: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_nyc_taxi && dbt run',
    )
    
    # Task 2: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_nyc_taxi && dbt test',
    )
    
    # Task 3: Generate docs
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt_nyc_taxi && dbt docs generate',
    )
    
    # Task 4: Email notification
    send_notification = EmailOperator(
        task_id='send_notification',
        to=[
            os.environ.get('DWIB_UAS_EMAIL_TO', 'makin@mailhog.com'),
        ],
        subject='[Airflow] dbt Transform Success',
        html_content="""
        <h3>dbt Transformation Completed</h3>
        <p>Date: {{ ds }}</p>
        <p>All models, tests, and docs generated successfully!</p>
        """,
    )
    
    # Dependencies
    # dbt_seed >>
    dbt_run >> dbt_test >> dbt_docs >> send_notification
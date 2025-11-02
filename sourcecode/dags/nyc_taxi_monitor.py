"""
Monitors data quality, freshness, and anomalies
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os

from scripts import monitor, utils


default_args = {
    'owner': 'izzulmakin',
    'depends_on_past': False,
    'email': ['dwib_uas_mkn@mailhog.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'nyc_taxi_monitor',
    default_args=default_args,
    description='Monitor NYC Taxi data quality and anomalies',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=utils.get_simulation_start_date(),
    end_date=None,
    catchup=False,  # trigger manual saja
    tags=['taxi', 'monitoring', 'datawarehouse'],
) as dag:
    
    # Task 1: Check data freshness
    task_freshness = PythonOperator(
        task_id='check_freshness',
        python_callable=monitor.check_data_freshness,
        provide_context=True,
    )
    
    # Task 2: Validate row counts
    task_validate = PythonOperator(
        task_id='validate_counts',
        python_callable=monitor.validate_row_counts,
        provide_context=True,
    )
    
    # Task 3: Compare with historical patterns
    task_compare = PythonOperator(
        task_id='compare_patterns',
        python_callable=monitor.compare_historical_patterns,
        provide_context=True,
    )
    
    # Task 4: Detect anomalies
    task_anomalies = PythonOperator(
        task_id='detect_anomalies',
        python_callable=monitor.detect_anomalies,
        provide_context=True,
    )
    
    # Task 5: Generate summary report
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=monitor.generate_summary_report,
        provide_context=True,
    )
    
    # Task 6: Send email notification
    task_email = EmailOperator(
        task_id='send_monitoring_report',
        to=[os.environ.get('DWIB_UAS_EMAIL_TO', 'makin@mailhog.com')],
        subject='NYC Taxi Monitoring Report - {{ ds }}',
        html_content="{{ task_instance.xcom_pull(task_ids='generate_report', key='summary_report') }}",
    )
    
    # Define task dependencies
    # All checks run in parallel, then anomaly detection, then report generation, then email
    [task_freshness, task_validate, task_compare] >> task_anomalies >> task_report >> task_email

#!/bin/bash
cd /opt/airflow/
/home/airflow/.local/bin/airflow tasks clear nyc_taxi_ingestion     --start-date 2023-01-01  --end-date 2023-03-31 --yes
/home/airflow/.local/bin/airflow dags backfill nyc_taxi_ingestion   --start-date 2023-01-01  --end-date 2023-03-31

/home/airflow/.local/bin/airflow tasks clear nyc_taxi_transform     --start-date 2023-01-01  --end-date 2023-03-31 --yes
/home/airflow/.local/bin/airflow dags backfill nyc_taxi_transform   --start-date 2023-03-01  --end-date 2023-03-31

/home/airflow/.local/bin/airflow tasks clear nyc_taxi_monitor       --start-date 2023-01-01  --end-date 2023-03-31 --yes
/home/airflow/.local/bin/airflow dags backfill nyc_taxi_monitor     --start-date 2023-03-30  --end-date 2023-03-31

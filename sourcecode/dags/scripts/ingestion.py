from datetime import datetime, timedelta
import requests
import os
from functools import lru_cache

import duckdb
import pandas as pd

from scripts.reusables import parquet_columns

from scripts.utils import (
    config,
    BASE_DIR,
    logger,
    context_tag,
    source_get_trip_files,
    source_get_zone_file,
    source_to_tablename,
)



def download_taxi_data(**context):
    """Download monthly Yellow Taxi Parquet file"""
    TAG = f'1.1' + context_tag(**context)
    # soal minta simulasi, jadi time pakai context bukan datetime
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    logger.info(f"="*80)
    logger.info(f"{TAG} - STARTING {context['execution_date']}")

    # taxi data
    trip_files = source_get_trip_files(year, month)
    for k, trip_file in trip_files.items():
        filename = trip_file['filename']
        # bila filename sudah ada, skip download
        if os.path.exists(trip_file['filepath']):
            logger.info(f"{TAG} File sudah ada: {filename}, skipping download.")
            continue
        # download url
        response = requests.get(trip_file['url'], stream=True)
        response.raise_for_status()
        with open(trip_file['filepath'], 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"{TAG} - Downloaded {filename} successfully")
    
    # zone lookup
    zone_file = source_get_zone_file()
    filepath = zone_file['filepath']
    url = zone_file['url']
    if not os.path.exists(filepath):
        response = requests.get(url)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            f.write(response.content)
        logger.info(f"{TAG} - Downloaded {filename} successfully")
    else:
        logger.info(f"{TAG} - File sudah ada: {filename}, skipping download.")



def validate_file(**context):
    """Validate downloaded file"""
    TAG = '1.2' + context_tag(**context)
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    logger.info(f"{TAG} - STARTING {context['execution_date']}")

    # taxi data
    trip_files = source_get_trip_files(year, month)
    for k, trip_file in  trip_files.items():
        filepath = trip_file['filepath']
        filename = trip_file['filename']
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filename}")
        
        # Load Parquet file
        df = pd.read_parquet(filepath)
        # konsistensi lower case
        df.columns = df.columns.str.lower()
        logger.info(f"{TAG} - Loaded {filename} for validation")
        logger.info(f"{TAG} - {filename} has columns: {df.columns.tolist()}")
        # check df not empty, verify semua kolom
        if df.empty:
            raise ValueError(f"File is empty: {filename}")
        
        required_columns = [column.lower() for column in [
            'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'RatecodeID',
            'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
            'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
            'tolls_amount', 'improvement_surcharge',
            'total_amount', 'congestion_surcharge', 'airport_fee'
        ]]
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(
                    f"Missing required column '{col}' in file: {filename}")
        
        # Filter: NULL pickup datetime & invalid datetime
        original_row_count = len(df)
        df_clean = df[
            df['tpep_pickup_datetime'].notna() & 
            df['tpep_dropoff_datetime'].notna()
        ]
        row_count = len(df_clean)
        
        logger.info(f"{TAG} - {filename} has rows:{row_count}, "
                    f"original rows:{original_row_count}, "
                    f"bad records count = {original_row_count - row_count}")
        bad_records_count = original_row_count - row_count
        if bad_records_count > 0:
            logger.warning(
                f"{TAG} - Found {bad_records_count} bad records "
                f"(NULL pickup datetime) in {filename}, excluding from count"
            )

        logger.info(f"{TAG} - Validated {filename}: {row_count} valid rows "
              f"(excluded {bad_records_count} bad records), "
              f"all required columns present.")
        
        # row count dipakai untuk validasi ketika load db
        context['ti'].xcom_push(
            key=f'{filename}_row_count', value=row_count)
        logger.info(f"{TAG} - Validated {filename}: {row_count} valid rows")
        
    
    # zone lookup 
    zone_file = source_get_zone_file()
    zone_filepath = zone_file['filepath']
    zone_filename = zone_file['filename']
    
    if not os.path.exists(zone_filepath):
        raise FileNotFoundError(f"Zone file not found: {zone_filename}")
    
    # baca csv, untuk cek row count
    zone_df = pd.read_csv(zone_filepath)
    zone_row_count = len(zone_df)
    
    # row count dipakai untuk validasi ketika load db
    context['ti'].xcom_push(key='zone_file_row_count', value=zone_row_count)
    logger.info(f"{TAG} - Validated {zone_filename}: {zone_row_count} rows")

    logger.info(f"{TAG} - Validation passed!")
    return True



def load_to_database(**context):
    """Load Parquet & csv file to DuckDB
    sekaligus validate row count yang di-load"""
    TAG = '1.3' + context_tag(**context)
    logger.info(f"{TAG} - STARTING {context['execution_date']}")

    # DuckDB
    conn = duckdb.connect(config.get('paths').get('db_path'))
    
    # schema/namespace
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    notif_text = ""

    # =====
    # trip

    files = source_get_trip_files(
        context['execution_date'].year,
        context['execution_date'].month
    )
    for k,trip_file in files.items():
        filepath = trip_file['filepath']
        filename = trip_file['filename']
        table_name = source_to_tablename(filename)

        expected_row_count = context['ti'].xcom_pull(
            task_ids='validate_data', 
            key=f'{filename}_row_count'
        )

        # konsistenkan column names ke lowercase
        df = pd.read_parquet(filepath)
        df.columns = df.columns.str.lower()


        # 1. Create table jika belum ada
        # duckdb bisa pakai variable/object python (df) langsung di string sql
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM df LIMIT 0
        """)
        
        # hitung existing rows count untuk bulan yang sama
        existing_count = (
            conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE DATE_TRUNC('month', tpep_pickup_datetime) = DATE '{
                    context['execution_date'].date()}'
            """).fetchone()[0]
        )

        logger.info(
            f"{TAG} - Taxi trip table {table_name}. "
            f"datemonth: {context['execution_date'].date()}. existing rows: "
            f"{existing_count}. expected rows: {expected_row_count}."
        )

        # duckdb beda sedikit dengan pandas, mungkin krn ada invalid rows
        TOLERANSI = 200
        selisih = abs(expected_row_count - existing_count)
        # bila existing count tidak sesuai ekspektasi
        # hapus data lama
        if existing_count!= 0 and selisih > TOLERANSI:
            logger.info(f"{TAG} - Hapus data lama di {table_name} "
                        f"untuk bulan {context['execution_date'].date()}"
                        f" karena row count tidak sesuai")    
            execution_month = context['execution_date'].replace(day=1)
            conn.execute(f"""
                DELETE FROM {table_name}
                WHERE DATE_TRUNC('month', tpep_pickup_datetime) = DATE '{
                    execution_month.date()}'
            """)
            existing_count = (
                conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE DATE_TRUNC('month', tpep_pickup_datetime) = DATE '{
                        execution_month.date()}'
                """).fetchone()[0]
            )
            if existing_count!=0:
                msg = (
                    f"Gagal hapus data lama di {table_name}, datetime: "
                    f"{execution_month.date()} remaining rows: {existing_count}."
                )
                logger.error(f"{TAG} - {msg}")
                raise ValueError(msg)
        elif selisih <= TOLERANSI:
            # bila data sudah lengkap
            logger.info(f"{TAG} - Data untuk {context['execution_date'].date()} "
                        f"di {table_name} sudah lengkap, "
                        f"expected: {expected_row_count} "
                        f"existing: {existing_count} "
                        f"skipping load.")
            # Set loaded_rows untuk notification
            loaded_rows = existing_count
            notif_text += (
                f"File: {filename}\n"
                f"Table: {table_name}\n"
                f"Rows already loaded: {loaded_rows} (skipped)\n\n"
            )


        if existing_count==0:
            logger.info(f"{TAG} "
                        f"{context['execution_date'].date()} in {table_name}, "
                        f"masih kosong, lanjut insert new data.")
            # 2. Insert data baru (append)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")

            #rows loaded
            loaded_rows = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE DATE_TRUNC('month', tpep_pickup_datetime) = DATE '{
                    context['execution_date'].date()}'
            """).fetchone()[0]
            
            selisih = abs(loaded_rows - expected_row_count)
            # count lagi setelah load
            if selisih > TOLERANSI:
                error_msg = (
                    f"Trip data row count: selisih pembacaan pandas vs "
                    f"loaded terlalu besar {filename}! "
                    f"Expected: {expected_row_count}, "
                    f"Loaded: {loaded_rows}"
                )
                logger.error(f"{TAG} - {error_msg}")
                raise ValueError(error_msg)
            
            logger.info(
                f"{TAG} - Loaded {loaded_rows} rows into {table_name} "
                f"for {context['execution_date'].date()}"
            )
            notif_text += (
                f"File: {filename}\n"
                f"Table: {table_name}\n"
                f"Rows loaded: {loaded_rows}\n\n"
            )

    
    # =====
    # zone

    zone_file = source_get_zone_file()
    filepath = zone_file['filepath']
    table_name = source_to_tablename(filepath)
    
    
    # Create table jika belum ada
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM read_csv('{filepath}') LIMIT 0
    """)

    existing_count = (
        conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    )

    # Pull expected row count from validate_file task
    expected_row_count = context['ti'].xcom_pull(
        task_ids='validate_data', 
        key='zone_file_row_count'
    )
    
    logger.info(
        f"{TAG} - Zone table {table_name} has "
        f"existing rows: {existing_count}. "
        f"expected rows: {expected_row_count}."
    )

    if expected_row_count is None:
        mgs = ("csv zone lookup, row count harus lebih dari 0 ketika "
                "validate dan ketika dikirim ke load_to_database"
                )
        logger.error(f"{TAG} - {mgs}")
        raise ValueError(mgs)

    if existing_count != expected_row_count:
        # hapus isi bila banyaknya tidak sesuai
        logger.info(f"{TAG} - Hapus data lama di {table_name}"
                    f" karena row count tidak sesuai")
        conn.execute(f"DELETE FROM {table_name}")
        existing_count = (
            conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        if existing_count!=0:
            msg = (
                f"Gagal hapus data lama di {table_name}, "
                f"masih ada {existing_count} rows"
            )
            logger.error(f"{TAG} - {msg}")
            raise ValueError(msg)

    if existing_count == 0:
        # Insert data
        conn.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM read_csv('{filepath}')
        """)
        
        loaded_rows2 = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        
        # Assert that loaded rows match expected rows
        if loaded_rows2 != expected_row_count:
            error_msg = (
                f"Zone data row count tidak sesuai! "
                f"expected: {expected_row_count}, "
                f"loaded: {loaded_rows2}"
            )
            logger.error(f"{TAG} - {error_msg}")
            raise ValueError(error_msg)
        else:
            logger.info(
                f"{TAG} - Zone data validation passed: "
                f"{loaded_rows2} rows sesuai expected: {expected_row_count}"
            )
        
        logger.info(f"{TAG} - Loaded {loaded_rows2} rows into {table_name}")
    else:
        loaded_rows2 = existing_count
        logger.info(f"{TAG} - {table_name} already has {existing_count}"
                    f" rows, skipping load")
    notif_text += (
        f"File: {zone_file['filename']}\n"
        f"Table: {table_name}\n"
        f"Rows loaded: {loaded_rows2}\n\n"
    )
    conn.close()
    
    notif_text = (
        f"nyc_taxi_ingestion for "
        f"{context['execution_date'].strftime('%Y-%m-%d')} success."
        f"\n\nLoad Summary:\n{notif_text}"
    )
    context['ti'].xcom_push(key='notification_text', value=notif_text)    

    return int(loaded_rows)+int(loaded_rows2)

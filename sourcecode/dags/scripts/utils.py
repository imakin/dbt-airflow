import os
import re
from datetime import datetime
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, Iterable
from functools import lru_cache
from . import reusables

BASE_DIR = reusables.get_base_dir('dags')
print('ini',BASE_DIR)
config = reusables.load_yaml(
                BASE_DIR / "config" / "config.yaml"
            )
error_logger = reusables.configure_error_logger(
                BASE_DIR / config.get('logger').get('error_log_file')
            )
logger = reusables.configure_logger(
                BASE_DIR / config.get('logger').get('log_file')
            )

def get_simulation_start_date() -> datetime:
    """ambil start_date dari config.yaml simulation.start_date
    return datetime
    """
    _date = config.get('simulation', {}).get('start_date', '2023-01-01')
    # yaml bisa jadi sudah datetime.date
    if type(_date)==date_type:
        # datetime.date (ga ada waktu dan tz) ke datetime.datetime
        return datetime.strptime(_date.strftime('%Y-%m-%d'), '%Y-%m-%d')
    return datetime.strptime(_date, '%Y-%m-%d')

def get_simulation_end_date() -> datetime:
    """ambil end_date dari config.yaml simulation.end_date
    return datetime
    """
    _date = config.get('simulation', {}).get('end_date', '2023-03-31')
    if type(_date)==date_type:
        # datetime.date (ga ada waktu dan tz) ke datetime.datetime
        return datetime.strptime(_date.strftime('%Y-%m-%d'), '%Y-%m-%d')
    return datetime.strptime(_date, '%Y-%m-%d')

def source_get_trip_files(year, month) -> Dict[str, Dict[str, Any]]:
    """
    ambil list source files
    berdasarkan config.yaml sources.urls.trip_record_format
    return dict: {filename1: {url, vehicle, filename1, filepath}, ...}
    """
    files = {}
    vehicles = config.get('sources').get('vehicles')
    for vehicle in vehicles:
        url = (
            config.get('sources').get('urls').get('trip_record_format').
            format(vehicle, f"{year}-{month:02d}")
        )
        filename = os.path.basename(url)
        files[filename] = {
            'url': url,
            'vehicle': vehicle,
            'filename': filename,
            'filepath': BASE_DIR / config.get('paths').get('raw') / filename
        }
    return files

def source_get_zone_file() -> Dict[str, Any]:
    """ambil file zone lookup
    return dict:{url, filename, filepath}
    """

    url = config.get('sources').get('urls').get('zone_lookup')
    filename = os.path.basename(url)
    return {
        'url': url,
        'filename': filename,
        'filepath': BASE_DIR / config.get('paths').get('raw') / filename
    }

def source_to_tablename(filename, schema_name="raw") -> str:
    """convert source filename to table name
    beda tanggal yyyy-mm tetap satu tabel
    @param filename: str, nama file source
    @param schema_name: str, nama schema di database, default "raw"
    return table_name: str
    """
    filename = os.path.basename(filename)
    table_name = filename.split('.')[0]
    # ambil hanya abjad atau _, hilangkan + dan _yyyy-mm
    table_name = re.sub(r'[^a-zA-Z_]', '', table_name)
    # trim _ didepan dan di belakang
    table_name = table_name.strip('_')
    return f"{schema_name}.{table_name}"


def task_hash(**context):
    # hash context['run_id']+context['task_instance_key_str']
    target = context['run_id'] + context['task_instance_key_str']
    h = hex(hash(target))
    return ('-'+h[3:] if h.startswith('-') else h[2:]) # buang '0x' atau negatif '-0x'

def context_tag(**context) -> str:
    """buat tag unik dari context
    """
    h = task_hash(**context)
    return f"{context['task_instance_key_str']} {h}"
    # return f"{context['task']}_{context['ds_nodash']}_{task_hash(**context)}"


def validate_config_yaml(**_: Dict):
    """validasi config.yaml, harus ada paths: dan source_files:
    raise ValueError bila tidak valid
    """
    logger.info(f"STARTING run_id: {_['dag_run'].run_id}")
    if not config:
        raise ValueError("config/config.yaml tidak ditemukan atau tidak valid")
    if 'paths' not in config:
        raise ValueError("config/config.yaml harus ada paths:")
    if 'raw' not in config.get('paths', {}):
        raise ValueError("config/config.yaml harus ada paths.raw:")
    if 'staging' not in config.get('paths', {}):
        raise ValueError("config/config.yaml harus ada paths.staging:")
    if 'datamart' not in config.get('paths', {}):
        raise ValueError("config/config.yaml harus ada paths.datamart:")

    if 'source_files' not in config:
        raise ValueError("config/config.yaml harus ada source_files:")
    if 'source_schema' not in config:
        raise ValueError("config/config.yaml harus ada source_schema:")
    if 'staging_database_name' not in config:
        raise ValueError("config/config.yaml harus ada staging_database_name:")


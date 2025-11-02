import os
import sys

import duckdb
import pandas as pd



# ke dir file ini
BASE_DIR = (os.path.realpath(os.path.dirname(__file__)))
os.chdir(BASE_DIR)
sys.path.append(BASE_DIR + '/..')

from scripts import utils

print("source files")
url_format = utils.config['sources']['urls']['trip_record_format']
vehicles = utils.config['sources']['vehicles']
vehicle_tables = []
for vehicle in vehicles:
    url = url_format.format(vehicle, '23-01')
    table_name = utils.source_to_tablename(url)
    vehicle_tables.append(table_name)
print(vehicle_tables)

#pilih 1
table_name = vehicle_tables[0].split('.')[-1]
schema = vehicle_tables[0].split('.')[0]

zone_schema, zone_table_name = utils.source_to_tablename(
    utils.config['sources']['urls']['zone_lookup']).split('.')

conn = duckdb.connect('../../data/dwib.duckdb', read_only=True)

print("schemas:")
schemas = conn.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    ORDER BY schema_name
""").df()
print(schemas)

print("tables:")
tables = conn.execute("""
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name
""").df()
print(tables)

print(f"{table_name} columns:")
columns = conn.execute(f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '${table_name}'
    ORDER BY ordinal_position
""").df()
print(columns)

row_count = conn.execute(f'SELECT COUNT(*) as total_rows FROM {schema}.{table_name}').df()
print(row_count)

sample = conn.execute(f'SELECT * FROM {schema}.{table_name} LIMIT 5').df()
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
print(sample)

print('Taxi zone:')
zones_count = conn.execute(f'SELECT COUNT(*) as total_zones FROM {zone_schema}.{zone_table_name}').df()
print(f"Total zones: {zones_count['total_zones'][0]}")
zones_sample = conn.execute(f'SELECT * FROM {zone_schema}.{zone_table_name} LIMIT 10').df()
print(zones_sample)

print("distinc zone boroughs:")
distinct_zones = conn.execute(f"""
    SELECT 
        Borough from {zone_schema}.{zone_table_name}
    GROUP BY Borough
""").df()
print(distinct_zones)

print('test')
quality = conn.execute(f"""
    SELECT 
        COUNT(*) as total_trips,
        COUNT(DISTINCT tpep_pickup_datetime) as unique_pickup_times,
        MIN(tpep_pickup_datetime) as earliest_pickup,
        MAX(tpep_pickup_datetime) as latest_pickup,
        SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) as negative_amounts,
        SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) as zero_distance,
        SUM(CASE WHEN passenger_count IS NULL OR passenger_count = 0 THEN 1 ELSE 0 END) as no_passengers
    FROM {schema}.{table_name}
""").df()
print(quality)

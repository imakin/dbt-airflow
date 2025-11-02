"""
explore database hasil dbt (analytics_staging.staging_taxi_trips)
"""
import os
import duckdb

# ke dir file ini
os.chdir(os.path.realpath(os.path.dirname(__file__)))


conn = duckdb.connect('../../data/dwib.duckdb', read_only=True)

print('=== SCHEMAS ===')
schema = (
    conn.execute('SELECT schema_name FROM information_schema.schemata').df()
)

tables = (
    conn.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'analytics_staging'"
    ).df()
)

print(schema)
print(tables)
print('\n\n')
print('row count:')
print(conn.execute('SELECT COUNT(*) as total_rows FROM analytics_staging.staging_taxi_trips').df())

print('sample 5:')
print(conn.execute('SELECT * FROM analytics_staging.staging_taxi_trips LIMIT 5').df())

conn.close()
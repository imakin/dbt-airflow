import duckdb
import pandas as pd

conn = duckdb.connect('../../data/dwib.duckdb', read_only=True)

print("=" * 80)
print("EXPLORE RAW DATA FOR DBT")
print("=" * 80)

# 1. Check schemas
print("\n1. SCHEMAS:")
schemas = conn.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name").df()
print(schemas)

# 2. Check tables in raw schema
print("\n2. TABLES IN RAW SCHEMA:")
tables = conn.execute("""
    SELECT table_name
    FROM information_schema.tables 
    WHERE table_schema = 'raw'
    ORDER BY table_name
""").df()
print(tables)

# Get row counts for each table
print("\n   ROW COUNTS:")
for table in tables['table_name']:
    count = conn.execute(f"SELECT COUNT(*) as count FROM raw.{table}").fetchone()[0]
    print(f"   - {table}: {count:,} rows")

# 3. Yellow tripdata columns & sample
print("\n3. RAW YELLOW TRIPDATA - COLUMNS:")
columns = conn.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'raw' AND table_name = 'yellow_tripdata'
    ORDER BY ordinal_position
""").df()
print(columns)

print("\n4. RAW YELLOW TRIPDATA - SAMPLE (5 rows):")
sample = conn.execute("SELECT * FROM raw.yellow_tripdata LIMIT 5").df()
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
print(sample)

# 4. Data quality issues to handle in dbt
print("\n5. DATA QUALITY ISSUES:")
quality = conn.execute("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END) as null_pickup,
        SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) as null_dropoff,
        SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) as zero_distance,
        SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) as zero_fare,
        SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) as zero_total,
        SUM(CASE WHEN passenger_count IS NULL OR passenger_count = 0 THEN 1 ELSE 0 END) as no_passengers,
        MIN(tpep_pickup_datetime) as earliest_trip,
        MAX(tpep_pickup_datetime) as latest_trip
    FROM raw.yellow_tripdata
""").df()
print(quality)

# 5. Zone lookup
print("\n6. RAW TAXI ZONES - SAMPLE:")
zones = conn.execute("SELECT * FROM raw.taxi_zone_lookup LIMIT 10").df()
print(zones)

print("\n7. ZONE BOROUGHS:")
boroughs = conn.execute("""
    SELECT "Borough", COUNT(*) as zone_count
    FROM raw.taxi_zone_lookup
    GROUP BY "Borough"
    ORDER BY zone_count DESC
""").df()
print(boroughs)

conn.close()

print("\n" + "=" * 80)
print("EXPLORATION COMPLETE!")
print("=" * 80)

import duckdb
import pandas as pd

conn = duckdb.connect('../../data/dwib.duckdb', read_only=True)

print('=== RAW_TAXI_ZONES TABLE INFO ===')
print('\n1. Total Rows:')
total = conn.execute('SELECT COUNT(*) as total_rows FROM raw.raw_taxi_zones').df()
print(total)

print('\n2. Unique LocationIDs:')
unique_ids = conn.execute('SELECT COUNT(DISTINCT "LocationID") as unique_location_ids FROM raw.raw_taxi_zones').df()
print(unique_ids)

print('\n3. Check for Duplicates by LocationID:')
duplicates = conn.execute("""
    SELECT 
        "LocationID",
        COUNT(*) as duplicate_count,
        STRING_AGG(DISTINCT "Borough", ', ') as boroughs,
        STRING_AGG(DISTINCT "Zone", ', ') as zones
    FROM raw.raw_taxi_zones
    GROUP BY "LocationID"
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC
""").df()
print(f"Total LocationIDs with duplicates: {len(duplicates)}")
if len(duplicates) > 0:
    print(duplicates)

print('\n4. Sample Data from raw_taxi_zones:')
sample = conn.execute('SELECT * FROM raw.raw_taxi_zones LIMIT 20').df()
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
print(sample)

print('\n5. Check Exact Row Duplicates:')
exact_duplicates = conn.execute("""
    SELECT 
        "LocationID", "Borough", "Zone", "service_zone",
        COUNT(*) as duplicate_count
    FROM raw.raw_taxi_zones
    GROUP BY "LocationID", "Borough", "Zone", "service_zone"
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC
""").df()
print(f"Total exact row duplicates: {len(exact_duplicates)}")
if len(exact_duplicates) > 0:
    print(exact_duplicates.head(10))

print('\n6. Summary Statistics:')
summary = conn.execute("""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT "LocationID") as unique_locations,
        COUNT(DISTINCT "Borough") as unique_boroughs,
        COUNT(DISTINCT "Zone") as unique_zones,
        COUNT(*) - COUNT(DISTINCT "LocationID") as duplicate_rows
    FROM raw.raw_taxi_zones
""").df()
print(summary)

conn.close()

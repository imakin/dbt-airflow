import os
import sys
import unittest
from pathlib import Path
import pandas as pd
sys.path.append("..")

print(f'ran with {sys.argv}')
from scripts import utils

year = 2023
dfs = []
for month in range(1, 4):
    trip_files = utils.source_get_trip_files(year, month)
    print(trip_files)
    vehicle = [k for k in trip_files][0]
    filename = trip_files[vehicle]['filename']
    filepath = trip_files[vehicle]['filepath']

    df = pd.read_parquet(filepath)
    print(df.head())
    dfs.append(df)
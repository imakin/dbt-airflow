import os
import sys
import unittest
from pathlib import Path
sys.path.append("..")

from scripts import utils

class TestReusables(unittest.TestCase):
    def test_config(self):
        """example config.yaml content:

            paths:
            raw: data/raw
            staging: data/staging
            datamart: data/datamart

            sources:
            urls:
                zone_lookup: https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
                trip_record_format: https://d37ci6vzurychx.cloudfront.net/trip-data/{}_tripdata_{}.parquet
            vehicles:
                yellow: yellow taxi
                green: green taxi
                fhv: for-hire vehicle
                fhvhv: high volume for-hire vehicle

            # simulasi tanggal sesuai perintah soal
            simulation:
            times:
                2023:
                01: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]
                02: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28]
                03: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]
            command_simulasi: >-
            airflow dags backfill nyc_taxi_ingestion 
            --start-date 2023-01-01 --end-date 2023-03-31

            notification_path: logs/notification.json
            report_path: logs/execution_report.json

            logger:
            log_file: sourcecode/logs/system.log
            error_log_file: sourcecode/logs/error.log
        """
        config = utils.config
        self.assertIsInstance(config, dict)
        self.assertIn("paths", config)
        self.assertIn("sources", config)
        self.assertIn("urls", config["sources"])
        self.assertIn("zone_lookup", config["sources"]["urls"])
        self.assertIn("trip_record_format", config["sources"]["urls"])
        self.assertIn("vehicles", config["sources"])
        
        self.assertIn("simulation", config)
        self.assertIn("times", config["simulation"])

        self.assertIn("command_simulasi", config)
        self.assertIn("notification_path", config)
        self.assertIn("report_path", config)
        
        self.assertIn("logger", config)
        self.assertIn("log_file", config["logger"])
        self.assertIn("error_log_file", config["logger"])
        

if __name__ == '__main__':
    unittest.main()
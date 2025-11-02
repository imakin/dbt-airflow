import os
import sys
import unittest
from pathlib import Path
sys.path.append("..")

from scripts import reusables

class TestReusables(unittest.TestCase):
    def test_base_dir(self):
        base_dir = reusables.get_base_dir('dags')
        self.assertIn("dags", os.listdir(base_dir ))


if __name__ == '__main__':
    unittest.main()
import unittest
from pathlib import Path
from mock import Mock
from datetime import datetime

from index_nci_modis_lpdaac import *


class Testlpdaac(unittest.TestCase):

    def test_modis_path_to_date_range(self):
        # not a real file though
        name = "/2017/12.11/MYD13Q1.A2017345.h29v12.006.2017361232038.hdf"
        file_path = Path(name)
        start_time, end_time = modis_path_to_date_range(file_path)

        self.assertEqual(start_time, datetime(2017, 12, 11, 0, 0))
        self.assertEqual(end_time, datetime(2017, 12, 26, 23, 59, 59, 999999))

    def test_split_path(self):
        apath = "S:D:/g/data/4.hdf:VI:250m 16 days blue reflectance"
        fmt, local_path, layer = split_path(apath)
        self.assertEqual(fmt, 'S:D')
        self.assertEqual(local_path, '/g/data/4.hdf')
        self.assertEqual(layer, 'VI:250m 16 days blue reflectance')


# -------------------------------------------------------------
if __name__ == "__main__":
    Suite = unittest.makeSuite(Testlpdaac, 'test')
    Runner = unittest.TextTestRunner()
    Runner.run(Suite)

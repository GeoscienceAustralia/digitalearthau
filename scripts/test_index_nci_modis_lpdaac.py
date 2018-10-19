import unittest
from mock import Mock

from index_nci_modis_lpdaac import *

class Testlpdaac(unittest.TestCase):

    def test_modis_path_to_date_range(self):
        name = "/2017/12.11/MYD13Q1.A2017345.h29v12.006.2017361232038.hdf"
        file_path = Mock() # 2017/12/12
        print(file_path.name)
        modis_path_to_date_range(file_path)

        self.assertEqual(35, 35)
# -------------------------------------------------------------
if __name__ == "__main__":
    Suite = unittest.makeSuite(Testlpdaac, 'test')
    Runner = unittest.TextTestRunner()
    Runner.run(Suite)
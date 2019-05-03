from pathlib import Path
from datetime import datetime

from scripts.index_nci_modis_lpdaac import *


def test_modis_path_to_date_range():
    # not a real file though
    name = "/2017/12.11/MYD13Q1.A2017345.h29v12.006.2017361232038.hdf"
    file_path = Path(name)
    start_time, end_time = modis_path_to_date_range(file_path)

    assert start_time == datetime(2017, 12, 11, 0, 0)
    assert end_time == datetime(2017, 12, 26, 23, 59, 59, 999999)


def test_split_path():
    apath = "S:D:/g/data/4.hdf:VI:250m 16 days blue reflectance"
    fmt, local_path, layer = split_path(apath)
    assert fmt == 'S:D'
    assert local_path == '/g/data/4.hdf'
    assert layer == 'VI:250m 16 days blue reflectance'

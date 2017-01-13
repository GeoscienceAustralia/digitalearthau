from . import paths


def test_list_file_paths():
    d = paths.write_files({
        "file1.txt": 'test',
        'dir1': {
            'file2.txt': 'test'
        }
    })

    listed_files = set(paths.list_file_paths(d))

    assert listed_files == {
        d.joinpath('file1.txt'),
        d.joinpath('dir1', 'file2.txt'),
    }


def test_get_data_paths_package():
    packaged_dataset = paths.write_files({
        'ga-metadata.yaml': '',
        'package': {
            'file1.txt': ''
        }
    })

    metadata_path = packaged_dataset.joinpath('ga-metadata.yaml')
    assert paths.get_metadata_path(packaged_dataset) == metadata_path

    base_path, all_files = paths.get_dataset_paths(metadata_path)
    assert base_path == packaged_dataset
    assert set(all_files) == {
        metadata_path,
        packaged_dataset.joinpath('package', 'file1.txt')
    }


def test_get_data_paths_netcdf():
    d = paths.write_files({
        'LS7_SOMETHING.nc': '',
    })

    # A netcdf file is it's own metadata and data file in one.
    netcdf_path = d.joinpath('LS7_SOMETHING.nc')
    assert paths.get_metadata_path(netcdf_path) == netcdf_path

    base_path, all_files = paths.get_dataset_paths(netcdf_path)
    assert base_path == netcdf_path
    assert set(all_files) == {
        netcdf_path
    }


def test_get_data_paths_sibling():
    sibling_dataset = paths.write_files({
        'LS7_SOMETHING.tif': '',
        'LS7_SOMETHING.tif.ga-md.yaml': '',

    })

    # A data file can have a sibling metadata file with extention '.ga-md.yaml'
    data_path = sibling_dataset.joinpath('LS7_SOMETHING.tif')
    metadata_path = sibling_dataset.joinpath('LS7_SOMETHING.tif.ga-md.yaml')
    assert paths.get_metadata_path(data_path) == metadata_path

    base_path, all_files = paths.get_dataset_paths(metadata_path)
    assert base_path == data_path
    assert set(all_files) == {
        metadata_path,
        data_path
    }

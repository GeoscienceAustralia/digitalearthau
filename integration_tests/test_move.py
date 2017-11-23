"""
Test out the dea-move CLI command.

Creates datasets on the filesystem, indexes them into a database, and then attempts to move them.

"""
import shutil
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from digitalearthau import move, paths, collections
from digitalearthau.collections import Collection
from integration_tests.conftest import DatasetForTests, freeze_index


@pytest.fixture
def destination_path(tmpdir) -> Path:
    """A directory that datasets can be moved to or from.

    Provides a temp directory that is registered with the `digitalearthau.paths` module as a base directory.
    """
    destination = Path(tmpdir) / 'destination_collection'
    destination.mkdir(exist_ok=False)
    paths.register_base_directory(destination)
    return destination


@pytest.fixture
def example_nc_dataset(integration_test_data, dea_index):
    template = integration_test_data / 'example_nbar_dataset.yaml'
    path_offset = ('LS8_OLI_NBAR', '17_-29', 'LS8_OLI_NBAR_3577_17_-29_20161018000035500000_v1508400361.nc')

    dataset_file = integration_test_data.joinpath(*path_offset)
    dataset_file.parent.mkdir(parents=True)
    make_fake_netcdf_dataset(dataset_file, template)

    assert dataset_file.exists()

    ls8_nc_test = Collection(
        name='ls8_nc_test',
        query={},
        file_patterns=[str(integration_test_data.joinpath('LS8_OLI_NBAR/*_*/LS8*.nc'))],
        unique=[],
        index_=dea_index
    )
    collections._add(ls8_nc_test)

    return DatasetForTests(
        collection=ls8_nc_test,
        id_=uuid.UUID('6b8d2798-cbc2-4244-847f-807cd068e9ad'),
        base_path=integration_test_data,
        path_offset=path_offset,
        parent_id=uuid.uuid4()
    )


def test_move(global_integration_cli_args,
              test_dataset: DatasetForTests,
              other_dataset: DatasetForTests,
              destination_path):
    """
    With two datasets in a collection, try to move one to a new disk location.

    One should be unchanged, the other should be moved.
    """
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    # Expect a destination_path with the same offset (eg "year/LS8_SOMETHING_AROTHER") but in our new base folder.
    expected_new_path = destination_path.joinpath(*test_dataset.path_offset)

    assert test_dataset.path != expected_new_path

    # Move one path to destination_path
    res: Result = _call_move(['--destination', destination_path, test_dataset.path], global_integration_cli_args)

    _check_successful_move(test_dataset, expected_new_path, other_dataset, res)


def test_nc_move(global_integration_cli_args,
                 example_nc_dataset,
                 other_dataset: DatasetForTests,
                 destination_path):
    """
    Move a dataset comprising a single ODC style NetCDF file with embedded dataset info
    """
    example_nc_dataset.add_to_index()
    example_nc_dataset.collection.file_patterns.append(str(destination_path))
    other_dataset.add_to_index()

    expected_destination = destination_path.joinpath(*example_nc_dataset.path_offset)
    assert example_nc_dataset.path != expected_destination
    res: Result = _call_move(['--no-checksum', '--destination', destination_path, example_nc_dataset.path],
                             global_integration_cli_args)

    _check_successful_move(example_nc_dataset, expected_destination, other_dataset, res)


@pytest.mark.xfail(reason="Not yet implemented", strict=True)
def test_move_when_already_exists_at_dest(global_integration_cli_args,
                                          test_dataset: DatasetForTests,
                                          other_dataset: DatasetForTests,
                                          destination_path):
    """
    Move a dataset to a new location, but it already exists and is valid at the destination

    This situation arose in real usage when datasets were
    1) moved, without cleaning up the archived datasets
    2) and then moved back (archived destination exists already).
    """
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    expected_new_path = destination_path.joinpath(*test_dataset.path_offset)

    # Preemptively copy dataset to destination.
    shutil.copytree(str(test_dataset.copyable_path), str(expected_new_path.parent))

    # Move one path to destination_path
    res = _call_move(['--destination', destination_path, test_dataset.path], global_integration_cli_args)

    _check_successful_move(test_dataset, expected_new_path, other_dataset, res)


def test_move_when_corrupt_exists_at_dest(global_integration_cli_args,
                                          test_dataset: DatasetForTests,
                                          other_dataset: DatasetForTests,
                                          destination_path):
    """
    Move a dataset to a location that already exists but is invalid.

    It should see that the destination is corrupt and skip the move.
    """
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    expected_new_path: Path = destination_path.joinpath(*test_dataset.path_offset)

    # Create a corrupt dataset at destination
    expected_new_path.parent.mkdir(parents=True)
    expected_new_path.write_text("invalid")

    original_index = freeze_index(test_dataset.collection.index_)

    # Move one path to destination_path
    res = _call_move(['--destination', destination_path, test_dataset.path], global_integration_cli_args)

    # Move script should have completed, but dataset should have been skipped.
    assert res.exit_code == 0, res.output
    print(res.output)

    now_index = freeze_index(test_dataset.collection.index_)

    assert original_index == now_index

    assert test_dataset.path.exists()


def _check_successful_move(test_dataset: DatasetForTests,
                           expected_new_path: Path,
                           unrelated_untouched_dataset: DatasetForTests,
                           res: Result):
    assert res.exit_code == 0, res.output
    print(res.output)

    assert expected_new_path.exists(), "File was not moved to destination_path?"
    assert test_dataset.path.exists(), "Old file was deleted: should only have been archived"

    expected_new_uri = expected_new_path.as_uri()

    # All three uris should be in the system still.
    all_indexed_uris = set(test_dataset.collection.all_indexed_uris())
    assert all_indexed_uris == {
        # The old uri one still indexed (but archived)
        test_dataset.uri,
        # The new uri
        expected_new_uri,
        # The other dataset unchanged
        unrelated_untouched_dataset.uri
    }
    # ... but only the new uri is active, so the moved dataset should only list that one:
    dataset = test_dataset.get_index_record()
    assert dataset is not None, "Moved dataset is no longer indexed??"
    assert dataset.uris == [expected_new_uri], "Expect only the newly moved location to be listed for the dataset"


def _call_move(args, global_integration_cli_args) -> Result:
    # We'll call the cli interface itself, rather than the python api, to get wider coverage in our test.
    res: Result = CliRunner().invoke(
        move.cli,
        global_integration_cli_args + [str(arg) for arg in args],
        catch_exceptions=False
    )
    return res


def make_fake_netcdf_dataset(nc_name, yaml_doc):
    from datacube.model import Variable
    from datacube.storage.netcdf_writer import create_variable, netcdfy_data
    from netCDF4 import Dataset
    import numpy as np
    content = yaml_doc.read_text()
    npdata = np.array([content], dtype=bytes)

    with Dataset(nc_name, 'w') as nco:
        var = Variable(npdata.dtype, None, ('time',), None)
        nco.createDimension('time', size=1)
        create_variable(nco, 'dataset', var)
        nco['dataset'][:] = netcdfy_data(npdata)

    # from datacube.utils import read_documents

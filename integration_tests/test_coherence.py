from pathlib import Path
import itertools
import pytest

from click.testing import CliRunner

from integration_tests.conftest import DatasetForTests
from digitalearthau import coherence

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_FILE_PATHS = [str(PROJECT_ROOT / 'digitalearthau/testing/testing-default.conf')]


@pytest.fixture(autouse=True)
def csvfile(tmpdir):
    coherence.CSV_OUTPUT_FILE = Path(tmpdir).joinpath('test.csv')
    return coherence.CSV_OUTPUT_FILE


def assert_click_command(command, expected_value, args):
    exe_opts = list(itertools.chain(*(('--test-dc-config', f) for f in CONFIG_FILE_PATHS)))
    exe_opts.extend(args)

    result = CliRunner().invoke(
        command,
        args=exe_opts,
        catch_exceptions=False
    )
    assert result.exit_code == 0, "Error for %r. output: %r" % (exe_opts, result.output)
    assert 'coherence.finish' in result.output
    assert expected_value in result.output


def test_check_locationless(ls8_pq_scene_test_dataset: DatasetForTests,
                            test_dataset: DatasetForTests,
                            csvfile: str):
    """
    Test dea-coherence --check-locationless command option
    """
    # Add new test dataset to index
    ls8_pq_scene_test_dataset.add_to_index()

    # Check if the new dataset has valid location
    assert len(ls8_pq_scene_test_dataset.get_index_record().uris) != 0, "Valid location expected for test dataset"

    # Remove the location in the index
    ls8_pq_scene_test_dataset.remove_location_in_index()

    # Add second dataset to index
    test_dataset.add_to_index()

    # Check if the other dataset has valid location
    assert len(test_dataset.get_index_record().uris) != 0, "Valid location expected for other dataset"

    # Remove the location in the index
    test_dataset.archive_location_in_index()

    # Ensure datasets still exists in the index and they do not have locations
    assert ls8_pq_scene_test_dataset.get_index_record() is not None, "Test dataset should still be in the index"
    assert test_dataset.get_index_record() is not None, "Other dataset should still be in the index"

    assert len(ls8_pq_scene_test_dataset.get_index_record().uris) == 0, "Test dataset location should none"
    assert len(test_dataset.get_index_record().uris) == 0, "Other dataset location should none"

    # Run Coherence with --check-locationless argument
    exe_opts = ['--check-locationless']

    timerange = ["2000-01-01 < time < 2018-12-31"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, 'locationless_count=2', exe_opts)


def test_check_downstream_datasets(ls8_pq_scene_test_dataset: DatasetForTests,
                                   test_dataset: DatasetForTests,
                                   csvfile: str):
    """
    Test dea-coherence --check-downstream-ds command option
    """
    # Add pq and other test dataset to index
    ls8_pq_scene_test_dataset.add_to_index()
    test_dataset.add_to_index()

    # Archive parent dataset for pq scene
    ls8_pq_scene_test_dataset.archive_parent_in_index()

    # Ensure the datasets still exists in the index and they do have locations
    assert ls8_pq_scene_test_dataset.get_index_record() is not None
    assert test_dataset.get_index_record() is not None

    all_indexed_uris = set(ls8_pq_scene_test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {ls8_pq_scene_test_dataset.uri, test_dataset.uri}, "Both dataset uri's should remain."

    # Run Coherence with --check-downstream argument
    exe_opts = ['--check-downstream']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, 'downstream_dataset_error_count=1', exe_opts)


def test_check_ancestors(ls8_pq_scene_test_dataset: DatasetForTests,
                         test_dataset: DatasetForTests,
                         csvfile: str):
    """
    Test dea-coherence --check-ancestors command option
    """
    # Add pq and other test dataset to index
    ls8_pq_scene_test_dataset.add_to_index()
    test_dataset.add_to_index()

    # Archive parent datasets
    ls8_pq_scene_test_dataset.archive_parent_in_index()
    test_dataset.archive_parent_in_index()

    # Ensure the datasets still exists in the index and they do have locations
    assert test_dataset.path.exists(), "Other dataset path exists"
    assert ls8_pq_scene_test_dataset.path.exists(), "Test dataset path exists"

    assert ls8_pq_scene_test_dataset.get_index_record() is not None
    assert test_dataset.get_index_record() is not None

    # Ensure the parent dataset have no locations
    assert len(ls8_pq_scene_test_dataset.get_parent_index_record().uris) == 0, "Test_ds parent location is not none"
    assert len(ls8_pq_scene_test_dataset.get_parent_index_record().uris) == 0, "Other_ds parent location should none"

    # Run Coherence with --check-ancestors argument
    exe_opts = ['--check-ancestors']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, 'archived_ancestor_count=2', exe_opts)


def test_archive_locationless(ls8_pq_scene_test_dataset: DatasetForTests,
                              test_dataset: DatasetForTests,
                              csvfile: str):
    """
    Test dea-coherence --archive-locationless command option
    """
    # Add pq and other test dataset to index
    ls8_pq_scene_test_dataset.add_to_index()
    test_dataset.add_to_index()

    # Remove the location in the index
    ls8_pq_scene_test_dataset.remove_location_in_index()
    test_dataset.remove_location_in_index()

    # Run Coherence to archive locationless datasets with --archive-locationless argument
    exe_opts = ['--archive-locationless']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, 'archived_locationless_count=2', exe_opts)

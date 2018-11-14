from datetime import datetime, timedelta
from pathlib import Path
import itertools

import pytest
from click.testing import CliRunner

from integration_tests.conftest import DatasetForTests
from digitalearthau import coherence

# Default is to clean up older than three days ago.
A_LONG_TIME_AGO = datetime.utcnow() - timedelta(days=4)

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_FILE_PATHS = [str(PROJECT_ROOT / 'digitalearthau/testing/testing-default.conf')]


@pytest.fixture
def csvfile(tmpdir):
    coherence.DEFAULT_CSV_FILE = Path(tmpdir).joinpath('test.csv')
    return coherence.DEFAULT_CSV_FILE


def assert_click_command(command, args):
    exe_opts = list(itertools.chain(*(('--test-dc-config', f) for f in CONFIG_FILE_PATHS)))
    exe_opts.extend(args)

    result = CliRunner().invoke(
        command,
        args=exe_opts,
        catch_exceptions=False
    )
    assert result.exit_code == 0, "Error for %r. output: %r" % (exe_opts, result.output)


def test_check_locationless(ls8_pq_scene_test_dataset: DatasetForTests,
                            other_dataset: DatasetForTests,
                            csvfile: str):
    """
    Test dea-coherence --check-locationless command option
    """
    # Newly archived
    ls8_pq_scene_test_dataset.add_to_index()
    assert len(ls8_pq_scene_test_dataset.get_index_record().uris) != 0, "Valid location expected for test dataset"
    ls8_pq_scene_test_dataset.remove_location_in_index()

    # Archived a while ago
    other_dataset.add_to_index()
    assert len(other_dataset.get_index_record().uris) != 0, "Valid location expected for other dataset"
    other_dataset.archive_location_in_index()

    assert ls8_pq_scene_test_dataset.get_index_record() is not None, "Test dataset should still be in the index"
    assert other_dataset.get_index_record() is not None, "Other dataset should still be in the index"

    assert len(ls8_pq_scene_test_dataset.get_index_record().uris) == 0, "Test dataset location should none"
    assert len(other_dataset.get_index_record().uris) == 0, "Other dataset location should none"

    exe_opts = ['--check-locationless']

    timerange = ["2000-01-01 < time < 2018-12-31"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)


def test_check_downstream_datasets(ls8_pq_scene_test_dataset: DatasetForTests,
                                   other_dataset: DatasetForTests,
                                   csvfile: str):
    """
    Test dea-coherence --check-downstream-ds command option
    """
    ls8_pq_scene_test_dataset.add_to_index()
    other_dataset.add_to_index()

    ls8_pq_scene_test_dataset.archive_parent_in_index()

    assert ls8_pq_scene_test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

    all_indexed_uris = set(ls8_pq_scene_test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {ls8_pq_scene_test_dataset.uri, other_dataset.uri}, "Both dataset uri's should remain."

    exe_opts = ['--check-downstream']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)


def test_check_ancestors(ls8_pq_scene_test_dataset: DatasetForTests,
                         other_dataset: DatasetForTests,
                         csvfile: str):
    """
    Test dea-coherence --check-ancestors command option
    """
    # Source dataset
    ls8_pq_scene_test_dataset.add_to_index()
    other_dataset.add_to_index()

    # Archive parent datasets
    ls8_pq_scene_test_dataset.archive_parent_in_index()
    other_dataset.archive_parent_in_index()

    assert other_dataset.path.exists(), "Other dataset path exists"
    assert ls8_pq_scene_test_dataset.path.exists(), "Test dataset path exists"

    assert ls8_pq_scene_test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

    assert len(ls8_pq_scene_test_dataset.get_parent_index_record().uris) == 0, "Test_ds parent location is not none"
    assert len(ls8_pq_scene_test_dataset.get_parent_index_record().uris) == 0, "Other_ds parent location should none"

    exe_opts = ['--check-ancestors']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)


def test_archive_locationless(ls8_pq_scene_test_dataset: DatasetForTests,
                              other_dataset: DatasetForTests,
                              csvfile: str):
    """
    Test dea-coherence --archive-locationless command option
    """
    # Source dataset
    ls8_pq_scene_test_dataset.add_to_index()
    other_dataset.add_to_index()

    # Archive location
    ls8_pq_scene_test_dataset.remove_location_in_index()
    other_dataset.remove_location_in_index()

    exe_opts = ['--archive-locationless']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)

    assert other_dataset.path.exists(), "Other dataset archived"
    assert ls8_pq_scene_test_dataset.path.exists(), "Test dataset archived"

    assert len(ls8_pq_scene_test_dataset.get_index_record().uris) == 0, "Test dataset location should none"
    assert len(other_dataset.get_index_record().uris) == 0, "Other dataset location should none"

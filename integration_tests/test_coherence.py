import shutil
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


def test_check_locationless(test_dataset: DatasetForTests,
                            other_dataset: DatasetForTests,
                            csvfile: str):
    """
    Test dea-coherence --check-locationless command option
    """
    # Newly archived
    test_dataset.add_to_index()
    test_dataset.archive_location_in_index()

    # Archived a while ago
    other_dataset.add_to_index()
    other_dataset.archive_location_in_index(archived_dt=A_LONG_TIME_AGO)

    # Change the UUID for dataset on disk so that we have a locationless scenario
    shutil.rmtree(test_dataset.copyable_path)
    shutil.copytree(other_dataset.copyable_path, test_dataset.copyable_path)

    assert other_dataset.path.exists(), "Dataset archived long time ago"
    assert test_dataset.path.exists(), "Too-recently-archived dataset"

    assert test_dataset.get_index_record() is not None, "Test dataset should still be in the index"
    assert other_dataset.get_index_record() is not None, "Other dataset should still be in the index"

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {test_dataset.uri, other_dataset.uri}, "Both uri should remain."

    exe_opts = ['--check-locationless']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)


def test_check_downstream_datasets(test_dataset: DatasetForTests,
                                   other_dataset: DatasetForTests,
                                   csvfile: str):
    """
    Test dea-coherence --check-downstream-ds command option
    """
    test_dataset.add_to_index()
    test_dataset.archive_location_in_index()
    other_dataset.add_to_index()

    assert test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {test_dataset.uri, other_dataset.uri}, "Both uri should remain."

    exe_opts = ['--check-downstream']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)


def test_check_ancestors(test_dataset: DatasetForTests,
                         other_dataset: DatasetForTests,
                         csvfile: str):
    """
    Test dea-coherence --check-ancestors command option
    """
    # Source dataset
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    # Archive datasets
    test_dataset.archive_in_index()
    other_dataset.archive_in_index()

    assert other_dataset.path.exists(), "Dataset archived long time ago"
    assert test_dataset.path.exists(), "Too-recently-archived dataset"

    assert test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

    exe_opts = ['--check-ancestors']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)


def test_archive_locationless(test_dataset: DatasetForTests,
                              other_dataset: DatasetForTests,
                              csvfile: str):
    """
    Test dea-coherence --archive-locationless command option
    """
    # Source dataset
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    # Archive location
    test_dataset.archive_location_in_index()
    other_dataset.archive_location_in_index()

    exe_opts = ['--archive-locationless']

    timerange = ["time in 2018"]
    exe_opts.extend(timerange)

    assert_click_command(coherence.main, exe_opts)

    assert other_dataset.path.exists(), "Dataset archived long time ago"
    assert test_dataset.path.exists(), "Too-recently-archived dataset"

    assert test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

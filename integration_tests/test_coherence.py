import shutil
from datetime import datetime, timedelta

from click.testing import CliRunner, Result

from integration_tests.conftest import DatasetForTests
from datacube.api.query import Query
from digitalearthau import coherence

# Default is to clean up older than three days ago.
A_LONG_TIME_AGO = datetime.utcnow() - timedelta(days=4)


def test_locationless(test_dataset: DatasetForTests,
                      other_dataset: DatasetForTests):
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

    query = Query(product='ls7_level1_scene', time=('2018-09-01', '2018-10-31'))

    coherence_args = [
        '--check-locationless',
        query,
    ]

    res: Result = CliRunner().invoke(
        coherence.cli,
        args=['dea-coherence', *coherence_args],
        catch_exceptions=False)

    assert res.exit_code


def test_siblings(test_dataset: DatasetForTests,
                  other_dataset: DatasetForTests):
    """
    Test dea-coherence --check-siblings command option
    """
    # Source dataset
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    # Change the UUID for dataset on disk so that we have a duplicate siblings scenario
    shutil.rmtree(test_dataset.copyable_path)
    shutil.copytree(other_dataset.copyable_path, test_dataset.copyable_path)

    # Index the updated test datasets (siblings)
    other_dataset.add_to_index()

    assert other_dataset.path.exists(), "Dataset archived long time ago"
    assert test_dataset.path.exists(), "Too-recently-archived dataset"

    assert test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {test_dataset.uri, other_dataset.uri}, "Both uri should remain."

    query = Query(product='ls7_level1_scene', time=('2018-09-01', '2018-10-31'))

    coherence_args = [
        '--check-siblings',
        query,
    ]

    res: Result = CliRunner().invoke(
        coherence.cli,
        args=['dea-coherence', *coherence_args],
        catch_exceptions=False)

    assert res.exit_code


def test_downstream_datasets(test_dataset: DatasetForTests,
                             other_dataset: DatasetForTests):
    """
    Test dea-coherence --check-downstream-ds command option
    """
    test_dataset.add_to_index()
    test_dataset.archive_location_in_index()
    other_dataset.add_to_index()
    other_dataset.add_to_index()
    other_dataset.add_to_index()

    assert other_dataset.path.exists(), "Dataset archived long time ago"
    assert test_dataset.path.exists(), "Too-recently-archived dataset"

    assert test_dataset.get_index_record() is not None
    assert other_dataset.get_index_record() is not None

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {test_dataset.uri, other_dataset.uri}, "Both uri should remain."

    query = Query(product='ls7_level1_scene', time=('2018-09-01', '2018-10-31'))

    coherence_args = [
        '--check-downstream-ds',
        query,
    ]

    res: Result = CliRunner().invoke(
        coherence.cli,
        args=['dea-coherence', *coherence_args],
        catch_exceptions=False)

    assert res.exit_code

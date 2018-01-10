from datetime import datetime, timedelta

from click.testing import CliRunner, Result

from digitalearthau import cleanup
from integration_tests.conftest import DatasetForTests

# Default is to clean up older than three days ago.
A_LONG_TIME_AGO = datetime.utcnow() - timedelta(days=4)


def test_cleanup_archived(global_integration_cli_args,
                          test_dataset: DatasetForTests,
                          other_dataset: DatasetForTests,
                          integration_test_data):
    """
    Two archived datasets, one eligible for cleanup
    """
    # Newly archived. Don't do anything.
    test_dataset.add_to_index()
    test_dataset.archive_location_in_index()

    # Archived a while ago. Should be cleaned up.
    other_dataset.add_to_index()
    other_dataset.archive_location_in_index(archived_dt=A_LONG_TIME_AGO)

    # Archive folder
    _call_cleanup(['archived', str(integration_test_data)], global_integration_cli_args)

    assert not other_dataset.path.exists(), "Dataset was not cleaned up"

    assert test_dataset.path.exists(), "Too-recently-archived dataset shouldn't be cleaned up"

    assert other_dataset.get_index_record() is not None, "A cleaned-up dataset should still be in the index"

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {test_dataset.uri}, "Only one uri should remain. The other was trashed."


def test_dont_cleanup(global_integration_cli_args,
                      test_dataset: DatasetForTests,
                      other_dataset: DatasetForTests,
                      integration_test_data):
    """
    Active or unindexed datasets should be left alone.
    """
    # Still active. Don't clean up!
    test_dataset.add_to_index()

    # Not indexed. Don't clean up!
    # other_dataset

    _call_cleanup(['archived', str(integration_test_data)], global_integration_cli_args)

    assert other_dataset.path.exists(), "Dataset shouldn't be touched"

    assert test_dataset.path.exists(), "Unindexed dataset shouldn't be touched"

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())

    # All still there
    assert all_indexed_uris == {test_dataset.uri}


def test_keep_stacks(global_integration_cli_args,
                     test_dataset: DatasetForTests,
                     other_dataset: DatasetForTests,
                     integration_test_data):
    """
    Two datasets pointing to the same location should only be cleaned up when both are archived.
    """
    # One archived
    test_dataset.add_to_index()
    test_dataset.archive_location_in_index(archived_dt=A_LONG_TIME_AGO)

    other_dataset.add_to_index()
    # A second location
    other_dataset.add_location(test_dataset.uri)
    # Archive the first location.
    other_dataset.archive_location_in_index(archived_dt=A_LONG_TIME_AGO)

    _call_cleanup(['archived', str(integration_test_data)], global_integration_cli_args)

    # Both datasets point to this path, but the second is still active. Don't clean up.
    assert test_dataset.path.exists(), "Only clean-up a stack if all of it is archived"

    # The dataset had two locations, this one was archived and should have been cleaned up.
    assert not other_dataset.path.exists(), "Archived location was not cleaned up"

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {test_dataset.uri}, "Only one uri should remain. The other was trashed."


def _call_cleanup(args, global_integration_cli_args) -> Result:
    # We'll call the cli interface itself, rather than the python api, to get wider coverage in our test.
    res: Result = CliRunner().invoke(
        cleanup.cli,
        global_integration_cli_args + [str(arg) for arg in args],
        catch_exceptions=False
    )

    assert res.exit_code == 0, res.output
    print(res.output)
    return res

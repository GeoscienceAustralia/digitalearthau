import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from digitalearthau import move, paths
from integration_tests.conftest import DatasetForTests, as_map


@pytest.fixture
def destination_path(tmpdir) -> Path:
    """A new "base folder" that a dataset could be moved to."""

    destination = Path(tmpdir).joinpath('destination_collection')
    destination.mkdir(exist_ok=False)
    paths.register_base_directory(destination)
    return destination


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

    res: Result = _call_move(
        # Move one path to destination_path
        ['-d', str(destination_path), str(test_dataset.path)],

        global_integration_cli_args
    )

    _check_successful_move(test_dataset, expected_new_path, other_dataset, res)


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

    res = _call_move(
        # Move one path to destination_path
        ['-d', str(destination_path), str(test_dataset.path)],

        global_integration_cli_args
    )

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

    original_index = as_map(test_dataset.collection.index_)
    res = _call_move(
        # Move one path to destination_path
        ['-d', str(destination_path), str(test_dataset.path)],

        global_integration_cli_args
    )

    # Move script should have completed, but dataset should have been skipped.
    assert res.exit_code == 0, res.output
    print(res.output)

    now_index = as_map(test_dataset.collection.index_)

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
    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
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
        [*global_integration_cli_args, *args],
        catch_exceptions=False
    )
    return res

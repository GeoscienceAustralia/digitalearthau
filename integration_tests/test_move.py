from pathlib import Path

from click.testing import CliRunner, Result

from digitalearthau import move, paths
from integration_tests.conftest import DatasetForTests


def test_move(global_integration_cli_args,
              test_dataset: DatasetForTests,
              other_dataset: DatasetForTests,
              tmpdir):
    """
    With two datasets in a collection, try to move one to a new disk location.

    One should be unchanged, the other should be moved.
    """
    test_dataset.add_to_index()
    other_dataset.add_to_index()

    destination = Path(tmpdir).joinpath('destination_collection')
    destination.mkdir(exist_ok=False)

    paths.register_base_directory(destination)

    res = _call_move(
        # Move one path to destination
        ['-d', str(destination), str(test_dataset.path)],

        global_integration_cli_args
    )

    print(res.output)
    assert res.exit_code == 0, res.output

    assert destination.exists(), "File was not moved to destination?"
    assert test_dataset.path.exists(), "Old file was deleted: should only have been archived"

    # Expect a destination with the same offset (eg "year/LS8_SOMETHING_AROTHER") but in our new base folder.
    expected_new_path = destination.joinpath(*test_dataset.path_offset)
    expected_new_uri = expected_new_path.as_uri()

    # All three uris should be in the system still.
    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    assert all_indexed_uris == {
        # The old uri one still indexed (but archived)
        test_dataset.uri,
        # The new uri
        expected_new_uri,
        # The other dataset unchanged
        other_dataset.uri
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

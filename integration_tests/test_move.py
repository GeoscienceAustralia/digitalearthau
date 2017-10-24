from pathlib import Path

from click.testing import CliRunner, Result

from digitalearthau import move, collections, paths
from digitalearthau.collections import Collection
from digitalearthau.index import add_dataset
from integration_tests.conftest import DatasetOnDisk

import pytest
import structlog
from dateutil import tz


def test_move(global_integration_cli_args, test_dataset: DatasetOnDisk, other_dataset: DatasetOnDisk, tmpdir):
    """
    With two datasets in a collection, try to move one.
    """

    # Add second dataset
    add_dataset(test_dataset.collection.index, test_dataset.dataset.id, test_dataset.uri)
    add_dataset(other_dataset.collection.index, other_dataset.dataset.id, other_dataset.uri)

    # Move one dataset.

    runner = CliRunner()

    destination = Path(tmpdir).joinpath('destination_collection')
    destination.mkdir(exist_ok=False)

    paths.register_base_directory(destination)

    # dest_collection = Collection(
    #     name='ls8_scene_dest',
    #     query={},
    #
    #     index=test_dataset.collection.index
    # )
    # collections._add(dest_collection)

    res: Result = runner.invoke(
        move.cli,
        [*global_integration_cli_args, '-d', str(destination), str(test_dataset.path)],
        catch_exceptions=False
    )

    print(res.output)
    assert res.exit_code == 0, res.output

    assert destination.exists(), "File was not moved to destination?"
    assert test_dataset.path.exists(), "Old file was deleted: should only have been archived"

    # Only the destination should be active now, so only it will be returned in search

    all_indexed_uris = set(test_dataset.collection.iter_index_uris())
    # The 'other' dataset should be unchanged.
    expected_new_path = destination.joinpath(*test_dataset.path_offset)
    assert all_indexed_uris == {
        # The old uri one still indexed (but archived)
        test_dataset.uri,
        # The new uri
        expected_new_path.as_uri(),
        # The other dataset unchanged
        other_dataset.uri
    }

    dataset = test_dataset.collection.index.get(test_dataset.dataset.id)

    test_dataset.collection.iter_index_uris()

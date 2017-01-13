from . import utils


def test_list_file_paths():
    d = utils.write_files({
        "file1.txt": 'test',
        'dir1': {
            'file2.txt': 'test'
        }
    })

    listed_files = set(utils.list_file_paths(d))

    assert listed_files == {
        d.joinpath('file1.txt'),
        d.joinpath('dir1', 'file2.txt'),
    }

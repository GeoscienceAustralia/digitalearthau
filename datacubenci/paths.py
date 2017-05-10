import atexit

import datetime
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import List, Iterable, Union

from datacube.utils import is_supported_document_type, read_documents, InvalidDocException

# This may eventually go to a config file...
# ".trash" directories will be created at this level for any datasets contained within.
_BASE_DIRECTORIES = [
    '/g/data/fk4/datacube',
    '/g/data/rs0/datacube',
    '/g/data/v10/reprocess',
    '/g/data/rs0/scenes/pq-scenes-tmp',
    '/g/data/rs0/scenes/nbar-scenes-tmp',
]

# Use a static variable so that trashed items in the same run will be in the same trash bin.
_TRASH_DAY = datetime.datetime.utcnow().strftime('%Y-%m-%d')


def register_base_directory(d: Union[str, Path]):
    _BASE_DIRECTORIES.append(str(d))


def get_trash_path(file_path):
    """
    For a given path on lustre, get the full path to a destination trash path.

    >>> trash_path = str(get_trash_path('/g/data/fk4/datacube/ls7/2003/something.nc'))
    >>> trash_path == '/g/data/fk4/datacube/.trash/{day}/ls7/2003/something.nc'.format(day=_TRASH_DAY)
    True
    >>> get_trash_path('/short/unknown_location/something.nc')
    Traceback (most recent call last):
    ...
    ValueError: Unknown location: can't calculate base directory: /short/unknown_location/something.nc
    """
    root_path, dir_offset = split_path_from_base(file_path)

    # A trash subfolder for each day.
    return root_path.joinpath('.trash', _TRASH_DAY, dir_offset)


def split_path_from_base(file_path):
    """
    Split a dataset path into base directory and offset.

    :type file_path: pathlib.Path | str
    :rtype: (pathlib.Path, str)

    >>> base, offset = split_path_from_base('/g/data/fk4/datacube/ls7/2003/something.nc')
    >>> str(base)
    '/g/data/fk4/datacube'
    >>> offset
    'ls7/2003/something.nc'
    >>> split_path_from_base('/short/unknown_location/something.nc')
    Traceback (most recent call last):
    ...
    ValueError: Unknown location: can't calculate base directory: /short/unknown_location/something.nc
    """

    for root_location in _BASE_DIRECTORIES:
        if str(file_path).startswith(root_location):
            dir_offset = str(file_path)[len(root_location) + 1:]
            return Path(root_location), dir_offset

    raise ValueError("Unknown location: can't calculate base directory: " + str(file_path))


def write_files(file_dict, containing_dir=None):
    """
    Convenience method for writing a tree of files to a temporary directory.

    (primarily indended for use in tests)

    Dict format is "filename": "text content"

    If content is another dict, it is created recursively in the same manner.

    write_files({'test.txt': 'contents of text file'})

    :param containing_dir: Optionally specify the directory to add the files to.
    :type file_dict: dict
    :rtype: pathlib.Path
    :return: Created temporary directory path
    """
    if not containing_dir:
        containing_dir = Path(tempfile.mkdtemp(suffix='neotestrun'))

    _write_files_to_dir(str(containing_dir), file_dict)

    def remove_if_exists(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    atexit.register(remove_if_exists, str(containing_dir))
    return containing_dir


def _write_files_to_dir(directory_path, file_dict):
    """
    Convenience method for writing a tree of files to a given directory.

    (primarily indended for use in tests)

    :type directory_path: str
    :type file_dict: dict
    """
    for filename, contents in file_dict.items():
        path = os.path.join(directory_path, filename)
        if isinstance(contents, dict):
            os.mkdir(path)
            _write_files_to_dir(path, contents)
        else:
            with open(path, 'w') as f:
                if isinstance(contents, list):
                    f.writelines(contents)
                elif isinstance(contents, str):
                    f.write(contents)
                else:
                    raise Exception('Unexpected file contents: %s' % type(contents))


def list_file_paths(path):
    """
    Build a list of files in the given path
    """
    output = []
    for directory, _, files in os.walk(str(path)):
        output.extend(Path(directory).joinpath(file_) for file_ in files)
    return output


def get_path_dataset_id(metadata_path: Path) -> uuid.UUID:
    """
    Get the dataset id embedded by the given path. Die if there are multiple.
    :param metadata_path:
    :return:
    """
    ids = get_path_dataset_ids(metadata_path)
    if len(ids) != 1:
        raise ValueError("Only single-document metadata files are currently supported for moving. "
                         "Found {} in {}".format(len(ids), metadata_path))

    return ids[0]


def _path_dataset_ids(path: Path) -> Iterable[uuid.UUID]:
    for _, metadata_doc in read_documents(path):
        if metadata_doc is None:
            raise InvalidDocException("Empty document from path {}".format(path))

        if 'id' not in metadata_doc:
            raise InvalidDocException("No id in path metadata: {}".format(path))

        yield uuid.UUID(metadata_doc['id'])


def get_path_dataset_ids(path: Path) -> List[uuid.UUID]:
    """
    Get all dataset ids embedded by the given path.

    (Either a standalone metadata file or embedded in a given NetCDF)

    :raises InvalidDocException
    """
    return list(_path_dataset_ids(path))


def get_dataset_paths(metadata_path):
    """
    Get the base location and all files for a given dataset (specified by the metadata path)
    :param metadata_path:
    :return: (base_path, all_files)
    """
    if metadata_path.suffix == '.nc':
        return metadata_path, [metadata_path]
    if metadata_path.name == 'ga-metadata.yaml':
        return metadata_path.parent, list_file_paths(metadata_path.parent)

    sibling_suffix = '.ga-md.yaml'
    if metadata_path.name.endswith(sibling_suffix):
        data_file = metadata_path.parent.joinpath(metadata_path.name[:-len(sibling_suffix)])
        return data_file, [metadata_path, data_file]

    raise ValueError("Unsupported path type: " + str(metadata_path))


def get_metadata_path(dataset_path):
    """
    Find a metadata path for a given input/dataset path.

    :type dataset_path: pathlib.Path
    :rtype: Path
    """

    # They may have given us a metadata file directly.
    if dataset_path.is_file() and (is_supported_document_type(dataset_path) or dataset_path.suffix == '.nc'):
        return dataset_path

    # Otherwise there may be a sibling file with appended suffix '.agdc-md.yaml'.
    expected_name = dataset_path.parent.joinpath('{}.ga-md'.format(dataset_path.name))
    found = _find_any_metadata_suffix(expected_name)
    if found:
        return found

    # Otherwise if it's a directory, there may be an 'agdc-metadata.yaml' file describing all contained datasets.
    if dataset_path.is_dir():
        expected_name = dataset_path.joinpath('ga-metadata')
        found = _find_any_metadata_suffix(expected_name)
        if found:
            return found

    raise ValueError('No metadata found for input %r' % dataset_path)


def _find_any_metadata_suffix(path):
    """
    Find any supported metadata files that exist with the given file path stem.
    (supported suffixes are tried on the name)

    Eg. searcing for '/tmp/ga-metadata' will find if any files such as '/tmp/ga-metadata.yaml' or
    '/tmp/ga-metadata.json', or '/tmp/ga-metadata.yaml.gz' etc that exist: any suffix supported by read_documents()

    :type path: pathlib.Path
    """
    existing_paths = list(filter(is_supported_document_type, path.parent.glob(path.name + '*')))
    if not existing_paths:
        return None

    if len(existing_paths) > 1:
        raise ValueError('Multiple matched metadata files: {!r}'.format(existing_paths))

    return existing_paths[0]

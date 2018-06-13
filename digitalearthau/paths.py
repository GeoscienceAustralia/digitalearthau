import atexit
import datetime
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import List, Iterable, Union, Tuple

import pathlib
import structlog
import logging

from datacube.utils import is_supported_document_type, read_documents, InvalidDocException, uri_to_local_path

_LOG = structlog.getLogger()
_LOG1 = logging.getLogger(__file__)

# This check is buggy when used with Tuple[] type: https://github.com/PyCQA/pylint/issues/867
# pylint: disable=invalid-sequence-index

# This may eventually go to a config file...
# ".trash" directories will be created at this level for any datasets contained within.
# TODO: Could these be inferred from the collections paths?
BASE_DIRECTORIES = [
    '/g/data/fk4/datacube',
    '/g/data/rs0/datacube',
    '/g/data/v10/reprocess',
    '/g/data/rs0/scenes',
    '/short/v10/scenes',
    '/g/data/v10/public/data',
]

# Use a static variable so that trashed items in the same run will be in the same trash bin.
_TRASH_DAY = datetime.datetime.utcnow().strftime('%Y-%m-%d')

# TODO: configurable?
NCI_WORK_ROOT = Path(os.environ.get('DEA_WORK_ROOT') or '/g/data/v10/work')
# Structure for work directories within the work root.
# Eg. '/g/data/v10/work/ls8_nbar_albers/create/2017-10/09-2102'
_JOB_WORK_OFFSET = '{output_product}/{task_type}/{work_time:%Y-%m}/{work_time:%d-%H%M%S}'


def register_base_directory(d: Union[str, Path]):
    BASE_DIRECTORIES.append(str(d))


def is_base_directory(d: Path):
    """
    >>> is_base_directory(Path("/g/data/rs0/datacube"))
    True
    >>> is_base_directory(Path("/tmp/something"))
    False
    """
    return str(d) in BASE_DIRECTORIES


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


def get_original_path(trashed_file_path):
    """
    For a given path in the trash, get the original pre-trash path.

    >>> str(get_original_path('/g/data/fk4/datacube/.trash/20170823/ls7/2003/something.nc'))
    '/g/data/fk4/datacube/ls7/2003/something.nc'
    >>> # Old trash structure (no second-level date folder)
    >>> str(get_original_path('/g/data/fk4/datacube/.trash-20170823/ls7/2003/something.nc'))
    '/g/data/fk4/datacube/ls7/2003/something.nc'
    >>> get_original_path('/g/data/fk4/datacube/ls7/2003/something.nc')
    Traceback (most recent call last):
    ...
    ValueError: Not a trashed location: '/g/data/fk4/datacube/ls7/2003/something.nc'
    """
    root_path, dir_offset = split_path_from_base(trashed_file_path)
    dir_offsets = dir_offset.split('/')
    # Old style, trash folder has date '.trash-YYYYMMDD/'
    if dir_offset.startswith('.trash-'):
        return root_path.joinpath(*dir_offsets[1:])
    # New style, second-level folder with date '.trash/YYYYMMDD/'
    elif dir_offset.startswith('.trash/'):
        return root_path.joinpath(*dir_offsets[2:])
    else:
        raise ValueError("Not a trashed location: %r" % str(trashed_file_path))


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

    for root_location in BASE_DIRECTORIES:
        if str(file_path).startswith(root_location):
            dir_offset = str(file_path)[len(root_location) + 1:]
            return Path(root_location), dir_offset

    raise ValueError("Unknown location: can't calculate base directory: " + str(file_path))


def write_files(files_spec, containing_dir=None):
    """
    Convenience method for writing a tree of files to a temporary directory.

    (primarily indended for use in tests)

    Dict format is "filename": "text content"

    If content is another dict, it is created recursively in the same manner.

    write_files({'test.txt': 'contents of text file'})

    :param containing_dir: Optionally specify the directory to add the files to,
                           otherwise a temporary directory will be created.
    :type files_spec: dict
    :rtype: pathlib.Path
    :return: Created temporary directory path
    """
    if not containing_dir:
        containing_dir = Path(tempfile.mkdtemp(suffix='neotestrun'))

    _write_files_to_dir(containing_dir, files_spec)

    def remove_if_exists(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    atexit.register(remove_if_exists, containing_dir)
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


def get_dataset_paths(metadata_path: Path) -> Tuple[Path, List[Path]]:
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


def read_document(path: Path) -> dict:
    """
    Read and parse exactly one document.
    """
    ds = list(read_documents(path))
    if len(ds) != 1:
        raise NotImplementedError("Expected one document to be in path %s" % path)

    _, doc = ds[0]
    return doc


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


def trash_uri(uri: str, dry_run=False, log=_LOG) -> bool:
    local_path = uri_to_local_path(uri)

    if not local_path.exists():
        log.warning("trash.not_exist", path=local_path)
        return False

    # TODO: to handle sibling-metadata we should trash "all_dataset_paths" too.
    base_path, all_dataset_files = get_dataset_paths(local_path)

    trash_path = get_trash_path(base_path)

    log.info("trashing", base_path=base_path, trash_path=trash_path)

    if not dry_run:
        if not trash_path.parent.exists():
            os.makedirs(str(trash_path.parent))

        if trash_path.parent.exists():
            os.rename(str(base_path), str(trash_path))

    return True


def get_product_work_directory(
        output_product: str,
        time=datetime.datetime.utcnow(),
        task_type='create',
):
    """Get an NCI work directory for processing the given product.

    :param time: A timestamp for roughly when your job happened (or was submitted)
    :param task_type: Informally the kind of work you're doing on the product: create, sync, archive, ...
    """
    if not NCI_WORK_ROOT.exists():
        _LOG1.info('Create new NCI_WORK_ROOT directory.')
        os.makedirs(NCI_WORK_ROOT)

    d = _make_work_directory(output_product, time, task_type)
    d.mkdir(parents=True, exist_ok=False)
    return d


def _make_work_directory(output_product, work_time, task_type):
    """
    >>> t = datetime.datetime.utcfromtimestamp(1507582964.90336)
    >>> _make_work_directory('ls8_nbar_albers', t, 'create')
    PosixPath('/g/data/v10/work/ls8_nbar_albers/create/2017-10/09-210244')
    >>> _make_work_directory('ls8_level1_scene', t, 'sync')
    PosixPath('/g/data/v10/work/ls8_level1_scene/sync/2017-10/09-210244')
    """
    job_offset = _JOB_WORK_OFFSET.format(
        work_time=work_time,
        task_type=task_type,
        output_product=output_product,
        request_uuid=uuid.uuid4()
    )
    return NCI_WORK_ROOT.joinpath(job_offset)

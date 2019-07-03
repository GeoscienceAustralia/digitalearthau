import os.path
import xarray
import yaml
from boltons import fileutils
from pandas import to_datetime
from pathlib import Path
from typing import Iterable, Union

from datacube.helpers import write_geotiff


def calc_uris(file_path, variable_params):
    base, ext = os.path.splitext(file_path)

    if ext == '.tif':
        # the file_path value used is highly coupled to
        # dataset_to_geotif_yaml since it's assuming the
        # yaml file is in the same dir as the tif file
        abs_paths, rel_files, yml = tif_filenames(file_path, variable_params.keys())
        uri = yml.as_uri()
        band_uris = {band: {'path': uri, 'layer': band} for band, uri in rel_files.items()}
        if all_files_exist(abs_paths.values()):
            raise FileExistsError('All output files already exist ', str(list(rel_files.values())))
    else:
        band_uris = None
        uri = file_path.absolute().as_uri()
        if file_path.exists():
            raise FileExistsError('Output file already exists', str(file_path))


def all_files_exist(filenames: Iterable):
    """
    Return True if all files in a list exist.

    :param filenames: A list of file paths.
    :return:
    """
    isthere = (os.path.isfile(i) for i in filenames)
    return all(isthere)


def tif_filenames(filename: Union[Path, str], bands: list, sep='_'):
    """
    Turn one file name into several file names, one per band.
    This turns a .tif filename into two dictionaries of filenames,
    For abs and rel the band as the key, with the band inserted into the file names.
        i.e ls8_fc.tif -> ls8_fc_BS.tif  (Last underscore is separator)
    The paths in abs_paths are absolute
    The paths in rel_files are relative to the yml
    yml is the path location to where the yml file will be written

    :param filename: a Path.
    :param bands: a list of bands/measurements
    :param sep: the separator between the base name and the band.
    :return: (abs_paths, rel_files, yml)
    """
    base, ext = os.path.splitext(filename)
    assert ext == '.tif'
    yml = Path(base + '.yml').absolute()
    abs_paths = {}
    rel_files = {}
    for band in bands:
        build = Path(base + sep + band + ext)
        abs_paths[band] = build.absolute().as_uri()
        # This is to get relative paths
        rel_files[band] = os.path.basename(build)
    return abs_paths, rel_files, yml


def dataset_to_geotif_yaml(dataset: xarray.Dataset,
                           odc_dataset: Dataset,
                           filename: Union[Path, str],
                           variable_params=None):
    """
    Write the dataset out as a set of geotifs with metadata in a yaml file.
    There will be one geotiff file per band.
    The band name is added into the file name.
    i.e ls8_fc.tif -> ls8_fc_BS.tif

    :param dataset:
    :param filename: Output filename
    :param variable_params: dict of variable_name: {param_name: param_value, [...]}
                            Used to get band names.

    """

    bands = variable_params.keys()
    abs_paths, _, yml = tif_filenames(filename, bands)

    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    # Write out the yaml file
    with fileutils.atomic_save(str(yml)) as stream:
        yaml.safe_dump(odc_dataset.metadata_doc, stream, encoding='utf8')

    # Iterate over the bands
    for key, bandfile in abs_paths.items():
        slim_dataset = dataset[[key]]  # create a one band dataset
        attrs = slim_dataset[key].attrs.copy()  # To get nodata in
        del attrs['crs']  # It's  format is poor
        del attrs['units']  # It's  format is poor
        slim_dataset[key] = dataset.data_vars[key].astype('int16', copy=True)
        write_geotiff(bandfile, slim_dataset.isel(time=0), profile_override=attrs)


def _get_filename(config, sources):
    region_code = getattr(sources.metadata, 'region_code', None)

    # data collection upgrade format
    start_time = to_datetime(sources.time.begin).strftime('%Y%m%d%H%M%S%f')
    end_time = to_datetime(sources.time.end).strftime('%Y%m%d%H%M%S%f')
    epoch_start = to_datetime(sources.time.begin)
    epoch_end = to_datetime(sources.time.begin)

    tile_index = None

    interp = dict(
        tile_index=tile_index,
        region_code=region_code,
        start_time=start_time,
        end_time=end_time,
        epoch_start=epoch_start,
        epoch_end=epoch_end,
        version=config.get('task_timestamp'))

    file_path_template = str(Path(config['location'], config['file_path_template']))
    filename = file_path_template.format(**interp)
    return filename

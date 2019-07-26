"""


"""

import importlib
import sys
# pylint: disable=map-builtin-not-iterating
from datetime import datetime
from pathlib import Path
from typing import Sequence, Iterable, Mapping, Type, Optional, List, Any

import attr
import cattr
import numpy as np
import structlog
import yaml

import datacube
from datacube.model import Dataset
from datacube.testutils.io import native_load
from datacube.virtual import Transformation
from eodatasets3.assemble import DatasetAssembler
from eodatasets3.model import DatasetDoc, ProductDoc
from eodatasets3.properties import StacPropertyView
from ._dask import dask_compute_stream

_LOG = structlog.get_logger()

cattr.register_structure_hook(np.dtype, np.dtype)


@attr.s(auto_attribs=True)
class OutputSettings:
    location: str = attr.ib(converter=Path)
    dtype: np.dtype = attr.ib(converter=np.dtype)
    nodata: str  # type depends on dtype
    preview_image: Optional[List[str]]
    metadata: Mapping[str, str]


@attr.s(auto_attribs=True)
class ProcessingSettings:
    dask_chunks: Mapping[str, int]


@attr.s(auto_attribs=True)
class D2DSettings:
    measurements: Sequence[str]
    measurement_renames: Mapping[str, str]
    transform: str
    transform_args: Any
    output: OutputSettings
    processing: ProcessingSettings


@attr.s(auto_attribs=True)
class D2DTask:
    dataset: Dataset
    settings: D2DSettings


class Dataset2Dataset:
    def __init__(self, *, config=None, config_file=None, dc_env=None):
        if config is not None:
            self.config = config
        else:
            self.config = config = yaml.safe_load(Path(config_file).read_bytes())

        # Connect to the ODC Index
        self.dc = datacube.Datacube(env=dc_env)
        self.input_product_name = self.config['specification']['product']
        self.input_product = self.dc.index.products.get_by_name(self.input_product_name)

    def generate_tasks(self, limit=None) -> Iterable[D2DTask]:
        # Find which datasets needs to be processed
        datasets = self.dc.index.datasets.search(limit=limit, product=self.input_product_name)

        tasks = (self.generate_task(ds) for ds in datasets)

        return tasks

    def generate_task(self, dataset) -> D2DTask:
        return D2DTask(dataset=dataset,
                       measurements=self.config['specification']['measurements'],
                       renames=self.config['specification']['measurement_renames'],
                       transform=self.config['specification']['transform'],
                       destination_path=Path(self.config['output']['location']),
                       metadata=self.config['metadata'])


def execute_with_dask(tasks: Iterable[D2DTask]):
    # Execute the tasks across the dask cluster
    from dask.distributed import Client
    client = Client()
    _LOG.info('started dask', dask_client=client)
    completed = dask_compute_stream(client,
                                    execute_task,
                                    tasks)
    _LOG.info('processing task stream')
    for result in completed:
        try:
            print(result)
        except Exception as e:
            print(e)
            print(sys.exc_info()[0])
        pass
    _LOG.info('completed')


def execute_task(task: D2DTask):
    log = _LOG.bind(task=task)
    transform = _import_transform(task.transform)
    transform = transform()

    # Load and process data
    data = native_load(task.dataset, measurements=task.measurements, dask_chunks={'x': 1000, 'y': 1000})
    data = data.rename(task.renames)

    log.info('data loaded')

    output_data = transform.compute(data)
    if 'time' in output_data.dims:
        output_data = output_data.squeeze('time')

    log.info('prepared lazy transformation', output_data=output_data)

    output_data = output_data.compute()
    crs = data.attrs['crs']

    del data

    dtypes = set(str(v.dtype) for v in output_data.data_vars.values())
    if 'int8' in dtypes:
        output_data = output_data.astype('uint8', copy=False)

    if 'crs' not in output_data.attrs:
        output_data.attrs['crs'] = crs
    source_doc = _convert_old_odc_dataset_to_new(task.dataset)

    # Ensure output path exists
    task.output.location.mkdir(parents=True, exist_ok=True)

    with DatasetAssembler(task.output.location, naming_conventions="dea") as p:
        p.add_source_dataset(source_doc, auto_inherit_properties=True)

        for k, v in task.metadata.items():
            setattr(p, k, v)
        p.properties['dea:dataset_maturity'] = 'interim'

        p.processed = datetime.utcnow()

        p.note_software_version(
            'd2dtransformer',
            "https://github.com/GeoscienceAustralia/digitalearthau",
            "0.1.0"
        )

        p.write_measurements_odc_xarray(
            output_data,
            nodata=255
        )
        dataset_id, metadata_path = p.done()

    return dataset_id, metadata_path


def _convert_old_odc_dataset_to_new(ds: Dataset) -> DatasetDoc:
    product = ProductDoc(name=ds.type.name)
    properties = StacPropertyView(ds.metadata_doc['properties'])
    return DatasetDoc(
        id=ds.id,
        product=product,
        crs=ds.crs.crs_str,
        properties=properties

    )


def _import_transform(transform_name: str) -> Type[Transformation]:
    module_name, class_name = transform_name.rsplit('.', maxsplit=1)
    module = importlib.import_module(name=module_name)
    imported_class = getattr(module, class_name)
    assert isinstance(imported_class, Transformation)
    return imported_class

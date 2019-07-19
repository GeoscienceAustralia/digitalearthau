"""

TODO:
- [ ] Logging
- [ ] Error Handling

"""

# pylint: disable=map-builtin-not-iterating
import importlib
import sys
from pathlib import Path
from typing import Sequence, NamedTuple, Iterable, Mapping

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


class NonVPTask(NamedTuple):
    dataset: Dataset
    measurements: Sequence[str]
    renames: Mapping[str, str]
    transform: str
    destination_path: Path
    metadata: Mapping[str, str]


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

    def generate_tasks(self, limit=3) -> Iterable[NonVPTask]:
        # Find which datasets needs to be processed
        datasets = self.dc.index.datasets.search(limit=limit, product=self.input_product_name)

        tasks = (self.generate_task(ds) for ds in datasets)

        return tasks

    def generate_task(self, dataset) -> NonVPTask:
        return NonVPTask(dataset=dataset,
                         measurements=self.config['specification']['measurements'],
                         renames=self.config['specification']['measurement_renames'],
                         transform=self.config['specification']['transform'],
                         destination_path=Path(self.config['file_output']['location']),
                         metadata=self.config['metadata'])


def execute_with_dask(tasks: Iterable[NonVPTask]):
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


def execute_task(task: NonVPTask):
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

    log.info('processed transform', output_data=output_data)

    output_data = output_data.compute()

    dtypes = set(str(v.dtype) for v in output_data.data_vars.values())
    if 'int8' in dtypes:
        output_data = output_data.astype('uint8', copy=False)

    from datetime import datetime
    source_doc = convert_old_odc_dataset_to_new(task.dataset)

    with DatasetAssembler(task.destination_path, naming_conventions="dea") as p:
        p.add_source_dataset(source_doc, auto_inherit_properties=True)

        for k, v in task.metadata.items():
            setattr(p, k, v)
        p.properties['dea:dataset_maturity'] = 'interim'

        p.processed = datetime.now()

        p.note_software_version(
            'd4worker_nonvp',
            "https://github.com/GeoscienceAustralia/digitalearthau",
            "0.1.0"
        )

        p.write_measurements_odc_xarray(output_data)
        dataset_id, metadata_path = p.done()

    return dataset_id, metadata_path


def convert_old_odc_dataset_to_new(ds: Dataset) -> DatasetDoc:
    product = ProductDoc(name=ds.type.name)
    properties = StacPropertyView(ds.metadata_doc['properties'])
    return DatasetDoc(
        id=ds.id,
        product=product,
        crs=ds.crs.crs_str,
        properties=properties

    )


def _import_transform(transform_name: str) -> Transformation:
    module_name, class_name = transform_name.rsplit('.', maxsplit=1)
    module = importlib.import_module(name=module_name)
    return getattr(module, class_name)

# Thanks http://fhs.github.io/pyhdf/modules/SD.html#programming-models
import lzma
import shutil
from pathlib import Path

import click
import numpy as np
from pyhdf.SD import SD, SDC


class PathlibPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


@click.command()
@click.argument('in_file_name', type=click.Path(exists=True, dir_okay=False, readable=True))
@click.argument('out_file_name', type=click.Path(exists=False, writable=True))
def shrink_inplace(in_file_name, out_file_name):
    """
    Shrink a HDF4 File

    Replace all data values with 0, and then compress with LZMA.

    Useful for generating tiny test data files that still contain all the metadata.
    """
    shutil.copy(in_file_name, out_file_name)

    in_file = SD(out_file_name, SDC.WRITE)

    for dataset_name, dataset_def in in_file.datasets().items():
        coord_axis, shape, dataset_type, index = dataset_def
        print(dataset_name, dataset_def)

        dataset = in_file.select(dataset_name)

        print(dataset[:])
        dataset[:] = np.zeros(shape, dataset.get().dtype)

        dataset.endaccess()

    in_file.end()

    with open(out_file_name, 'rb') as fin, lzma.open(out_file_name + '.xz', 'wb') as fout:
        fout.write(fin.read())


if __name__ == '__main__':
    shrink_inplace()

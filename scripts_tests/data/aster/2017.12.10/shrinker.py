# Thanks http://fhs.github.io/pyhdf/modules/SD.html#programming-models

from pyhdf.SD import SD, SDC

IN_FILE = 'test.hdf'
OUT_FILE = 'shrunk.hdf'

in_file = SD(IN_FILE, SDC.READ)

out_file = SD(OUT_FILE, SDC.CREATE | SDC.WRITE)

# Copy Global Attributes

for key, (value, index, data_type, length) in in_file.attributes().items():
    new_attr = out_file.attr(key)
    new_attr.set(data_type, value)

# Copy Datasets

for dataset_name, dataset_def in in_file.datasets().items():
    coord_axis, shape, dataset_type, index = dataset_def
    print(key, dataset_def)
    dataset = in_file.select(dataset)

    # Copy Dimensions

    # Copy Attributes

    # Copy Data


from uuid import UUID

from digitalearthau.index import DatasetLite
from digitalearthau.paths import write_files
from digitalearthau.sync.differences import DatasetNotIndexed, Mismatch, ArchivedDatasetOnDisk, UnreadableDataset, \
    mismatches_from_file


def test_load_dump_mismatch():
    mismatch = DatasetNotIndexed(
        DatasetLite(UUID("c98c3f2e-add7-4b34-9c9f-2cb8c7f806d2")),
        uri='file:///g/data/fk4/datacube/002/LS5_TM_FC/-17_-31/LS5_TM_FC_3577_-17_-31_19920722013931500000.nc'
    )
    row = mismatch.to_dict()
    assert row == {
        'name': 'dataset_not_indexed',
        'dataset_id': 'c98c3f2e-add7-4b34-9c9f-2cb8c7f806d2',
        'uri': 'file:///g/data/fk4/datacube/002/LS5_TM_FC/-17_-31/LS5_TM_FC_3577_-17_-31_19920722013931500000.nc',
    }

    deserialised_mismatch = Mismatch.from_dict(row)
    assert deserialised_mismatch == mismatch
    assert deserialised_mismatch.__dict__ == mismatch.__dict__


def test_load_from_file():
    root = write_files({
        'outputs.jsonl': """
{"name":"archived_dataset_on_disk","dataset_id":"582e9a74-d343-42d2-9105-a248b4b04f4a",\
"uri":"file:///g/data/fk4/datacube/002/LS5_TM_FC/-10_-39/LS5_TM_FC_3577_-10_-39_19990918011811500000.nc"}
{"name":"unreadable_dataset", "dataset_id":"None","uri":\
"file:///g/data/fk4/datacube/002/LS5_TM_FC/0_-30/LS5_TM_FC_3577_0_-30_20080331005819500000.nc"}
        """
    })

    mismatches = list(mismatches_from_file(root.joinpath('outputs.jsonl')))

    assert mismatches == [
        ArchivedDatasetOnDisk(
            DatasetLite(UUID('582e9a74-d343-42d2-9105-a248b4b04f4a')),
            'file:///g/data/fk4/datacube/002/LS5_TM_FC/-10_-39/LS5_TM_FC_3577_-10_-39_19990918011811500000.nc'
        ),
        UnreadableDataset(
            None,
            'file:///g/data/fk4/datacube/002/LS5_TM_FC/0_-30/LS5_TM_FC_3577_0_-30_20080331005819500000.nc'
        )
    ]

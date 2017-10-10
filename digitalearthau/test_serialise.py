import pathlib

import datetime
from typing import NamedTuple, List

from digitalearthau import serialise


def test_roundtrip():
    # Convert to dict and back again, checking that it's identical.

    class EmbeddedObj(NamedTuple):
        a: str
        my_dt: datetime.datetime

    class ObjA(NamedTuple):
        v1: str
        v2: List[int]
        mynamedtuple: EmbeddedObj
        my_path: pathlib.Path

    m = ObjA(
        "a string",
        [1, 2, 3],
        EmbeddedObj(
            "b string",
            datetime.datetime.utcnow()
        ),
        pathlib.Path("/tmp/test")
    )

    d = serialise.type_to_dict(m)
    print(repr(d))
    new_m = serialise.dict_to_type(d, ObjA)
    print(repr(new_m))
    assert m == new_m

# TODO Add test serialising the actual objects we have (TaskDescription etc..)

import pathlib

import datetime
from typing import NamedTuple, List

from digitalearthau import serialise
from digitalearthau.events import Status


def test_roundtrip():
    # Convert to dict and back again, checking that it's identical.

    class EmbeddedObj(NamedTuple):
        arg1: str
        my_dt: datetime.datetime

    class ObjA(NamedTuple):
        var1: str
        var2: List[int]
        mynamedtuple: EmbeddedObj
        my_path: pathlib.Path
        status: Status

    m = ObjA(
        "a string",
        [1, 2, 3],
        EmbeddedObj(
            "b string",
            datetime.datetime.utcnow()
        ),
        pathlib.Path("/tmp/test"),
        Status.ACTIVE
    )

    d = serialise.type_to_dict(m)
    print(repr(d))
    new_m = serialise.dict_to_type(d, ObjA)
    print(repr(new_m))
    assert m == new_m

# TODO Add test serialising the actual objects we have (TaskDescription etc..)

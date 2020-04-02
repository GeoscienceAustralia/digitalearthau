# coding=utf-8

import numpy


def simple_object_repr(o):
    """
    Calculate a possible repr() for the given object using the class name and all __dict__ properties.

    eg. MyClass(prop1='val1')

    It will call repr() on property values too, so beware of circular dependencies.
    """
    return "%s(%s)" % (
        o.__class__.__name__,
        ", ".join("%s=%r" % (k, v) for k, v in sorted(o.__dict__.items()))
    )


def wofs_fuser(dest, src):
    """
    Fuse two WOfS water measurements represented as `ndarray`
    """
    empty = (dest & 1).astype(numpy.bool)
    both = ~empty & ~((src & 1).astype(numpy.bool))
    dest[empty] = src[empty]
    dest[both] |= src[both]

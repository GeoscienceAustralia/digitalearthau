# coding=utf-8


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

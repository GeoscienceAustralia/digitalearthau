# coding=utf-8
"""
Find duplicate datasets
(eg. if a dataset has been reprocessed but both versions are indexed and active)

"""
import csv
import sys
from datetime import datetime
from typing import Iterable
from uuid import UUID

from dateutil import tz
from psycopg2._range import Range
from singledispatch import singledispatch

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.index.fields import Field
from datacube.model import DatasetType, MetadataType
from datacubenci import collections


def parse_field_expression(md: MetadataType, expression: str):
    parts = expression.split('.')
    assert all(p.isidentifier() for p in parts)

    name = parts.pop(0)
    field = md.dataset_fields.get(name)
    if not field:
        raise ValueError('No field named %r in %r', name, md.name)

    while parts:
        name = parts.pop(0)
        field = getattr(field, name, None)
        if not field:
            raise ValueError('No field %s for expression %s in %s', name, expression, md.name)

    return field


def write_duplicates_csv(collections_: Iterable[collections.Collection],
                         out_stream):
    with index_connect(application_name='find-duplicates') as c:

        has_started = False
        for collection in collections_:
            matching_products = c.products.search(**collection.query)
            for product in matching_products:
                unique_fields = tuple(parse_field_expression(product.metadata_type, f)
                                      for f in collection.unique_fields)

                _write_csv(
                    unique_fields,
                    get_dupes(c, unique_fields, product),
                    out_stream,
                    append=has_started
                )
                has_started = True


def get_dupes(index, unique_fields, product):
    # type: (Index, Iterable[Field], DatasetType) -> Iterable[dict]
    headers = _get_headers(unique_fields)

    for group, dataset_refs in index.datasets.search_product_duplicates(product, *unique_fields):
        values = (product.name,) + group + (len(dataset_refs), list(dataset_refs))
        yield dict(zip(headers, values))


def _get_headers(unique_fields):
    # type: (Iterable[Field]) -> Iterable[str]
    return ('product',) + tuple(f.name for f in unique_fields) + ('count', 'dataset_refs',)


@singledispatch
def printable(val):
    return val


@printable.register(type(None))
def printable_none(val):
    return ''


@printable.register(datetime)
def printable_dt(val):
    """
    :type val: datetime.datetime
    """
    return _assume_utc(val).isoformat()


def _assume_utc(val):
    if val.tzinfo is None:
        return val.replace(tzinfo=tz.tzutc())
    else:
        return val.astimezone(tz.tzutc())


@printable.register(Range)
def printable_r(val):
    """
    :type val: psycopg2._range.Range
    """
    if val.lower_inf:
        return printable(val.upper)
    if val.upper_inf:
        return printable(val.lower)

    return printable(val.lower)


@printable.register(list)
def printable_list(val):
    """
    Space separated

    (space separated list of uuids is useful for copying to use as cli args.)

    :type val: list
    """
    return ' '.join(printable(v) for v in val)


@printable.register(UUID)
def printable_uuid(val):
    return str(val)


def _write_csv(unique_fields, dicts, stream, append=False):
    writer = csv.DictWriter(stream, _get_headers(unique_fields))
    if not append:
        writer.writeheader()
    writer.writerows(
        (
            {k: printable(v) for k, v in d.items()}
            for d in dicts
        )
    )


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        sys.stderr.write(
            'Usage: {} [collections...]\n\n'
            'Where collections are among: \n\t{}\n\n'
            'Or specify --all to check all\n'.format(
                sys.argv[0], '\n\t'.join(sorted(collections.NCI_COLLECTIONS.keys())))
        )
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        cos = list(collections.NCI_COLLECTIONS.values())
    else:
        cos = [collections.NCI_COLLECTIONS[name] for name in sys.argv[1:]]

    write_duplicates_csv(cos, sys.stdout)


if __name__ == '__main__':
    main()

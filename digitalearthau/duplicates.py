# coding=utf-8

import csv
import sys
from datetime import datetime
from functools import singledispatch
from typing import Iterable
from uuid import UUID

import click
from dateutil import tz
from psycopg2._range import Range

from datacube.index.index import Index
from datacube.index.fields import Field
from datacube.model import DatasetType, MetadataType
from datacube.ui.click import global_cli_options, pass_index
from digitalearthau import collections


def parse_field_expression(md: MetadataType, expression: str):
    parts = expression.split('.')
    assert all(p.isidentifier() for p in parts)

    name = parts.pop(0)
    field = md.dataset_fields.get(name)
    if not field:
        raise ValueError(f'No field named {name} in {md.name}')

    while parts:
        name = parts.pop(0)
        field = getattr(field, name, None)
        if not field:
            raise ValueError(f'No field {name} for expression {expression} in {md.name}')

    return field


def write_duplicates_csv(
        index: Index,
        collections_: Iterable[collections.Collection],
        out_stream):
    has_started = False
    for collection in collections_:
        matching_products = index.products.search(**collection.query)
        for product in matching_products:
            unique_fields = tuple(parse_field_expression(product.metadata_type, f)
                                  for f in collection.unique)

            _write_csv(
                unique_fields,
                get_dupes(index, unique_fields, product),
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


collections.init_nci_collections(None)


@click.command('duplicates')
@global_cli_options
@click.option('-a', '--all_', is_flag=True)
@click.argument('collections_', type=click.Choice(collections.registered_collection_names()), nargs=-1)
@pass_index(app_name="find-duplicates")
def cli(index, all_, collections_):
    """
    Find duplicate datasets for a collection.

    (eg. if a dataset has been reprocessed but both versions are indexed and active)

    This uses the unique fields defined in a collection to try to group them.

    Note that this is really a prototype: it won't report all duplicates as the unique fields aren't good enough.

      - Scenes group by "day" not "solar day"

      - Tiled products should be grouped by tile_index, but it's not in the metadata.

    """
    collections.init_nci_collections(index)

    if all_:
        collection_names = collections.registered_collection_names()
    else:
        collection_names = collections_

    write_duplicates_csv(
        index,
        [collections.get_collection(name) for name in collection_names],
        sys.stdout
    )


if __name__ == '__main__':
    cli()

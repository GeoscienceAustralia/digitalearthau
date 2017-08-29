#!/usr/bin/env python
"""
Find gaps in the purported one-to-one correspondences between datasets across
products.
"""

from sys import stderr
from datetime import datetime
from itertools import combinations

import yaml
import click
from dateutil.relativedelta import relativedelta

from datacube import Datacube
from datacube.model import Range
from datacube.api import GridWorkflow


class Dataset:
    """
    Simplified representation of a dataset.
    Two `Dataset` objects are considered "equal",
    for the purposes of finding the correspondence between two products,
    whenever they have the same spatio-temporal coordinates.
    """

    def __init__(self, id_, **keys):
        self.id_ = id_
        self.keys = keys

    def __hash__(self):
        """ Uniqueness determined by location. """
        sorted_keys = sorted(self.keys.keys())
        sorted_items = tuple((key, self.keys[key])
                             for key in sorted_keys)
        return hash(sorted_items)

    def __eq__(self, other):
        """ Uniqueness determined by location. """
        return self.keys == other.keys

    def __ne__(self, other):
        """ Uniqueness determined by location. """
        return not self.__eq__(other)

    def __str__(self):
        """ String representation by ID. """
        return str(self.id_)

    def __repr__(self):
        """ String representation by ID. """
        return str(self)

    def to_dict(self):
        """ `dict` representation. """
        return dict(id=self.id_, **self.keys)


class Tally:
    """ Tally of mismatches. """
    def __init__(self, product, misses=None, total=0):
        self.product = product
        self.misses = misses or set()
        self.total = total

    def __add__(self, other):
        assert self.product == other.product

        return Tally(self.product,
                     self.misses | other.misses,
                     self.total + other.total)

    def summary(self):
        """ Summary as a dictionary. """
        return {
            'mismatch_count': len(self.misses),
            'mismatches': [data.to_dict() for data in self.misses],
            'total_count': self.total
        }


def common_product_kind(datacube, products):
    """ Check that the products are all of the same kind: scene or tile. """

    def product_kind(product):
        """
        Whether the product is a scene or a tile.
        """
        prod = datacube.index.products.get_by_name(product)
        return prod.definition['metadata_type']

    kinds = {product_kind(product) for product in products}

    assert len(kinds) == 1, "the given products are not of the same kind"

    for kind in kinds:
        return kind


def mismatches(datacube, product1, product2, grid_workflow, query):
    """
    Returns a pair of :class:`Tally` objects recording the mismatch
    between two products matching the same (spatio-temporal) `query`.
    Expects a pre-configured `GridWorkflow` object `grid_workflow`.
    """
    datasets = datacube.index.datasets

    def check_no_duplicates(every, unique, product):
        """
        There should be no duplicates among the locations
        in a product.
        """
        if len(every) == len(unique):
            # nothing weird here
            return True

        count = 0

        for dataset in every:
            equals = [d for d in every if d == dataset]
            if len(equals) != 1:
                # what?
                count = count + 1

                if count > 3:
                    continue

                print('these datasets in {}'.format(product), file=stderr)
                print('have the same spatio-temporal location',
                      file=stderr)

                for i, elem in enumerate(equals):
                    print("#{} id: {}".format(i + 1, elem), file=stderr)
                    for key in elem.keys:
                        print("#{} {}: {}".format(i + 1, key, elem.keys[key]),
                              file=stderr)

                    print(file=stderr)

        if count > 3:
            print('... and there were {} more.'.format(count - 3),
                  file=stderr)

        return False

    def unique_scenes(product):
        """ Identify scenes by their time, path and row. """

        def extract(path_or_row):
            """ Make satellite path or row entry `yaml` friendly. """
            assert path_or_row.begin == path_or_row.end
            return int(path_or_row.begin)

        every = [Dataset(id_=str(dataset.id),
                         time=str(dataset.center_time),
                         path=extract(dataset.metadata.sat_path),
                         row=extract(dataset.metadata.sat_row))
                 for dataset in datasets.search(product=product, **query)]
        unique = set(every)

        check_no_duplicates(every, unique, product)

        return unique

    def unique_tiles(product):
        """ Identify tiles by their time and index. """
        cells = grid_workflow.cell_observations(product=product, **query)
        every = [Dataset(id_=str(dataset.id),
                         time=str(dataset.center_time),
                         index=str(index))
                 for index in cells
                 for dataset in cells[index]['datasets']]
        unique = set(every)

        check_no_duplicates(every, unique, product)

        return unique

    kind = common_product_kind(datacube, (product1, product2))

    if kind == 'landsat_scene':
        uniqueness_function = unique_scenes
    else:
        uniqueness_function = unique_tiles

    set1 = uniqueness_function(product1)
    set2 = uniqueness_function(product2)
    common = set1 & set2

    return (Tally(product1, set1 - common, len(set1)),
            Tally(product2, set2 - common, len(set2)))


def distribute(datacube, product1, product2, grid_workflow, queries):
    """
    Accumulates mismatches between two products over a list of `queries`.
    """
    total_left, total_right = Tally(product1), Tally(product2)

    for sub in queries:
        left, right = mismatches(datacube, product1, product2,
                                 grid_workflow, sub)
        total_left = total_left + left
        total_right = total_right + right

    return total_left, total_right


def divide(interval, divs):
    """
    Generate the `divs` number of equal partitions of the `interval`.
    """
    if divs is None:
        yield interval
    else:
        begin, end = interval.begin, interval.end
        step = (end - begin) / divs
        ticks = [begin] + [begin + (i + 1) * step
                           for i in range(divs - 1)] + [end]

        yield from (Range(a, b)
                    for a, b in zip(ticks[:-1], ticks[1:]))


def subdivide_time_domain(time_divs=None, **query):
    """
    Generate sub-queries that divide the time domain into smaller pieces.
    """
    if 'time' in query:
        remaining = {key: query[key] for key in query if key != 'time'}

        for interval in divide(query['time'], time_divs):
            yield dict(time=interval, **remaining)
    else:
        yield query


def find_gaps(datacube, products, query, time_divs=None):
    """ Summary of gaps in the `products` compared pairwise. """
    products = list(set(products))
    assert len(products) != 0 and len(products) != 1, "no products to compare"

    grid_workflow = GridWorkflow(datacube.index, product=products[0])

    def mismatch_summary(product1, product2):
        """ Summarize mismatch info into a dictionary. """
        subqueries = subdivide_time_domain(time_divs=time_divs,
                                           **query)
        left, right = distribute(datacube,
                                 product1, product2, grid_workflow,
                                 subqueries)
        return {
            'products': [left.product, right.product],
            left.product: left.summary(),
            right.product: right.summary()
        }

    return [mismatch_summary(product1, product2)
            for product1, product2 in combinations(products, 2)]


def str_to_date(spec, shift=0):
    """ Parse :class:`str` specification to :class:`datetime`. """
    parts = [int(x) for x in spec.split('-')]

    try:
        year, month, day = parts
        return datetime(year, month, day) + relativedelta(days=shift)
    except ValueError:
        pass

    try:
        year, month = parts
        return datetime(year, month, 1) + relativedelta(months=shift)
    except ValueError:
        year = parts[0]
        return datetime(year, 1, 1) + relativedelta(years=shift)


def time_query(start_date, end_date):
    """ Time range of query. """
    if start_date is None:
        assert end_date is None, "no start date given"
        return {}
    else:
        start = str_to_date(start_date)
        if end_date is None:
            end = datetime.now()
        else:
            # shift=1 picks up the last moment of that date spec
            end = str_to_date(end_date, shift=1)
        return {'time': Range(start, end)}


# pylint: disable=no-value-for-parameter
@click.command(help=__doc__)
@click.argument('products', nargs=-1, type=str, required=True)
@click.option('--output-file', '-o',
              type=click.File('wt'),
              default='-',
              help="Output .yaml file to write report to (default: stdout).")
@click.option('--start-date', type=str,
              help="Start of date range (YYYY-MM-DD, YYYY-MM, or YYYY).")
@click.option('--end-date', type=str,
              help="End of date range (YYYY-MM-DD, YYYY-MM, or YYYY).")
@click.option('--time-divs', type=int,
              help="Split up the computation into number of segments"
                   " of the date range.")
def main(products, output_file, start_date, end_date, time_divs):
    """ Entry point. """
    datacube = Datacube(app='find-those-gaps')

    summary = find_gaps(datacube, products,
                        time_query(start_date, end_date), time_divs)

    yaml.dump(summary, output_file, default_flow_style=False)


if __name__ == '__main__':
    main()

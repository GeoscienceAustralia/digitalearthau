import logging
from itertools import chain
from pathlib import Path
from typing import Iterable

import dawg
import structlog
from boltons import fileutils

from datacube.index import index_connect

_LOG = structlog.get_logger()


def make_path_set(log: logging.Logger,
                  product: str,
                  supplementary_paths: Iterable[Path]) -> dawg.CompletionDAWG:
    with index_connect(application_name='sync') as index:
        # assert db.in_transaction
        log.info("paths.db.load")
        all_db_results = index.datasets.search_returning(['uri'], product=product)

        log.info("paths.trie")
        uri_set = dawg.CompletionDAWG(
            chain(
                (str(uri) for uri, in all_db_results),
                (path.absolute().as_uri() for path in supplementary_paths)
            )
        )
        log.info("paths.trie.done")
    return uri_set


def iter_product_pathsets(product_locations, cache_path):
    fileutils.mkdir_p(str(cache_path))
    for product, filesystem_root in product_locations.items():
        log = _LOG.bind(product=product)
        locations_cache = cache_path.joinpath(product + '-locations.dawg')

        fileutils.atomic_save(str(locations_cache))
        if locations_cache.exists():
            path_set = dawg.CompletionDAWG()
            path_set.load(str(locations_cache))
        else:
            path_set = make_path_set(log, product, filesystem_root.rglob("ga-metadata.yaml"))
            with fileutils.atomic_save(str(locations_cache)) as f:
                path_set.write(f)
        yield product, path_set

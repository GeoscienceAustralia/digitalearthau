import dawg
from itertools import chain

from pathlib import Path

from sqlalchemy import select

from typing import Iterable
from datacube.index.postgres import _connections
from datacube.index import index_connect
import structlog

_LOG = structlog.get_logger()


def load_path_dawg(log, supplementary_paths: Iterable[str]) -> dawg.CompletionDAWG:
    plat = 'ls8'
    prod = 'level1'

    with index_connect(application_name='sync') as index, index.datasets._db.begin() as db:
        # assert db.in_transaction
        log.info("paths.db.load")
        product = '%s_%s_scene' % (plat, prod)
        all_db_results = index.datasets.search_returning(['uri'], product=product)

        log.info("paths.trie")

        uri_set = dawg.CompletionDAWG(
            chain(
                (str(uri) for uri, in all_db_results),
                (Path(path).absolute().as_uri() for path in supplementary_paths)
            )
        )
        log.info("paths.trie.done")
    return uri_set


def main():
    finished_dawg = load_path_dawg(_LOG, [])
    finished_dawg.save('all_locations.dawg')
    print(next(finished_dawg.iterkeys('file://')))


if __name__ == '__main__':
    main()

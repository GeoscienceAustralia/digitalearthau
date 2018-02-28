# coding=utf-8
import logging

import click

import digitalearthau
from datacube.index import Index
from datacube.scripts import ingest
from datacube.ui.click import pass_index, global_cli_options
from datacube.utils import read_documents

DEA_MD_TYPES = digitalearthau.CONFIG_DIR / 'metadata-types.yaml'
DEA_PRODUCTS_DIR = digitalearthau.CONFIG_DIR / 'products'
DEA_INGESTION_DIR = digitalearthau.CONFIG_DIR / 'ingestion'


def print_header(msg):
    click.secho(msg, bold=True)


def print_(msg):
    click.echo('    {}'.format(msg))


def init_dea(
        index: Index,
        with_permissions: bool,
        log_header=print_header,
        log=print_
):
    """
    Create or update a DEA configured ODC instance.
    """
    log_header(f"ODC init of {index.url}")
    was_created = index.init_db(with_default_types=False,
                                with_permissions=with_permissions)

    if was_created:
        log('Created.')
    else:
        log('Updated.')

    log('Checking indexes/views.')
    index.metadata_types.check_field_indexes(
        allow_table_lock=True,
        rebuild_indexes=False,
        rebuild_views=True,
    )

    log_header('Checking DEA metadata types')
    # Add DEA metadata types, products.
    for _, md_type_def in read_documents(DEA_MD_TYPES):
        md = index.metadata_types.add(index.metadata_types.from_doc(md_type_def))
        log(f"{md.name}")

    log_header('Checking DEA products')
    for _, product_def in read_documents(*DEA_PRODUCTS_DIR.glob('*.yaml')):
        product = index.products.add_document(product_def)
        log(f"{product.name}")

    log_header('Checking DEA ingested definitions')

    for path in DEA_INGESTION_DIR.glob('*.yaml'):
        ingest_config = ingest.load_config_from_file(index, path)

        source_type, output_type = ingest.ensure_output_type(
            index, ingest_config, allow_product_changes=True
        )
        log(f"{output_type.name:<20}\t\t← {source_type.name}")


@click.group('system')
@global_cli_options
def cli():
    pass


@cli.command('init')
@click.option(
    '--init-users/--no-init-users', is_flag=True, default=True,
    help="Include user roles and grants. (default: true)"
)
@pass_index(expect_initialised=False)
def init_dea_cli(index: Index, init_users: bool):
    """
    Initialise a DEA-configured datacube.

    It does the equivalent of this:

        datacube -v system init --no-default-types
        datacube -v metadata_type add digitalearthau/config/metadata-types.yaml
        datacube -v product add digitalearthau/config/products/ls*_scenes.yaml

    ... and adds all ingest products too.

    """
    init_dea(index, with_permissions=init_users)


if __name__ == '__main__':
    cli()

# coding=utf-8
import click

import digitalearthau
from datacube.index._api import Index
from datacube.scripts import ingest
from datacube.scripts.system import database_init
from datacube.ui.click import pass_index, global_cli_options
from datacube.utils import read_documents

DEA_MD_TYPES = digitalearthau.CONFIG_DIR / 'metadata-types.yaml'
DEA_PRODUCTS_DIR = digitalearthau.CONFIG_DIR / 'products'
DEA_INGESTION_DIR = digitalearthau.CONFIG_DIR / 'ingestion'


@click.command()
@global_cli_options
@pass_index(expect_initialised=False)
@click.pass_context
def init_dea(ctx: click.Context, index: Index):
    click.secho("ODC setup", bold=True)
    init_result = ctx.invoke(database_init, default_types=False, lock_table=True)

    click.secho('Checking DEA metadata types', bold=True)
    # Add DEA metadata types, products.
    for _, md_type_def in read_documents(DEA_MD_TYPES):
        md = index.metadata_types.add(index.metadata_types.from_doc(md_type_def))
        click.echo(f"    {md.name}")

    click.secho('Checking DEA products', bold=True)
    for _, product_def in read_documents(*DEA_PRODUCTS_DIR.glob('*.yaml')):
        product = index.products.add_document(product_def)
        click.echo(f"    {product.name}")

    click.secho('Checking DEA ingested definitions', bold=True)

    for path in DEA_INGESTION_DIR.glob('*.yaml'):
        ingest_config = ingest.load_config_from_file(index, path)

        source_type, output_type = ingest.ensure_output_type(
            index, ingest_config, allow_product_changes=True
        )
        click.echo(f"    {output_type.name:<20}\t\t← {source_type.name}")


if __name__ == '__main__':
    init_dea()

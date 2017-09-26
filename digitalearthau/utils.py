import click

import digitalearthau
from datacube.scripts.system import database_init
from datacube.ui.click import pass_index, global_cli_options
from datacube.utils import read_documents


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


DEA_MD_TYPES = digitalearthau.CONFIG_DIR / 'metadata-types.yaml'
DEA_PRODUCTS_DIR = digitalearthau.CONFIG_DIR / 'products'


@click.command()
@global_cli_options
@pass_index(expect_initialised=False)
@click.pass_context
def init_dea(ctx: click.Context, index):
    init_result = ctx.invoke(database_init, default_types=False, lock_table=True)

    # Add DEA metadata types, products.
    for _, md_type_def in read_documents(DEA_MD_TYPES):
        index.metadata_types.add(index.metadata_types.from_doc(md_type_def))

    for product_def in read_documents(*DEA_PRODUCTS_DIR.glob('*.yaml')):
        index.products.add_document(product_def)

    # TODO: Generate/load ingest configs too.


if __name__ == '__main__':
    init_dea()

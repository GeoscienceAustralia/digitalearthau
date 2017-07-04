from __future__ import print_function

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import click
import lxml.etree


def load_mapping_table(mapping_yaml='mapping.yaml'):
    with open(mapping_yaml) as mapping_stream:
        return load(mapping_stream, Loader=Loader)


def convert_cmi_node(ctx, param, value):
    try:
        node_id = int(value)
        return 'http://cmi.ga.gov.au/ecat/{}'.format(node_id)
    except ValueError:
        return value


def open_iso_tree(iso):
    return lxml.etree.parse(iso)


def query_xpath(xpath, tree):
    query = '//{}/text()'.format(xpath)
    val = tree.xpath(query, namespaces=tree.getroot().nsmap)
    if not val:
        return None
    return val[0]


def clean_text(text):
    return text.replace(u'\xa0', u' ').encode('ascii').decode('utf-8')


@click.command()
@click.option('mapping', '-m', help='Mapping file of global attributes to xpath queries',
              type=click.Path(exists=True, readable=True), default='mapping.yaml', show_default=True)
@click.argument('iso', callback=convert_cmi_node, required=True)
@click.argument('output', type=click.Path(writable=True), required=False, default=None)
def main(mapping, iso, output=None):
    """Convert an ISO19115 metadata document to a list of NetCDF global attributes
    
    The ISO19115 metadata document can specified by a URL, filepath or CMI node ID.
    """
    mapping_table = load_mapping_table(mapping)

    tree = open_iso_tree(iso)

    found_global_attrs = {}
    for key, xpaths in mapping_table.items():
        for xpath in xpaths:
            val = query_xpath(xpath, tree)
            if val:
                try:
                    found_global_attrs[key] = clean_text(val)
                    break
                except UnicodeEncodeError as err:
                    click.echo('{key} had the error {err}, with the text:\n{val}'.format(key=key, err=err, val=val),
                               err=True)

    output_yaml = dump({'global_attributes': found_global_attrs}, Dumper=Dumper, default_flow_style=False)
    if output:
        with open(output, 'w') as out_file:
            out_file.write(output_yaml)
    else:
        click.echo(output_yaml)


if __name__ == '__main__':
    main()

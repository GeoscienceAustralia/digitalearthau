from __future__ import print_function

import os
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


def find_attrs_in_tree(tree, mapping_table):
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
    return found_global_attrs


def local_file(filename):
    """ Returns an absolute path of a filepath relative to this script. """
    return os.path.join(os.path.split(os.path.realpath(__file__))[0], filename)


@click.command()
@click.option('mapping', '-m', help='Mapping file of global attributes to xpath queries.',
              type=click.Path(exists=True, readable=True), default=local_file('mapping.yaml'))
@click.argument('iso', callback=convert_cmi_node, required=True)
@click.argument('output_path', type=click.Path(writable=True), required=False, default=None)
def main(mapping, iso, output_path=None):
    """Convert an ISO19115 metadata document to a list of NetCDF global attributes
    
    The ISO19115 metadata document can specified by a URL, filepath or CMI node ID.
    
    If no output_path is given, the output is printed to screen.
    """
    mapping_table = load_mapping_table(mapping)

    tree = open_iso_tree(iso)

    global_attrs = find_attrs_in_tree(tree, mapping_table)

    output_yaml = dump({'global_attributes': global_attrs}, Dumper=Dumper, default_flow_style=False)

    if output_path:
        with open(output_path, 'w') as out_file:
            out_file.write(output_yaml)
    else:
        click.echo(output_yaml)


if __name__ == '__main__':
    main()

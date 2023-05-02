#!/usr/bin/env python

import csv
import pathlib
from typing import List, Any
import dataclasses

import click
import orjson
import synapseclient
import synapseutils
from typing import Iterator, Dict


RAW_DATA_PATH = pathlib.Path('data/raw')


def read_ndjson(path: str) -> Iterator[Dict]:
    """Read ndjson file, load json line by line."""
    with open(path) as jsonfile:
        for l_ in jsonfile.readlines():
            yield orjson.loads(l_)


def read_tsv(path: str, delimiter="\t") -> Iterator[Dict]:
    """Read tsv file line by line."""
    with open(path) as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter=delimiter)
        for row in reader:
            yield row


@click.group()
def extract():
    """Extract data from Synapse."""
    pass


@extract.command('project')
@click.argument('output_path', default=RAW_DATA_PATH)
@click.option('--project_id')
def extract_project(output_path, project_id):
    """Extract project."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    project = syn.get(project_id)
    output_path = pathlib.Path(output_path)
    with open(output_path / "project.ndjson", "w") as fp:
        bytes_ = orjson.dumps(project.__dict__, option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE)
        fp.write(str(bytes_, 'UTF-8'))


@extract.command('hierarchy')
@click.argument('output_path', default=RAW_DATA_PATH)
@click.option('--project_id')
def extract_tree(output_path, project_id):
    """Extract project hierarchy."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    output_path = pathlib.Path(output_path)

    # Traverse through the hierarchy of files and folders stored under the synId.
    # https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.walk_functions.walk
    walker = synapseutils.walk_functions.walk(syn, project_id,
                                              includeTypes=['folder', 'file', 'table', 'link', 'entityview',
                                                            'dockerrepo', 'submissionview', 'dataset',
                                                            'materializedview'])

    @dataclasses.dataclass
    class NamedId:
        name: str
        id_: str
        entity: Any

    @dataclasses.dataclass
    class WalkedPath:
        dir_path: NamedId
        items: List[NamedId]
        file_names: List[NamedId]

    def _map_item(item: tuple, fetch=True):
        """Transform a 2 item tuple to a NamedId"""
        # Getting the entity retrieves an object that holds metadata describing the matrix,
        # and also downloads the file to a local cache.
        # We _don't_ want the file, we'll do that separately
        entity = None
        if fetch:
            e_ = syn.get(item[1], downloadFile=False)
            # itemize keys and values to make the entity json serializable
            entity = {
                'properties': e_.properties,
                'annotations': e_.annotations
            }
            if hasattr(e_, '_file_handle'):
                entity['file_handle'] = e_._file_handle  # noqa
        return NamedId(item[0], item[1], entity)

    def _map_items(items: List[tuple], fetch=True):
        """Transform a list of 2 item tuples to a list of NamedId"""
        return [_map_item(i, fetch) for i in items]

    # Traverse through the hierarchy of files and folders stored under the synId.
    # https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.walk_functions.walk
    path_hierarchy = [WalkedPath(_map_item(dir_path), _map_items(items), _map_items(file_names)) for
                      dir_path, items, file_names in walker]

    with open(output_path / "hierarchy.ndjson", "w") as fp:
        for p in path_hierarchy:
            bytes_ = orjson.dumps(p.__dict__, option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE)
            fp.write(str(bytes_, 'UTF-8'))


@extract.command('files')
@click.argument('output_path', default=RAW_DATA_PATH)
@click.option('--project_id')
def extract_files(output_path, project_id):
    """Synchronizes all the files in a folder (including subfolders) from Synapse."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    output_path = pathlib.Path(output_path)

    # Synchronizes all the files in a folder (including subfolders) from Synapse
    # and adds a readme manifest with file metadata.
    # https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncFromSynapse
    synapseutils.syncFromSynapse(syn, project_id, path=str(output_path))


@extract.command('table')
@click.argument('output_path', default=RAW_DATA_PATH)
@click.option('--table_id')
def extract_table(output_path, table_id):
    """Extract project summary."""
    syn = synapseclient.Synapse(silent=True)
    syn.login(silent=True)
    output_path = pathlib.Path(output_path)
    table_csv = syn.tableQuery(f"select * from {table_id}", resultsAs="csv")
    with open(output_path / "summary_table.ndjson", "w") as fp:
        for annotation in read_tsv(table_csv.filepath, delimiter=","):
            bytes_ = orjson.dumps(annotation, option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE)
            fp.write(str(bytes_, 'UTF-8'))


@extract.command('sample')
@click.argument('output_path', default=RAW_DATA_PATH)
@click.option('--file_id')
def extract_sample_annotations(output_path, file_id):
    """Extract sample annotations."""
    syn = synapseclient.Synapse(silent=True)
    syn.login(silent=True)
    output_path = pathlib.Path(output_path)
    entity = syn.get(file_id, downloadFile=True, downloadLocation=output_path)
    with open(output_path / "sample_annotations.ndjson", "w") as fp:
        for annotation in read_tsv(entity.path, delimiter=","):
            bytes_ = orjson.dumps(annotation, option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE)
            fp.write(str(bytes_, 'UTF-8'))


if __name__ == '__main__':
    extract()

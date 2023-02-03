import csv
import pathlib
import uuid
from typing import Iterator, Dict

from orjson import orjson

RAW_DATA_PATH = pathlib.Path('data/raw')
PROJECT_ID = 'syn21577710'
SUMMARY_TABLE_ID = 'syn18486042'
SAMPLE_ANNOTATIONS_ID = 'syn18662790'
FHIR_DATA_PATH = pathlib.Path('data/fhir')

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


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

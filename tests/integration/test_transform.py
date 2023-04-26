import gzip
import importlib
import io
import pathlib
from typing import List

import orjson
from fhir.resources import FHIRAbstractModel
from fhir.resources.coding import Coding
from fhir.resources.identifier import Identifier

FHIR_MODULE = importlib.import_module('fhir.resources')


def test_expected_files(output_path: pathlib.Path, expected_files: List[str]):
    """Ensure file exists"""
    assert all(
        [pathlib.Path(output_path / expected_file).is_file() for expected_file in expected_files]
    )


def _is_ndjson(file_path: pathlib.Path) -> bool:
    """Open file, read all lines as json."""
    fp = _to_file(file_path)

    try:
        with fp:
            for line in fp.readlines():
                orjson.loads(line)
        return True
    except Exception as e:
        print(file_path, e)
        return False


def _to_file(file_path):
    """Open file appropriately."""
    if file_path.name.endswith('gz'):
        fp = io.TextIOWrapper(io.BufferedReader(gzip.GzipFile(file_path)))
    else:
        fp = open(file_path, "rb")
    return fp


def test_valid_ndjson(output_path: pathlib.Path, expected_files: List[str]):
    """Ensure each file is valid json."""
    assert all(
        [_is_ndjson(pathlib.Path(output_path / expected_file)) for expected_file in expected_files]
    )


def _is_fhir(file_path: pathlib.Path) -> bool:
    """Open file, read all lines as fhir."""

    fp = _to_file(file_path)
    try:
        with fp:
            for line in fp.readlines():
                _to_fhir(line)

        return True
    except Exception as e:
        print(file_path, e)
        return False


def _to_fhir(json_string: str) -> FHIRAbstractModel:
    """Parse string to fhir resource."""
    resource = orjson.loads(json_string)
    assert 'resourceType' in resource, f"No 'resourceType' in {resource}"
    klass = FHIR_MODULE.get_fhir_model_class(resource['resourceType'])
    return klass.parse_obj(resource)


def test_valid_fhir(output_path: pathlib.Path, expected_files: List[str]):
    """Ensure each file is valid fhir."""
    assert all(
        [_is_fhir(pathlib.Path(output_path / expected_file)) for expected_file in expected_files]
    )


def _are_fhir_conventions_ok(file_path: pathlib.Path) -> bool:
    """Collect all CodeableConcepts in resource, ensure they meet conventions."""

    resource = None

    def _check_coding(self: Coding, *args, **kwargs):
        # note `self` is the Coding
        assert self.code, f"Expected code {resource.id} {self}"
        assert self.system, f"Expected system {resource.id} {self}"
        assert self.display, f"Expected display {resource.id} {self}"
        return orig_coding_dict(self, *args, **kwargs)

    def _check_identifier(self: Identifier, *args, **kwargs):
        # note `self` is the Identifier
        assert self.value, f"Expected value {resource.id} {self}"
        assert self.system, f"Expected system {resource.id} {self}"
        return orig_identifier_dict(self, *args, **kwargs)

    # monkey patch dict() methods
    orig_coding_dict = Coding.dict
    Coding.dict = _check_coding

    orig_identifier_dict = Identifier.dict
    Identifier.dict = _check_identifier

    fp = _to_file(file_path)
    try:
        with fp:
            for line in fp.readlines():
                resource = _to_fhir(line)
                # trigger object traversal
                resource.dict()
        return True
    except AssertionError as e:
        print(file_path, e)
        return False
    finally:
        # restore patches
        Coding.dict = orig_coding_dict
        Identifier.dict = orig_identifier_dict


def test_fhir_conventions(output_path: pathlib.Path, expected_files: List[str]):
    """Ensure each resource in each file, assure that all codeable concepts are valid."""

    assert all(
        [
            _are_fhir_conventions_ok(pathlib.Path(output_path / expected_file))
            for expected_file in expected_files
        ]
    )

import pathlib
from typing import List

import pytest


@pytest.fixture
def output_path() -> pathlib.Path:
    """Path to output data."""
    return pathlib.Path('output/')


@pytest.fixture
def expected_files() -> List[str]:
    return """specimen_transform.transform.Specimen.json.gz
    transform.documentReference.DocumentReference.json.gz
    transform.patient.Patient.json.gz
    transform.researchStudy.ResearchStudy.json.gz
    transform.researchSubject.ResearchSubject.json.gz
    transform.specimenDocs.specimenDocs.json.gz
    transform.task.Task.json.gz
    transform.transform.Substance.json.gz
    transform.transform_observation.Observation.json.gz
    """.split()

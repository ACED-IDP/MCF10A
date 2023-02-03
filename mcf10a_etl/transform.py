import csv
import pathlib
import uuid
from typing import Iterator, Dict

import click
from fhir.resources.patient import Patient
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.specimen import Specimen
from fhir.resources.task import Task
from fhir.resources.documentreference import DocumentReference
from fhir.resources.extension import Extension
from orjson import orjson

from mcf10a_etl import FHIR_DATA_PATH, PROJECT_ID, ACED_NAMESPACE, read_ndjson, RAW_DATA_PATH


@click.group()
def transform():
    """Transform raw data from Synapse."""
    pass


@transform.command('study')
@click.argument('output_path', default=FHIR_DATA_PATH)
def transform_study(output_path):
    """FHIR ResearchStudy"""
    # TODO extract wiki page and complete RS.description
    output_path = pathlib.Path(output_path)
    assert output_path.is_dir(), f"{output_path} should be a directory."

    rs = ResearchStudy.parse_obj({
        'id': str(uuid.uuid5(ACED_NAMESPACE, PROJECT_ID)),
        'title': 'LINCS MCF10A Molecular Deep Dive (MDD)',
        'status': 'completed',
        'identifier': [
            {
                'system': 'https://www.synapse.org/',
                'value': PROJECT_ID
            }
        ]
    })
    with open(output_path / "ResearchStudy.ndjson", "w") as fp:
        fp.write(rs.json())


@transform.command('subjects')
@click.argument('output_path', default=FHIR_DATA_PATH)
def transform_subjects(output_path):
    """FHIR Patient, ResearchSubject"""
    output_path = pathlib.Path(output_path)
    assert output_path.is_dir(), f"{output_path} should be a directory."

    study_id = str(uuid.uuid5(ACED_NAMESPACE, PROJECT_ID))

    assay_summaries = [summary for summary in read_ndjson(RAW_DATA_PATH / "summary_table.ndjson")]
    cell_line_names = set([summary['Cell_Line'] for summary in assay_summaries])

    sample_annotations = [sample_annotation for sample_annotation in
                          read_ndjson(RAW_DATA_PATH / "sample_annotations.ndjson")]

    with open(output_path / "ResearchSubject.ndjson", "w") as fp:
        for cell_line_name in cell_line_names:
            patient_id = str(uuid.uuid5(ACED_NAMESPACE, f"Patient/{cell_line_name}"))
            subject_id = str(uuid.uuid5(ACED_NAMESPACE, f"ResearchSubject/{cell_line_name}"))
            rs = ResearchSubject.parse_obj({
                'id': subject_id,
                'individual': {'reference': f"Patient/{patient_id}"},
                'study': {'reference': f"ResearchStudy/{study_id}"},
                'status': 'on-study'
            })
            fp.write(rs.json(option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE))

    #
    demographics = {
        a['cellLine']: {'species': a['species'], 'gender': a['sex'], 'cellType': a['cellType']}
        for a in sample_annotations
    }

    with open(output_path / "Patient.ndjson", "w") as fp:
        for cell_line_name, demographic in demographics.items():
            patient_id = str(uuid.uuid5(ACED_NAMESPACE, f"Patient/{cell_line_name}"))
            patient = Patient.parse_obj({
                'id': patient_id,
                'gender': demographic['gender'],
                'identifier': [
                    {
                        'system': 'https://www.synapse.org/',
                        'value': cell_line_name
                    }
                ]

            })
            fp.write(patient.json(option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE))


@transform.command('specimens')
@click.argument('output_path', default=FHIR_DATA_PATH)
def transform_specimens(output_path):
    """FHIR Specimen"""
    output_path = pathlib.Path(output_path)
    assert output_path.is_dir(), f"{output_path} should be a directory."

    assay_summaries = [summary for summary in read_ndjson(RAW_DATA_PATH / "summary_table.ndjson")]
    cell_line_names = set([summary['Cell_Line'] for summary in assay_summaries])

    sample_annotations = [sample_annotation for sample_annotation in
                          read_ndjson(RAW_DATA_PATH / "sample_annotations.ndjson")]

    with open(output_path / "Specimen.ndjson", "w") as fp:
        for sample_annotation in sample_annotations:
            patient_id = str(uuid.uuid5(ACED_NAMESPACE, f"Patient/{sample_annotation['cellLine']}"))
            # https://www.synapse.org/#!Synapse:syn12979102
            # specimenName: the sample's ligand, time, collection, and replicate (separated by _)
            specimen_id = str(uuid.uuid5(ACED_NAMESPACE, f"Specimen/{sample_annotation['specimenName']}"))
            specimen = Specimen.parse_obj({
                'id': specimen_id,
                'subject': {'reference': f"Patient/{patient_id}"},
                'identifier': [
                    {
                        'system': 'https://www.synapse.org/#specimenID',
                        'value': sample_annotation['specimenID']
                    },
                    {
                        'system': 'https://www.synapse.org/#specimenName',
                        'value': sample_annotation['specimenName']
                    }
                ],
                'collection': {
                    'bodySite': {
                        'coding': [
                            {
                                'system': "http://purl.bioontology.org/ontology/SNOMEDCT",
                                'code': "76752008",
                                'display': 'Breast Structure'
                            }
                        ],
                        'text':  sample_annotation['cellType']
                    }
                }
            })
            fp.write(specimen.json(option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE))


@transform.command('tasks')
@click.argument('output_path', default=FHIR_DATA_PATH)
def transform_tasks(output_path):
    """FHIR Task, DocumentReference"""
    output_path = pathlib.Path(output_path)
    assert output_path.is_dir(), f"{output_path} should be a directory."

    assay_summaries = [summary for summary in read_ndjson(RAW_DATA_PATH / "summary_table.ndjson")]
    cell_line_names = set([summary['Cell_Line'] for summary in assay_summaries])

    sample_annotations = [sample_annotation for sample_annotation in
                          read_ndjson(RAW_DATA_PATH / "sample_annotations.ndjson")]

    hierarchy = [item for item in
                 read_ndjson(RAW_DATA_PATH / "hierarchy.ndjson")]

    def _document_references(_synapse_id):
        """Find the synapse_id in the hierarchy, render DocumentReference."""
        files_ = []
        for item in hierarchy:
            for file in item['file_names']:
                if file['id_'] == _synapse_id:
                    files_.append(file)
        document_references_ = []
        for file in files_:
            file_handle = file['entity']['file_handle']
            dr_ = DocumentReference.parse_obj({
                'id': file_handle['etag'],
                'status': 'current',
                'subject':  {'reference': f"Patient/{patient_id}"},
                'date': file_handle['createdOn'],
                'identifier': [
                    {
                        'system': 'https://www.synapse.org/',
                        'value': synapse_id
                    }
                ],
                # TODO type, category ?
                'content': [
                    {
                        'attachment': {
                            'contentType': file_handle['contentType'],
                            'title': file_handle['fileName'],
                            'extension': [
                                {
                                    "url": "http://aced-idp.org/fhir/StructureDefinition/md5",
                                    "valueString": file_handle['contentMd5']
                                }
                            ],
                            'size': file_handle['contentSize'],
                            'creation': file_handle['createdOn'],
                        }
                    }
                ]

            })
            document_references_.append(dr_)
        return document_references_

    with open(output_path / "DocumentReference.ndjson", "w") as dr_fp:

        with open(output_path / "Task.ndjson", "w") as fp:
            for sample_annotation in sample_annotations:
                patient_id = str(uuid.uuid5(ACED_NAMESPACE, f"Patient/{sample_annotation['cellLine']}"))
                # https://www.synapse.org/#!Synapse:syn12979102
                # specimenName: the sample's ligand, time, collection, and replicate (separated by _)
                specimen_id = str(uuid.uuid5(ACED_NAMESPACE, f"Specimen/{sample_annotation['specimenName']}"))
                assays = {k: v for k, v in sample_annotation.items() if v.startswith('syn')}
                for assay_name, synapse_id in assays.items():
                    task_id = str(uuid.uuid5(ACED_NAMESPACE, f"Task/{sample_annotation['specimenName']}-{assay_name}"))
                    document_references = _document_references(synapse_id)
                    outputs = [{
                        'type': {
                            'coding': [{'system': 'http://hl7.org/fhir','code': 'DocumentReference',}],
                            'text': 'DocumentReference'
                        },
                        'valueReference': {'reference': f"DocumentReference/{d_.id}"}
                    }
                        for d_ in document_references]
                    task = Task.parse_obj({
                        'id': task_id,
                        'for': {'reference': f"Patient/{patient_id}"},
                        'focus': {'reference': f"Specimen/{specimen_id}"},
                        'status': 'completed',
                        'intent': 'order',
                        'input': [
                            {
                                'type': {
                                    'coding': [{
                                        'system': 'http://hl7.org/fhir',
                                        'code': 'Specimen',
                                    }],
                                    'text': 'Specimen'
                                },
                                'valueReference': {'reference': f"Specimen/{specimen_id}"}
                            }
                        ],
                        'output': outputs,
                    })
                    fp.write(task.json(option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE))
                    for dr in document_references:
                        dr_fp.write(dr.json(option=orjson.OPT_NAIVE_UTC | orjson.OPT_APPEND_NEWLINE))


# https://data.humantumoratlas.org/standard/biospecimen
# https://www.ebi.ac.uk/ols/ontologies/bao


if __name__ == '__main__':
    transform()

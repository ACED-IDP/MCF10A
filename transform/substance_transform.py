#!/usr/bin/env python
import pathlib
import logging
import uuid
import json
import math
import sys
import gzip
import shutil

from fhir.resources.specimen import Specimen
from fhir.resources.substance import Substance
from fhir.resources.observation import Observation

EMITTERS = {}
FHIR_DATA_PATH = pathlib.Path('../data/fhir')
FHIR_SPECIMEN_PATH = pathlib.Path(
    '../output/transform.specimenDocs.specimenDocs.json.gz')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


def close_all_emitters():
    """Close all emitters."""
    for _ in EMITTERS.values():
        _.close()


def substance_id(ligand_combo):
    return str(uuid.uuid5(ACED_NAMESPACE, ligand_combo))


def code_mappings(ligand):
    mappings = {
        'BMP2': ['CHEMBL1926496', 'Bone Morphogenetic Protein 2'],
        'EGF': ['ENSG00000138798', 'Epidermal Growth Factor'],
        'HGF': ['ENSG00000019991', 'Hepatocyte Growth Factor'],
        'IFNG': ['ENSG00000111537', 'Interferon Gamma'],
        'OSM': ['ENSG00000099985', 'Oncostatin M'],
        'PBS': ['CHEMBL259100', 'Undefined'],
        'TGFB': ['CHEMBL1795178', 'Transforming Growth Factor Beta-1'],
        'ctrl': ['NA', 'NA']
    }

    for key in mappings:
        if key == ligand:
            if mappings[key][0][:2] == "EN":
                return mappings[key], \
                    "https://useast.ensembl.org/Homo_sapiens/Gene/"
            elif mappings[key][0][:2] == "CH":
                return mappings[key], \
                    "https://www.ebi.ac.uk/chembl/compound_report_card/"
            elif mappings[key][0][:2] == "NA":
                return "NA", "NA"

    assert (False), f"ERROR, ligand {ligand} does not have a code mapping"


def time_parser(dict_annotation_line):
    time3 = dict_annotation_line.get("experimentalTimePoint")
    days = math.floor(int(time3) / 24) + 1
    # Note, this implies that studies that go longer than 240 hours will not
    # be properly accounted for
    if days > 1:
        days = "0" + str(days)
        time3 = "00"
    else:
        days = "01"
        if len(time3) == 1:
            time3 = "0" + time3

    return f"1970-01-{days}T{time3}:00:00Z"


def populate_additive(ligand):
    additive_dict = {
        # update this reference to the correct substance that represents
        # ligand/ dosage
        "reference": f"Substance/{substance_id(ligand)}",
    }
    return additive_dict

# Note: MDACC_name didn't make it into the translation


def emit_specimen(
        output_path,
        specimen_line,
        annotation_line,
        flag,
        substances):

    first_ligand = annotation_line['ligand'] + \
        '-' + str(annotation_line['ligandDose'])
    second_ligand = annotation_line['secondLigand'] + \
        '-' + str(annotation_line['secondLigandDose'])
    first_ligand_index, second_ligand_index = 0, 0

    for i, s in enumerate(substances):
        if first_ligand in s:
            first_ligand_index = i
        if second_ligand in s:
            second_ligand_index = i

    additive_dict = []
    dict_specimen_line = json.loads(specimen_line)
    # these operations were overwriting identifiers coming from sifter
    # specimenID = [{"value": annotation_line.get('specimenID')}]
    # dict_specimen_line['identifier']= specimenID
    if annotation_line.get("ligand") != 'ctrl':
        additive_dict.append(populate_additive(substances[first_ligand_index]))

    if annotation_line.get("secondLigand") != 'none':
        additive_dict.append(
            populate_additive(
                substances[second_ligand_index]))

    processing_node = [{"additive": additive_dict,
                        "timeDateTime": time_parser(annotation_line)}]
    dict_specimen_line['processing'] = processing_node
    dict_specimen_line['resourceType'] = "Specimen"
    if (flag == "Specimen"):
        print(json.dumps(dict_specimen_line))
    specimen_ = Specimen.parse_obj(dict_specimen_line)
    return specimen_

# This leaves out RNASEQ_DATA_FASTQ data counts since spotchecking a few
# of their synIDS didn't return anything from the SummaryTable


def emit_observation(output_path, specimen_line, flag, annotation_line):
    specimen_line = specimen_line.json()
    Assays = [key.split("QCpass")[0]
              for key in annotation_line.keys() if "QCpass" in key]
    assay_mappings = {
        'ATACseq': [
            'https://ontobee.org/ontology/BAO',
            '0010038',
            'ATAC-seq Epigenetic Profiling Assay'],
        'Cyclic Immunoflourescence': [
            'https://ontobee.org/ontology/NCIT',
            'C181929',
            'Tissue-Based Cyclic Immunofluorescence'],
        'GCP': [
            'https://ontobee.org/ontology/BAO',
            '0010039',
            'Global Chromatin Epigenetic Profiling Assay'],
        'IF': [
            'https://ontobee.org/ontology/MMO',
            '0000662',
            'Immunofluorescence'],
        'L1000': [
            'https://ontobee.org/ontology/BAO',
            '0010046',
            'ATAC-seq Epigenetic Profiling Assay'],
        'live cell imaging': [
            'https://ontobee.org/ontology/CHEBI',
            '39442',
            'Fluorescent Probe Live Cell Imaging'],
        'RNAseq': [
            'https://ontobee.org/ontology/OBI',
            '0001177',
            'RNA sequencing assay'],
        'RPPA': [
            'https://ontobee.org/ontology/NCIT',
            'C184797',
            'Reverse Phase Protein Array']}

    for assay in Assays:
        subtypes = [key for key in annotation_line.keys(
        ) if "Level" in key and assay in key]
        count = sum(annotation_line[x] != "NA" for x in subtypes)

        mappings = assay_mappings[assay[:-1]]
        observation_dict = {
            "id": str(uuid.uuid5(ACED_NAMESPACE, (annotation_line["specimenName"] + assay + str(count)))),
            "resourceType": "Observation",
            "valueInteger": count,
            "status": "final",
            "category":
            [{"coding":
              [{
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "laboratory",
                  "display": "laboratory"
              }]
              }],
            "code": {
                "coding":
                [{
                    "system": mappings[0],
                    "code":mappings[1],
                    "display":f'{str(count)} samples per time point'
                }],
                "text": mappings[2]
            },
            "specimen": {"reference": str(uuid.uuid5(ACED_NAMESPACE, ("Specimen/" + annotation_line["specimenName"])))},
            "subject": {"reference": str(uuid.uuid5(ACED_NAMESPACE, ("Patient/" + annotation_line["cellLine"])))}

            # "valueCodeableConcept":{"coding":[{"display":str(count)}]}
        }

        if (flag == "Observation"):
            print(json.dumps(observation_dict))

        Observation.parse_obj(observation_dict)


def transform_directory(file_path, output_path, specimen_path, flag):
    """Transform directory listing."""

    file_path = pathlib.Path(file_path)
    output_path = pathlib.Path(output_path)
    assert file_path.is_file(), f"{file_path} does not exist."
    assert output_path.is_dir(
    ), f"{output_path} does not exist or is not a directory."

    if flag == "Substance" or flag == "Observation":
        specimen_path = pathlib.Path(
            "../data/fhir/Pre_Transform/Specimen.ndjson")
    else:
        specimen_path = pathlib.Path(specimen_path)
        with gzip.open(specimen_path, 'rb') as f_in:
            with open('specimen_for_real.json', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        specimen_path = 'specimen_for_real.json'

    with open(file_path, "rt") as fp, open(specimen_path, "rt") as sp:
        specimen_lines = sp.readlines()
        annotation_lines = fp.readlines()

        substances, unique_substances = [], []
        for annotation_line, specimen_line in zip(
                annotation_lines, specimen_lines):
            if not annotation_line or not specimen_line:
                continue

            annotation_line = json.loads(annotation_line)

            first_ligand = annotation_line['ligand'] + '-' + \
                str(annotation_line['ligandDose'])

            fl_Unique = first_ligand + '-' + \
                str(annotation_line['specimenName'])

            second_ligand = annotation_line['secondLigand'] + '-' + \
                str(annotation_line['secondLigandDose'])

            sl_Unique = second_ligand + '-' + \
                str(annotation_line['specimenName'])

            if first_ligand != "ctrl-0" and first_ligand not in substances:
                substances.append(first_ligand)
                unique_substances.append(fl_Unique)
                coding_code, site = code_mappings(
                    annotation_line.get('ligand'))
                substance_dict = {
                    "id": substance_id(fl_Unique),
                    "resourceType": "Substance",
                    "category": [{
                        "coding": [{
                            "system": "http://terminology.hl7.org/CodeSystem/substance-category",
                            "code": "chemical",
                            "display": "chemical"
                        }]
                    }],
                    "code":
                        {
                        "concept": {
                            "coding": [{
                                "system": site,
                                "code": coding_code[0],
                                "display":coding_code[1]
                            }],
                            "text": coding_code[1]
                        }
                    },
                        "quantity": {
                            "value": int(annotation_line['ligandDose']),
                        "unit": str(annotation_line['ligandDoseUnit'])
                    },
                        "instance": False

                }

                if flag == "Substance":
                    print(json.dumps(substance_dict))
                Substance.parse_obj(substance_dict)

            if second_ligand != "none-0" and second_ligand not in substances:
                substances.append(second_ligand)
                unique_substances.append(sl_Unique)
                coding_code, site = code_mappings(
                    annotation_line.get('secondLigand'))
                substance_dict = {
                    "id": substance_id(sl_Unique),
                    "resourceType": "Substance",
                    "category": [{
                        "coding": [{
                            "system": "http://terminology.hl7.org/CodeSystem/substance-category",
                            "code": "chemical",
                            "display": "chemical"
                        }]
                    }],
                    "code":
                        {
                            "concept": {
                                "coding": [{
                                    "system": site,
                                    "code": coding_code[0],
                                    "display":coding_code[1]
                                }],
                                "text": coding_code[1]
                            },
                    },
                    "quantity": {
                        "value": int(annotation_line['secondLigandDose']),
                        "unit": str(annotation_line['secondLigandDoseUnit'])
                    },
                    "instance": False

                }
                if flag == "Substance":
                    print(json.dumps(substance_dict))
                Substance.parse_obj(substance_dict)

            specimen_line_new = emit_specimen(
                output_path,
                specimen_line,
                annotation_line,
                flag,
                unique_substances)
            emit_observation(
                output_path,
                specimen_line_new,
                flag,
                annotation_line)

    close_all_emitters()


transform_directory(sys.argv[1], FHIR_DATA_PATH, sys.argv[3], sys.argv[2])

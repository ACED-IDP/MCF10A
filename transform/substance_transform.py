#!/usr/bin/env python
import click
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

from fhir.resources.task import Task
from fhir.resources.observation import Observation
from fhir.resources.domainresource import DomainResource

EMITTERS = {}
ALREADY_EMITTED = set()
FHIR_DATA_PATH = pathlib.Path('../data/fhir')
FHIR_SPECIMEN_PATH = pathlib.Path('../output/transform.specimenDocs.specimenDocs.json.gz')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')

def close_all_emitters():
    """Close all emitters."""
    for _ in EMITTERS.values():
        _.close()

def emit(output_path: pathlib.Path, resource: DomainResource):
	"""Serialize to ndjson."""
	if resource.relative_path() not in ALREADY_EMITTED:
		if resource.resource_type not in EMITTERS:
			EMITTERS[resource.resource_type] = open(output_path / f"{resource.resource_type}.ndjson", "w")
		EMITTERS[resource.resource_type].write(resource.json())

		if resource.resource_type not in EMITTERS:
			EMITTERS[resource.resource_type] = open(output_path / f"{resource.resource_type}.ndjson", "w")
		EMITTERS[resource.resource_type].write('\n')

	ALREADY_EMITTED.add(resource.relative_path())

def substance_id(ligand_combo):
	return str(uuid.uuid5(ACED_NAMESPACE, ligand_combo))

def code_mappings(ligand):
	mappings = {
		'BMP2': 'CHEMBL1926496',
		'EGF': 'ENSG00000138798',
		'HGF': 'ENSG00000019991',
		'IFNG': 'ENSG00000111537',
		'OSM': 'ENSG00000099985',
		'PBS': 'CHEMBL259100',
		'TGFB': 'CHEMBL1795178',
		'ctrl': 'NA'
		}

	for key in mappings:
		if key == ligand:
			if mappings[key][:2] == "EN":
				return mappings[key], "https://useast.ensembl.org/Homo_sapiens/Gene/"
			elif mappings[key][:2] == "CH":
				return mappings[key], "https://www.ebi.ac.uk/chembl/g/#search_results/all/"
			elif mappings[key][:2] == "NA": 
				return "NA", "NA"	
			
	assert(False),f"ERROR, ligand {ligand} does not have a code mapping"

def time_parser(dict_annotation_line):
	time3 =dict_annotation_line.get("experimentalTimePoint")
	days = math.floor(int(time3)/24) + 1
	# Note, this implies that studies that go longer than 240 hours will not be properly accounted for
	if days > 1:
			days = "0" + str(days)
			time3 = "00"
	else:
		days = "01"
		if len(time3) == 1:
			time3 = "0" + time3

	return f"1970-01-{days}T{time3}:00:00Z"

def populate_additive(ligand):
	additive_dict={
		# update this reference to the correct substance that represents ligand/ dosage 
		"reference": f"Substance/{substance_id(ligand)}",
	}
	return additive_dict

# Note: MDACC_name didn't make it into the translation
def emit_specimen(output_path, specimen_line, annotation_line, flag):

	additive_dict = []
	dict_specimen_line = json.loads(specimen_line)
	specimenID = [{"value": annotation_line.get('specimenID')}]
	dict_specimen_line['identifier']= specimenID
	first_ligand = annotation_line['ligand'] + '-' +  str(annotation_line['ligandDose'])
	second_ligand = annotation_line['secondLigand'] + '-' + str(annotation_line['secondLigandDose'])

	additive_dict.append(populate_additive(first_ligand))

	if annotation_line.get("secondLigand") != 'none':
		additive_dict.append(populate_additive(second_ligand))

	processing_node =[{"additive":additive_dict, "timeDateTime":time_parser(annotation_line)}]
	dict_specimen_line['processing']= processing_node
	dict_specimen_line['resourceType']="Specimen"
	if (flag == True):
		print(json.dumps(dict_specimen_line))
	specimen_ = Specimen.parse_obj(dict_specimen_line)
	emit(output_path, specimen_)


def transform_directory(file_path, output_path,specimen_path, flag):
	"""Transform directory listing."""
	

	flag = flag.lower() in ("true")
	file_path = pathlib.Path(file_path)
	output_path = pathlib.Path(output_path)
	assert file_path.is_file(), f"{file_path} does not exist."
	assert output_path.is_dir(), f"{output_path} does not exist or is not a directory."

	if flag == False:
		specimen_path = pathlib.Path("../data/fhir/Pre_Transform/Specimen.ndjson")
	else:
		specimen_path = pathlib.Path(specimen_path)
		with gzip.open(specimen_path, 'rb') as f_in:
			with open('specimen_for_real.json', 'wb') as f_out:
				shutil.copyfileobj(f_in, f_out)
		specimen_path ='specimen_for_real.json'

		
	with open(file_path, "rt") as fp, open(specimen_path, "rt") as sp:
		specimen_lines = sp.readlines()
		#task_lines = tp.readlines()
		annotation_lines = fp.readlines()

		substances = []
		for annotation_line, specimen_line in zip(annotation_lines,specimen_lines):
			if not annotation_line or not specimen_line:
				continue
			
			annotation_line = json.loads(annotation_line)
			first_ligand = annotation_line['ligand'] + '-' +  str(annotation_line['ligandDose'])
			second_ligand = annotation_line['secondLigand'] + '-' + str(annotation_line['secondLigandDose'])

	
			if first_ligand != 'none-0' and first_ligand not in substances:
				substances.append(first_ligand)
				coding_code, site = code_mappings(annotation_line.get('ligand'))
				substance_dict = {
					"id": substance_id(first_ligand),
					"resourceType":"Substance",
					"category":[{"coding":[{"system":"http://terminology.hl7.org/CodeSystem/substance-category#chemical"}]}],
					"code":{"coding":[{"system":site, "code":coding_code}]},
					"instance":[{"quantity":{"value":int(annotation_line.get("ligandDose")),"unit": annotation_line.get("ligandDoseUnit")}}]
				}
				if flag == False:
					print(json.dumps(substance_dict))
				substance_dict_ = Substance.parse_obj(substance_dict)
				emit(output_path, substance_dict_)


			if second_ligand != 'none-0' and second_ligand not in substances:
				substances.append(second_ligand)
				coding_code, site = code_mappings(annotation_line.get('secondLigand'))
				substance_dict = {
					"id": substance_id(second_ligand),
					"resourceType":"Substance",
					"category":[{"coding":[{"code":"http://terminology.hl7.org/CodeSystem/substance-category#chemical"}]}],
					"code":{"coding":[{"system":site, "code":coding_code}]},
					"instance":[{"quantity":{"value":int(annotation_line.get("secondLigandDose")),"unit":annotation_line.get("secondLigandDoseUnit")}}]
				}
				if flag == False:
					print(json.dumps(substance_dict))
				substance_dict_ = Substance.parse_obj(substance_dict)
				emit(output_path, substance_dict_)

					
			emit_specimen(output_path, specimen_line,annotation_line, flag)

			
			#emit_task(output_path,annotation_line,specimen_line)
		
	
	close_all_emitters()
	

transform_directory(sys.argv[1],FHIR_DATA_PATH,sys.argv[3],sys.argv[2])

import os
import sys


def func(docref):
    os.remove("../output/transform.specimenDocs.specimenDocs.json.gz")
    os.rename(docref, "../output/Observation.ndjson.gz"),
    os.rename(
        "../output/transform.documentReference.DocumentReference.json.gz",
        "../output/DocumentReference.ndjson.gz")
    os.rename(
        "../output/specimen_transform.transform.Specimen.json.gz",
        "../output/Specimen.ndjson.gz")
    os.rename(
        "../output/transform.task.Task.json.gz",
        "../output/Task.ndjson.gz")
    os.rename(
        "../output/transform.transform.Substance.json.gz",
        "../output/Substance.ndjson.gz")
    os.rename(
        "../output/transform.researchStudy.ResearchStudy.json.gz",
        "../output/ResearchStudy.ndjson.gz")
    os.rename(
        "../output/transform.researchSubject.ResearchSubject.json.gz",
        "../output/ResearchSubject.ndjson.gz")
    os.rename(
        "../output/transform.patient.Patient.json.gz",
        "../output/Patient.ndjson.gz")


func(sys.argv[1])

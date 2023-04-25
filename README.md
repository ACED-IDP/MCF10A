
# MCF10A
LINCS MCF10A Molecular Deep Dive (MDD)

## Add submodule
The `data_model` repo submodule
```
git submodule init
git submodule update
```

## Startup

Setup python env
```
python3 -m venv venv ; source venv/bin/activate
pip install -r requirements.txt
pip install -e .
```


Create your own .env file using `https://www.synapse.org/#!PersonalAccessTokens:`

```
cat Secrets/.env
SYNAPSE_AUTH_TOKEN=ey...
```

Load your environment:

```
export $(cat Secrets/.env | xargs)
```

## Toolchain

### Install GO
Install GO (at least Go 1.19): https://go.dev/dl/

### Sifter / Lathe
Clean your cache if you are expecting a new version of sifter/lathe to be downloaded

```
go clean -modcache
go install github.com/bmeg/sifter@latest
go install github.com/bmeg/lathe@latest
```

### Update path

```
export PATH=$PATH:$HOME/go/bin
```

## Building

Build Snakefile
```
lathe plan plan.yaml
```

Run build
```
snakemake -j 4
```

### Outputs

Generated data should be under `output` and include:
```
transform.documentReference.DocumentReference.json.gz
transform.patient.Patient.json.gz
transform.researchStudy.ResearchStudy.json.gz
transform.researchSubject.ResearchSubject.json.gz
transform.specimens.Specimen.json.gz
transform.task.Task.json.gz
```

### Uploading into gen3

Refer to DocumentReference.ndjson for the already  translated working data.


### Steps to upload: 
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

Create a new project in Gen3 titled "MCF10A" for this example.

Upload metadata data into peregrine:
nice -10 scripts/gen3_emitter.py data load --db_host localhost --sheepdog_creds_path ../compose-services-training/Secrets/sheepdog_creds.json --project_code MCF10A

Load metadata into elastic:
nice -10 python3 scripts/load.py  load  flat --project_id aced-MCF10A --index file --path studies/MCF10A/extractions/DocumentReference.ndjson
nice -10 python3 scripts/load.py load  flat --project_id aced-MCF10A --index patient --path studies/MCF10A/extractions/Patient.ndjson

The two attached files DocumentReference.ndjson and Patient.ndjson should be placed in studiesMCF10A/extractions 
The remaining .ndjson files can be transformed into python objects with the transformer.py from the sifter outputs.

Note: There was no transformer written to convert the patient from sifter output into something that could be uploaded into gen3. It is hardcoded into the Patient.ndjson file.

Note: The transform.py modifications to modify document reference to fit gen3 gitops have been lost. 
Some of the fields in scripts/transform.py must be changed, but only mostly lines 409 - 430  starting at the codeblock "resource.resource_type == 'DocumentReference':" 
attachment.url does not exist in this MCF10A data. A couple of the other properties might also not be compatible with the MCF10A data structure.





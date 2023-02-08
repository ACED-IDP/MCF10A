
# MCF10A
LINCS MCF10A Molecular Deep Dive (MDD)

## Startup

Setup python env
```
python3 -m venv venv ; source venv/bin/activate
pip intall -r requirements.txt
pip intall -e .
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


## Extract

Download raw data

```
export $(cat Secrets/.env | xargs)
mcf10a_etl extract --help

Usage: mcf10a_etl extract [OPTIONS] COMMAND [ARGS]...

  Extract data from Synapse.

Options:
  --help  Show this message and exit.

Commands:
  files      Synchronizes all the files in a folder (including...
  hierarchy  Extract project hierarchy.
  project    Extract project.
  sample     Extract sample annotations.
  table      Extract project summary.


```

## Transform

Transform raw data into FHIR

```
$ mcf10a_etl transform --help
Usage: mcf10a_etl transform [OPTIONS] COMMAND [ARGS]...

  Transform raw data from Synapse.

Options:
  --help  Show this message and exit.

Commands:
  specimens  FHIR Specimen
  study      FHIR ResearchStudy
  subjects   FHIR Patient, ResearchSubject
  tasks      FHIR Task, DocumentReference
```
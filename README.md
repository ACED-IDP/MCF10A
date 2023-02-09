
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

## Toolchain

### Install GO
Install GO (at least Go 1.19): https://go.dev/dl/

### Sifter / Lathe

```
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
lathe plan transform -C .
```

Run build
```
snakemake -j 4
```

### Outputs

Generated data should be under `outputs` and include:
```
transform.documentReference.DocumentReference.json.gz
transform.patient.Patient.json.gz
transform.researchStudy.ResearchStudy.json.gz
transform.researchSubject.ResearchSubject.json.gz
transform.specimens.Specimen.json.gz
transform.task.Task.json.gz
```
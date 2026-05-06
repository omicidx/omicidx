# omicidx-parsers

[![PyPI version](https://badge.fury.io/py/omicidx.svg)](https://badge.fury.io/py/omicidx)

Python parsers and Pydantic data models for public genomics repository metadata.
This package is the parsing layer for the [OmicIDX](https://github.com/omicidx)
project, which provides a cloud-native, unified metadata index for SRA, GEO,
BioSample, and BioProject.

## What it does

`omicidx-parsers` handles the extraction and validation of raw XML metadata from
NCBI repositories into structured, typed Python objects:

| Source | Entities parsed |
|--------|----------------|
| NCBI SRA | Study, Sample, Experiment, Run |
| NCBI GEO | Series (GSE), Sample (GSM), Platform (GPL) |
| NCBI BioSample / BioProject | BioSample, BioProject |

Each entity is modelled as a [Pydantic](https://docs.pydantic.dev/) v2 model,
providing validation, serialisation to JSON/dict, and a stable schema for
downstream ETL pipelines.

## Installation

```bash
pip install omicidx
```

Requires Python >= 3.9.

## Usage

### SRA

Parse an NCBI SRA mirroring XML file (gzipped):

```python
from omicidx.sra.parser import parse_xml_file

for record in parse_xml_file("meta_study_set.xml.gz"):
    print(record)
```

The parser detects the entity type (study, sample, experiment, run) from the
filename and returns an iterator of dicts.

To parse directly from a URL:

```python
from omicidx.sra.parser import parse_xml_url

for record in parse_xml_url(url, entity="STUDY"):
    print(record)
```

### GEO

```python
from omicidx.geo.parser import get_geo_accessions

# Iterate over all GSE accessions
for accession in get_geo_accessions(etyp="GSE"):
    print(accession)
```

Pydantic models for GEO entities (`GEOSeries`, `GEOSample`, `GEOPlatform`,
etc.) are in `omicidx.geo.pydantic_models`.

### BioSample / BioProject

```python
import gzip
from omicidx.biosample import BioSampleParser, BioProjectParser

# BioSample
with gzip.open("biosample_set.xml.gz", "rb") as f:
    for sample in BioSampleParser(f):
        print(sample)  # dict

# BioProject
with gzip.open("bioproject.xml.gz", "rb") as f:
    for project in BioProjectParser(f):
        print(project)  # dict
```

## Data models

Pydantic models are in the following modules:

- `omicidx.sra.pydantic_models` — SRA Study, Sample, Experiment, Run, and
  related sub-models (TaxCount, FileSet, BaseQualities, etc.)
- `omicidx.geo.pydantic_models` — GEO Series, Sample, Platform, Channel,
  Characteristic, Contact
- `omicidx.biosample` — BioSample, BioProject, and supporting types

All models use Pydantic v2 and serialise cleanly to JSON via `.model_dump_json()`.

## Relationship to OmicIDX ETL

This package is consumed by
[omicidx-gh-etl](https://github.com/omicidx/omicidx-gh-etl), which orchestrates
the full ETL pipeline: downloading raw XML from NCBI, parsing via this package,
transforming through a DuckDB warehouse, and publishing as Parquet to a public CDN.

If you want to **query** OmicIDX data rather than parse raw XML yourself, see
the [OmicIDX documentation](https://omicidx.github.io) — no local installation
required.

## Development

```bash
git clone https://github.com/omicidx/omicidx-parsers
cd omicidx-parsers
pip install -e ".[dev]"
pytest tests/
```

## License

MIT

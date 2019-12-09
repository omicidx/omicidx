# omicidx

Overview repo for the omicidx project -- start here!

Metadata from NCBI SRA, GEO, and Biosample

This project is broken up into several repositories:

- [REST API](https://github.com/omicidx/omicidx-api)
- [Example ipython and R markdown documents](https://github.com/omicidx/omicidx_examples)
- [Builder](https://github.com/omicidx/omicidx-builder)
- [Documentation (a work-in-progress)](https://github.com/omicidx/omicidx-docs)
- [Parsers (not really for public consumptions)](https://github.com/omicidx/omicidx-parsers)

## REST API

Current REST API location: 
- http://api.omicidx.cancerdatasci.org/

Current REST API Documentation (two forms, but same information):
- [Interactive Swagger documentation](http://api.omicidx.cancerdatasci.org/docs)
- [ReDoc documentation](http://api.omicidx.cancerdatasci.org/redoc)

### Example queries

- [Get study by accession](http://api.omicidx.cancerdatasci.org/sra/studies/SRP012682)
- [Get sample by accession](http://api.omicidx.cancerdatasci.org/sra/samples/SRS1017133)
- [Search for human RNA-Seq experiments with "cancer" in any text](http://api.omicidx.cancerdatasci.org/sra/experiments?q=cancer%20AND%20library_strategy%3ARNA-Seq%20AND%20sample.taxon_id%3A9606&size=10)


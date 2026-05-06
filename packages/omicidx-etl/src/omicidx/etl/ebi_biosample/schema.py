"""Schema definitions for EBI BioSample records."""

import pyarrow as pa


def get_biosample_schema() -> pa.Schema:
    """Get the PyArrow schema for EBI BioSample records.

    Returns a schema that matches the structure of EBI BioSample API responses
    with flattened characteristics.
    """
    return pa.schema([
        pa.field("accession", pa.string()),
        pa.field("name", pa.string()),
        pa.field("update", pa.string()),
        pa.field("release", pa.string()),
        pa.field("create", pa.string()),
        pa.field("taxId", pa.int64()),
        pa.field("characteristics", pa.list_(pa.struct([
            pa.field("text", pa.string()),
            pa.field("ontologyTerms", pa.list_(pa.string())),
            pa.field("unit", pa.string()),
            pa.field("characteristic", pa.string())
        ]))),
        pa.field("organization", pa.list_(pa.struct([
            pa.field("Name", pa.string()),
            pa.field("Role", pa.string()),
            pa.field("Address", pa.string()),
            pa.field("URI", pa.string()),
            pa.field("Email", pa.string())
        ]))),
        pa.field("contact", pa.list_(pa.struct([
            pa.field("Name", pa.string()),
            pa.field("Role", pa.string()),
            pa.field("Email", pa.string())
        ]))),
        pa.field("publications", pa.list_(pa.struct([
            pa.field("pubmed_id", pa.string()),
            pa.field("doi", pa.string())
        ]))),
        pa.field("externalReferences", pa.list_(pa.struct([
            pa.field("url", pa.string()),
            pa.field("duo", pa.list_(pa.string()))
        ]))),
        pa.field("_links", pa.struct([
            pa.field("self", pa.struct([pa.field("href", pa.string())])),
            pa.field("curationLinks", pa.struct([pa.field("href", pa.string())])),
            pa.field("samples", pa.struct([pa.field("href", pa.string())])),
            pa.field("curationLink", pa.struct([pa.field("href", pa.string())]))
        ]))
    ])

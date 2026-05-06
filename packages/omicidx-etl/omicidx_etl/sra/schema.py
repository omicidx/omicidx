"""
PyArrow schema definitions for SRA record types.

This module provides `get_pyarrow_schemas()` and a module-level
`PYARROW_SCHEMAS` mapping used by the SRA extraction code.
"""
from typing import Dict
import pyarrow as pa


def get_pyarrow_schema(schema_name: str) -> pa.Schema:
    """Get PyArrow schema definitions for each SRA record type.

    Returns an empty dict if `pyarrow` is not available so callers can
    import this module safely in minimal environments.
    """

    # Common nested types
    identifier_type = pa.struct([
        ("namespace", pa.string()),
        ("id", pa.string()),
        ("uuid", pa.string())
    ])

    attribute_type = pa.struct([
        ("tag", pa.string()),
        ("value", pa.string())
    ])

    xref_type = pa.struct([
        ("db", pa.string()),
        ("id", pa.string())
    ])

    file_alternative_type = pa.struct([
        ("url", pa.string()),
        ("free_egress", pa.string()),
        ("access_type", pa.string()),
        ("org", pa.string())
    ])

    file_type = pa.struct([
        ("cluster", pa.string()),
        ("filename", pa.string()),
        ("url", pa.string()),
        ("size", pa.int64()),
        ("date", pa.string()),
        ("md5", pa.string()),
        ("sratoolkit", pa.string()),
        ("alternatives", pa.list_(file_alternative_type))
    ])

    run_read_type = pa.struct([
        ("index", pa.int64()),
        ("count", pa.int64()),
        ("mean_length", pa.float64()),
        ("sd_length", pa.float64())
    ])

    base_count_type = pa.struct([
        ("base", pa.string()),
        ("count", pa.int64())
    ])

    quality_type = pa.struct([
        ("quality", pa.int32()),
        ("count", pa.int64())
    ])

    tax_count_entry_type = pa.struct([
        ("rank", pa.string()),
        ("name", pa.string()),
        ("parent", pa.int32()),
        ("total_count", pa.int64()),
        ("self_count", pa.int64()),
        ("tax_id", pa.int32())
    ])

    tax_analysis_type = pa.struct([
        ("nspot_analyze", pa.int64()),
        ("total_spots", pa.int64()),
        ("mapped_spots", pa.int64()),
        ("tax_counts", pa.list_(tax_count_entry_type))
    ])

    experiment_read_type = pa.struct([
        ("base_coord", pa.int64()),
        ("read_class", pa.string()),
        ("read_index", pa.int64()),
        ("read_type", pa.string())
    ])

    # Schema for SRA Run records
    run_schema = pa.schema([
        ("accession", pa.string()),
        ("alias", pa.string()),
        ("experiment_accession", pa.string()),
        ("title", pa.string()),
        ("total_spots", pa.int64()),
        ("total_bases", pa.int64()),
        ("size", pa.int64()),
        ("avg_length", pa.float64()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("files", pa.list_(file_type)),
        ("reads", pa.list_(run_read_type)),
        ("base_counts", pa.list_(base_count_type)),
        ("qualities", pa.list_(quality_type)),
        ("tax_analysis", tax_analysis_type)
    ])

    # Schema for SRA Study records
    study_schema = pa.schema([
        ("accession", pa.string()),
        ("study_accession", pa.string()),
        ("alias", pa.string()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("abstract", pa.string()),
        ("study_type", pa.string()),
        ("center_name", pa.string()),
        ("broker_name", pa.string()),
        ("BioProject", pa.string()),
        ("GEO", pa.string()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("xrefs", pa.list_(xref_type)),
        ("pubmed_ids", pa.list_(pa.string()))
    ])

    # Schema for SRA Sample records
    sample_schema = pa.schema([
        ("accession", pa.string()),
        ("alias", pa.string()),
        ("title", pa.string()),
        ("organism", pa.string()),
        ("description", pa.string()),
        ("taxon_id", pa.int32()),
        ("geo", pa.string()),
        ("BioSample", pa.string()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("xrefs", pa.list_(xref_type))
    ])

    # Schema for SRA Experiment records
    experiment_schema = pa.schema([
        ("accession", pa.string()),
        ("experiment_accession", pa.string()),
        ("alias", pa.string()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("design", pa.string()),
        ("center_name", pa.string()),
        ("study_accession", pa.string()),
        ("sample_accession", pa.string()),
        ("platform", pa.string()),
        ("instrument_model", pa.string()),
        ("library_name", pa.string()),
        ("library_construction_protocol", pa.string()),
        ("library_layout", pa.string()),
        ("library_layout_orientation", pa.string()),
        ("library_layout_length", pa.string()),
        ("library_layout_sdev", pa.string()),
        ("library_strategy", pa.string()),
        ("library_source", pa.string()),
        ("library_selection", pa.string()),
        ("spot_length", pa.int64()),
        ("nreads", pa.int64()),
        ("identifiers", pa.list_(identifier_type)),
        ("attributes", pa.list_(attribute_type)),
        ("xrefs", pa.list_(xref_type)),
        ("reads", pa.list_(experiment_read_type))
    ])

    schemas = {
        "run": run_schema,
        "study": study_schema,
        "sample": sample_schema,
        "experiment": experiment_schema
    }
    
    return schemas.get(schema_name, pa.schema([]))

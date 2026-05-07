"""Offline unit tests for SRA XML parser functions using inline XML fixtures."""

import xml.etree.ElementTree as ET

import pytest
from omicidx.parsers.sra import parser as sra_parser

# ---------------------------------------------------------------------------
# Minimal XML fixtures
# ---------------------------------------------------------------------------

STUDY_XML = """<STUDY accession="SRP000001" alias="my_study" center_name="NCBI">
  <IDENTIFIERS>
    <PRIMARY_ID>SRP000001</PRIMARY_ID>
    <EXTERNAL_ID namespace="BioProject">PRJNA12345</EXTERNAL_ID>
    <EXTERNAL_ID namespace="GEO">GSE12345</EXTERNAL_ID>
  </IDENTIFIERS>
  <DESCRIPTOR>
    <STUDY_TITLE>Test Study Title</STUDY_TITLE>
    <STUDY_ABSTRACT>This is the abstract.</STUDY_ABSTRACT>
    <STUDY_DESCRIPTION>Study description here.</STUDY_DESCRIPTION>
    <STUDY_TYPE existing_study_type="Transcriptome Analysis"/>
  </DESCRIPTOR>
  <STUDY_LINKS>
    <STUDY_LINK>
      <XREF_LINK><DB>pubmed</DB><ID>12345678</ID></XREF_LINK>
    </STUDY_LINK>
  </STUDY_LINKS>
  <STUDY_ATTRIBUTES>
    <STUDY_ATTRIBUTE>
      <TAG>ENA-SPOT-COUNT</TAG>
      <VALUE>1000000</VALUE>
    </STUDY_ATTRIBUTE>
  </STUDY_ATTRIBUTES>
</STUDY>"""

EXPERIMENT_XML = """<EXPERIMENT accession="SRX000001" alias="exp_alias" center_name="NCBI">
  <IDENTIFIERS>
    <PRIMARY_ID>SRX000001</PRIMARY_ID>
  </IDENTIFIERS>
  <DESIGN>
    <DESIGN_DESCRIPTION>A simple design</DESIGN_DESCRIPTION>
    <LIBRARY_DESCRIPTOR>
      <LIBRARY_NAME>lib1</LIBRARY_NAME>
      <LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY>
      <LIBRARY_SOURCE>TRANSCRIPTOMIC</LIBRARY_SOURCE>
      <LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION>
      <LIBRARY_LAYOUT><SINGLE/></LIBRARY_LAYOUT>
    </LIBRARY_DESCRIPTOR>
  </DESIGN>
  <PLATFORM>
    <ILLUMINA>
      <INSTRUMENT_MODEL>Illumina HiSeq 2000</INSTRUMENT_MODEL>
    </ILLUMINA>
  </PLATFORM>
  <STUDY_REF accession="SRP000001">
    <IDENTIFIERS><PRIMARY_ID>SRP000001</PRIMARY_ID></IDENTIFIERS>
  </STUDY_REF>
  <SAMPLE_DESCRIPTOR accession="SRS000001"/>
  <EXPERIMENT_ATTRIBUTES>
    <EXPERIMENT_ATTRIBUTE>
      <TAG>library_prep_kit</TAG>
      <VALUE>TruSeq</VALUE>
    </EXPERIMENT_ATTRIBUTE>
  </EXPERIMENT_ATTRIBUTES>
</EXPERIMENT>"""

SAMPLE_XML = """<SAMPLE accession="SRS000001" alias="samp_alias">
  <IDENTIFIERS>
    <PRIMARY_ID>SRS000001</PRIMARY_ID>
    <EXTERNAL_ID namespace="BioSample">SAMN00000001</EXTERNAL_ID>
  </IDENTIFIERS>
  <TITLE>Human blood sample</TITLE>
  <SAMPLE_NAME>
    <TAXON_ID>9606</TAXON_ID>
    <SCIENTIFIC_NAME>Homo sapiens</SCIENTIFIC_NAME>
  </SAMPLE_NAME>
  <DESCRIPTION>Blood drawn from healthy donor.</DESCRIPTION>
  <SAMPLE_ATTRIBUTES>
    <SAMPLE_ATTRIBUTE>
      <TAG>tissue</TAG>
      <VALUE>blood</VALUE>
    </SAMPLE_ATTRIBUTE>
  </SAMPLE_ATTRIBUTES>
</SAMPLE>"""

RUN_XML = """<RUN accession="SRR000001" alias="run_alias" total_spots="1000000"
             total_bases="100000000" size="50000000">
  <IDENTIFIERS>
    <PRIMARY_ID>SRR000001</PRIMARY_ID>
  </IDENTIFIERS>
  <EXPERIMENT_REF accession="SRX000001"/>
  <TITLE>Test Run</TITLE>
  <RUN_ATTRIBUTES>
    <RUN_ATTRIBUTE>
      <TAG>run_quality</TAG>
      <VALUE>good</VALUE>
    </RUN_ATTRIBUTE>
  </RUN_ATTRIBUTES>
</RUN>"""


# ---------------------------------------------------------------------------
# parse_study
# ---------------------------------------------------------------------------


def test_parse_study_basic_fields():
    xml = ET.fromstring(STUDY_XML)
    result = sra_parser.parse_study(xml)

    assert result["accession"] == "SRP000001"
    assert result["alias"] == "my_study"
    assert result["title"] == "Test Study Title"
    assert result["abstract"] == "This is the abstract."
    assert result["study_type"] == "Transcriptome Analysis"


def test_parse_study_identifiers():
    xml = ET.fromstring(STUDY_XML)
    result = sra_parser.parse_study(xml)

    # ID normalization strips "geo|gds|bioproject|..." but not "prjna" prefix
    assert result["BioProject"] == "PRJNA12345"
    assert result["GEO"] == "GSE12345"


def test_parse_study_pubmed_ids():
    xml = ET.fromstring(STUDY_XML)
    result = sra_parser.parse_study(xml)

    assert "pubmed_ids" in result
    assert "12345678" in result["pubmed_ids"]


# ---------------------------------------------------------------------------
# parse_experiment
# ---------------------------------------------------------------------------


def test_parse_experiment_basic_fields():
    xml = ET.fromstring(EXPERIMENT_XML)
    result = sra_parser.parse_experiment(xml)

    assert result["accession"] == "SRX000001"
    assert result["library_strategy"] == "RNA-Seq"
    assert result["library_source"] == "TRANSCRIPTOMIC"
    assert result["library_selection"] == "cDNA"
    assert result["library_layout"] == "SINGLE"
    assert result["instrument_model"] == "Illumina HiSeq 2000"


def test_parse_experiment_attributes():
    xml = ET.fromstring(EXPERIMENT_XML)
    result = sra_parser.parse_experiment(xml)

    assert "attributes" in result
    attrs = {a["tag"]: a["value"] for a in result["attributes"]}
    assert attrs["library_prep_kit"] == "TruSeq"


# ---------------------------------------------------------------------------
# parse_sample
# ---------------------------------------------------------------------------


def test_parse_sample_basic_fields():
    xml = ET.fromstring(SAMPLE_XML)
    result = sra_parser.parse_sample(xml)

    assert result["accession"] == "SRS000001"
    assert result["title"] == "Human blood sample"
    assert result["organism"] == "Homo sapiens"
    assert result["taxon_id"] == 9606


def test_parse_sample_biosample_xref():
    xml = ET.fromstring(SAMPLE_XML)
    result = sra_parser.parse_sample(xml)

    assert result["BioSample"] == "SAMN00000001"


def test_parse_sample_attributes():
    xml = ET.fromstring(SAMPLE_XML)
    result = sra_parser.parse_sample(xml)

    attrs = {a["tag"]: a["value"] for a in result["attributes"]}
    assert attrs["tissue"] == "blood"


# ---------------------------------------------------------------------------
# parse_run
# ---------------------------------------------------------------------------


def test_parse_run_basic_fields():
    xml = ET.fromstring(RUN_XML)
    result = sra_parser.parse_run(xml)

    assert result["accession"] == "SRR000001"
    assert result["total_spots"] == 1000000
    assert result["total_bases"] == 100000000
    assert result["experiment_accession"] == "SRX000001"


def test_parse_run_avg_length():
    xml = ET.fromstring(RUN_XML)
    result = sra_parser.parse_run(xml)

    assert "avg_length" in result
    assert result["avg_length"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# dict_from_single_xml / model_from_single_xml
# ---------------------------------------------------------------------------


def test_dict_from_single_xml_study():
    result = sra_parser.dict_from_single_xml(STUDY_XML)
    assert result["entity_type"] == "study"
    assert result["accession"] == "SRP000001"


def test_dict_from_single_xml_experiment():
    result = sra_parser.dict_from_single_xml(EXPERIMENT_XML)
    assert result["entity_type"] == "experiment"
    assert result["accession"] == "SRX000001"


def test_model_from_single_xml_study():
    from omicidx.parsers.sra.pydantic_models import SraStudy

    model = sra_parser.model_from_single_xml(STUDY_XML)
    assert isinstance(model, SraStudy)
    assert model.accession == "SRP000001"


# ---------------------------------------------------------------------------
# _parse_attributes: edge cases
# ---------------------------------------------------------------------------


def test_parse_attributes_none_returns_empty():
    result = sra_parser._parse_attributes(None)
    assert result == {}


def test_parse_attributes_missing_value_text():
    """Attribute with no VALUE text should be skipped, not crash."""
    xml = ET.fromstring(
        """<SAMPLE_ATTRIBUTES>
        <SAMPLE_ATTRIBUTE><TAG>key</TAG><VALUE/></SAMPLE_ATTRIBUTE>
    </SAMPLE_ATTRIBUTES>"""
    )
    result = sra_parser._parse_attributes(xml)
    # No crash; attribute recorded with None value
    assert "attributes" in result
    assert result["attributes"][0]["tag"] == "key"
    assert result["attributes"][0]["value"] is None

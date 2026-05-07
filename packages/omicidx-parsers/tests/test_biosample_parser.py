"""Offline unit tests for BioSampleParser with XML fixtures."""

from io import StringIO

from omicidx.parsers.biosample import BioSampleParser


def _make_biosample_xml(body: str) -> StringIO:
    return StringIO(f"<BioSampleSet>{body}</BioSampleSet>")


MINIMAL_BIOSAMPLE = """
<BioSample accession="SAMN00000001" id="1">
  <Ids>
    <Id db="SRA">SRS000001</Id>
    <Id db="GEO">GSM000001</Id>
    <Id db="dbGaP">phs000001</Id>
  </Ids>
  <Description>
    <Title>Test sample</Title>
    <Comment><Paragraph>A test biosample.</Paragraph></Comment>
  </Description>
  <Organism taxonomy_id="9606" taxonomy_name="Homo sapiens"/>
  <Attributes>
    <Attribute attribute_name="tissue" harmonized_name="tissue" display_name="Tissue">blood</Attribute>
  </Attributes>
  <Models><Model>Generic</Model></Models>
</BioSample>
"""


def test_biosample_basic_fields():
    parser = BioSampleParser(_make_biosample_xml(MINIMAL_BIOSAMPLE))
    result = next(parser)

    assert result["accession"] == "SAMN00000001"
    assert result["id"] == "1"
    assert result["title"] == "Test sample"
    assert result["description"] == "A test biosample."
    assert result["taxon_id"] == 9606
    assert result["taxonomy_name"] == "Homo sapiens"


def test_biosample_cross_references():
    parser = BioSampleParser(_make_biosample_xml(MINIMAL_BIOSAMPLE))
    result = next(parser)

    assert result["sra_sample"] == "SRS000001"
    assert result["gsm"] == "GSM000001"
    assert result["dbgap"] == "phs000001"


def test_biosample_attributes():
    parser = BioSampleParser(_make_biosample_xml(MINIMAL_BIOSAMPLE))
    result = next(parser)

    assert "tissue" in result["attributes"]
    assert result["attribute_recs"][0]["attribute_name"] == "tissue"
    assert result["attribute_recs"][0]["value"] == "blood"


def test_biosample_model():
    parser = BioSampleParser(_make_biosample_xml(MINIMAL_BIOSAMPLE))
    result = next(parser)

    assert result["model"] == "Generic"


def test_biosample_multiple_records():
    xml = _make_biosample_xml(
        MINIMAL_BIOSAMPLE
        + MINIMAL_BIOSAMPLE.replace("SAMN00000001", "SAMN00000002").replace(
            'id="1"', 'id="2"'
        )
    )
    parser = BioSampleParser(xml)
    results = list(parser)
    assert len(results) == 2
    assert results[0]["accession"] == "SAMN00000001"
    assert results[1]["accession"] == "SAMN00000002"


NO_SRA_BIOSAMPLE = """
<BioSample accession="SAMN99999999" id="99">
  <Ids>
    <Id db="BioProject">PRJNA99999</Id>
  </Ids>
  <Description>
    <Title>No SRA cross-ref</Title>
  </Description>
  <Organism taxonomy_id="10090" taxonomy_name="Mus musculus"/>
  <Attributes/>
  <Models><Model>Generic</Model></Models>
</BioSample>
"""


def test_biosample_missing_sra_cross_refs():
    """BioSample with no SRA/GEO/dbGaP IDs should have None for those fields."""
    parser = BioSampleParser(_make_biosample_xml(NO_SRA_BIOSAMPLE))
    result = next(parser)

    assert result["sra_sample"] is None
    assert result["gsm"] is None
    assert result["dbgap"] is None


def test_biosample_no_description_paragraph():
    """BioSample without Description/Comment/Paragraph should not crash."""
    xml_no_comment = """
<BioSample accession="SAMN11111111" id="11">
  <Ids><Id db="SRA">SRS111111</Id></Ids>
  <Description><Title>No paragraph</Title></Description>
  <Organism taxonomy_id="9606" taxonomy_name="Homo sapiens"/>
  <Attributes/>
</BioSample>
"""
    parser = BioSampleParser(_make_biosample_xml(xml_no_comment))
    result = next(parser)

    assert result["accession"] == "SAMN11111111"
    assert result["description"] is None

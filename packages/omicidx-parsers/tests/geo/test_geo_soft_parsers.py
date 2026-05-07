"""Offline unit tests for GEO SOFT parser internal functions."""

import pytest
from omicidx.parsers.geo import parser as geo_parser

# ---------------------------------------------------------------------------
# _split_on_first_equal
# ---------------------------------------------------------------------------


def test_split_on_first_equal_basic():
    assert geo_parser._split_on_first_equal("key = value") == ("key", "value")


def test_split_on_first_equal_multiple_equals():
    result = geo_parser._split_on_first_equal("this = abc = 1")
    assert result == ("this", "abc = 1")


def test_split_on_first_equal_caret():
    """Lines starting with ^ (entity markers) use = as delimiter."""
    key, val = geo_parser._split_on_first_equal("^SERIES = GSE2553")
    assert key == "^SERIES"
    assert val == "GSE2553"


# ---------------------------------------------------------------------------
# get_geo_entities
# ---------------------------------------------------------------------------

SOFT_BLOCK = """\
^SERIES = GSE2553
!Series_title = My Series Title
!Series_status = Public on Jan 01 2020
^SAMPLE = GSM12345
!Sample_title = Sample One
!Sample_status = Public on Jan 01 2020
"""


def test_get_geo_entities_returns_dict():
    lines = SOFT_BLOCK.splitlines()
    result = geo_parser.get_geo_entities(lines)
    assert isinstance(result, dict)


def test_get_geo_entities_keys():
    lines = SOFT_BLOCK.splitlines()
    result = geo_parser.get_geo_entities(lines)
    assert "GSE2553" in result
    assert "GSM12345" in result


def test_get_geo_entities_values_are_lists():
    lines = SOFT_BLOCK.splitlines()
    result = geo_parser.get_geo_entities(lines)
    assert isinstance(result["GSE2553"], list)
    assert isinstance(result["GSM12345"], list)


# ---------------------------------------------------------------------------
# Relation-parsing helpers
# ---------------------------------------------------------------------------


def test_get_subseries_from_relations():
    rels = [
        "SuperSeries of: GSE111",
        "BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA1",
    ]
    result = geo_parser.get_subseries_from_relations(rels)
    assert result == ["GSE111"]


def test_get_bioprojects_from_relations():
    rels = ["BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA12345"]
    result = geo_parser.get_bioprojects_from_relations(rels)
    assert result == ["PRJNA12345"]


def test_get_sra_from_relations():
    rels = ["SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRP000001"]
    result = geo_parser.get_sra_from_relations(rels)
    assert result == ["SRP000001"]


def test_get_biosample_from_relations():
    rels = ["BioSample: https://www.ncbi.nlm.nih.gov/biosample/SAMN00000001"]
    result = geo_parser.get_biosample_from_relations(rels)
    assert result == ["SAMN00000001"]


def test_relation_helpers_empty_list():
    assert geo_parser.get_subseries_from_relations([]) == []
    assert geo_parser.get_bioprojects_from_relations([]) == []
    assert geo_parser.get_sra_from_relations([]) == []
    assert geo_parser.get_biosample_from_relations([]) == []


def test_relation_helpers_no_match():
    rels = ["something else entirely"]
    assert geo_parser.get_subseries_from_relations(rels) == []
    assert geo_parser.get_bioprojects_from_relations(rels) == []
    assert geo_parser.get_sra_from_relations(rels) == []
    assert geo_parser.get_biosample_from_relations(rels) == []


# ---------------------------------------------------------------------------
# _split_geo_name / _split_contributor_names
# ---------------------------------------------------------------------------


def test_split_geo_name_full():
    result = geo_parser._split_geo_name("John,M,Doe")
    assert result == {"first": "John", "middle": "M", "last": "Doe"}


def test_split_geo_name_first_last():
    result = geo_parser._split_geo_name("Jane,Doe")
    assert result == {"first": "Jane", "middle": "Doe"}


def test_split_contributor_names_empty():
    assert geo_parser._split_contributor_names([]) == []


def test_split_contributor_names():
    result = geo_parser._split_contributor_names(["John,M,Doe", "Jane,,Smith"])
    assert len(result) == 2
    assert result[0]["first"] == "John"


# ---------------------------------------------------------------------------
# get_channel_characteristics
# ---------------------------------------------------------------------------


def test_get_channel_characteristics_basic():
    d = {
        "source_name_ch1": ["liver"],
        "organism_ch1": ["Homo sapiens"],
        "characteristics_ch1": ["tissue: liver", "age: 30"],
    }
    result = geo_parser.get_channel_characteristics(d, 1)
    assert result["source_name"] == "liver"
    assert result["organism"] == "Homo sapiens"
    char_dict = {c["tag"]: c["value"] for c in result["characteristics"]}
    assert char_dict["tissue"] == "liver"
    assert char_dict["age"] == "30"


# ---------------------------------------------------------------------------
# Network-dependent tests — marked so they can be skipped in CI
# ---------------------------------------------------------------------------


pytestmark_network = pytest.mark.network


@pytest.mark.network
def test_get_geo_accession_soft_live():
    """Hits NCBI — requires network."""
    result = geo_parser.get_geo_accession_soft("GSE10")
    assert isinstance(result, str)
    assert result.startswith("^SERIES = ")


@pytest.mark.network
def test_get_geo_accession_xml_live():
    """Hits NCBI — requires network."""
    import io

    result = geo_parser.get_geo_accession_xml("GSE10")
    assert isinstance(result, io.BytesIO)
    firstline = next(result)
    assert firstline.decode("UTF-8").startswith("<?xml")

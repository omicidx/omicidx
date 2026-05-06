import pytest

from omicidx.sra import parser as s
import datetime

EXAMPLE_SRR = 'SRR390728'

@pytest.fixture
def example_runbrowser_model():
    """Return a populated pydantic model dict from runbrowser"""
    return s.models_from_runbrowser(EXAMPLE_SRR)

def test_runbrowser_result_has_keys(example_runbrowser_model):
    """runbrowser model has study, sample, experiment records"""
    example_runbrowser_model = example_runbrowser_model.dict()
    for i in 'sample study experiment'.split():
        assert i in example_runbrowser_model.keys()

def test_runbrowser_run_has_qualities(example_runbrowser_model):
    run = example_runbrowser_model
    assert len(run.qualities) > 0

def test_runbrowser_run_has_tax_analysis(example_runbrowser_model):
    run = example_runbrowser_model
    assert run.tax_analysis is not None

def test_runbrowser_run_accession_matches(example_runbrowser_model):
    run = example_runbrowser_model
    assert run.accession == EXAMPLE_SRR

def test_runbrowser_run_has_entries(example_runbrowser_model):
    run = example_runbrowser_model
    assert len(run.files)>0
    assert len(run.base_counts)>0
    assert len(run.tax_analysis.tax_counts) > 0
    assert run.total_bases > 0
    assert run.total_spots > 0
    assert run.cluster_name == "public"

def test_runbrowser_livelist_entries(example_runbrowser_model):
    run = example_runbrowser_model
    assert isinstance(run.published, datetime.datetime)
    assert isinstance(run.received, datetime.datetime) or run.received is None 
    assert isinstance(run.lastupdate, datetime.datetime) or run.lastupdate is None 
    assert isinstance(run.insdc, bool)
    assert isinstance(run.status, str)

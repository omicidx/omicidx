"""Tooling and utilities for working with ontologies

```
from omicidx.ontologies.utils import parse_ontology

cl = ontology_from_obo_library('cl.obo')
cl_item = cl['CL:0000900']
# lowercase definition
cl_item.definition.lower() 
# term
cl_item.name
# all direct relationships
cl_item.relationships
# superclasses (all ancestors)
#  and an equivalent subclasses is available (all descendents)
list(cl_item.superclasses())
"""
from pronto import Ontology
import requests

ONTOLOGY_CATALOG_URL = 'http://obofoundry.org/registry/ontologies.jsonld'


def ontology_from_obo_library(ontology_short_name: str) -> Ontology:
    """parse an ontology

    This is taken directly from pronto Ontology right 
    now. Parse an OBO, JSON-graph, or OWL format ontology.
    
    Parameters
    ==========
    ontology_short_name: str 
        The short name from (cl.obo for cell line, ncit.obo for NCIT, etc.)
        
    Return
    ======
    An pronto Ontology object
    """
    ont = Ontology.from_obo_library(ontology_short_name)
    return ont


def get_all_ontologies_from_obo_library():
    """get complete list of ontologies from ontobee

    Return
    ======
    list of dict, each representing an ontology.

    ```
    ont_ids = list([ont['id'] for ont in get_all_ontologies_from_obo_library()])
    ont_ids[0:5]
    ```

    """
    resp = requests.get(ONTOLOGY_CATALOG_URL)
    return resp.json()['ontologies']

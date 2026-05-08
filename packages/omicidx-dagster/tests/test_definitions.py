"""Import-time smoke test for the Dagster code location.

Catches errors that only surface when Dagster loads the module:
decorator validation, deps wiring, schema parsing, missing imports.
A failure here means the deployed code-server will fail to load.
"""


def test_definitions_loadable():
    from omicidx.dagster import definitions

    assert definitions is not None

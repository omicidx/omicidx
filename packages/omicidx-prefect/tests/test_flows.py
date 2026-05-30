"""Smoke tests: every flow module imports cleanly and registers its flows.

A failure here means the worker would fail to load the deployment.
"""

import importlib

FLOW_MODULES = [
    "omicidx.prefect.flows.sra",
    "omicidx.prefect.flows.geo",
    "omicidx.prefect.flows.biosample",
    "omicidx.prefect.flows.pubmed",
    "omicidx.prefect.flows.ebi_biosample",
    "omicidx.prefect.flows.consolidate",
    "omicidx.prefect.flows.ducklake",
    "omicidx.prefect.flows.ducklake_biosample",
    "omicidx.prefect.flows.ducklake_geo",
    "omicidx.prefect.flows.ducklake_sra",
    "omicidx.prefect.flows.ducklake_pubmed",
    "omicidx.prefect.flows.ducklake_ebi_biosample",
    "omicidx.prefect.flows.ducklake_derived",
    "omicidx.prefect.flows.ducklake_linkage",
    "omicidx.prefect.flows.ducklake_load",
    "omicidx.prefect.flows.postgres",
    "omicidx.prefect.flows.sql",
    "omicidx.prefect.flows.main",
]


def test_flow_modules_import(monkeypatch):
    # Settings() reads env vars at import time via the config helpers, but
    # only when actually called. Stub the required vars so any module-level
    # config access works on import.
    monkeypatch.setenv("PUBLISH_ROOT", "s3://test-bucket")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    for name in FLOW_MODULES:
        mod = importlib.import_module(name)
        assert mod is not None


def test_semaphore_namespace_validation():
    import pytest
    from omicidx.prefect.semaphore import SemaphoreStore

    with pytest.raises(ValueError):
        SemaphoreStore("")
    with pytest.raises(ValueError):
        SemaphoreStore("///")

    store = SemaphoreStore("sra/study")
    assert store.namespace == "sra/study"


def test_semaphore_key_validation(monkeypatch, tmp_path):
    """Keys must not contain slashes."""
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    # Bust the lru_cache so the test env sticks
    from omicidx.prefect import config

    config.settings.cache_clear()

    import pytest
    from omicidx.prefect.semaphore import SemaphoreStore

    store = SemaphoreStore("test")
    with pytest.raises(ValueError):
        store.exists("bad/key")
    with pytest.raises(ValueError):
        store.exists("")


def test_semaphore_roundtrip(monkeypatch, tmp_path):
    """Write a semaphore, read it back, list, then clear."""
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    from omicidx.prefect import config

    config.settings.cache_clear()

    from omicidx.prefect.semaphore import SemaphoreStore

    store = SemaphoreStore("sra/study")
    assert not store.exists("2024-09-13_Full")
    store.mark_done("2024-09-13_Full", metadata={"row_count": 42})
    assert store.exists("2024-09-13_Full")

    payload = store.read("2024-09-13_Full")
    assert payload["namespace"] == "sra/study"
    assert payload["key"] == "2024-09-13_Full"
    assert payload["metadata"]["row_count"] == 42
    assert "completed_at" in payload

    assert store.list_keys() == ["2024-09-13_Full"]
    assert store.clear("2024-09-13_Full") is True
    assert not store.exists("2024-09-13_Full")
    assert store.clear("2024-09-13_Full") is False

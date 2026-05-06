"""Tests for GEO skip guard logic and write_geo_entity_worker behaviour.

Regression tests for the bug introduced in commit b859a5d, which changed the
skip guard in geo_metadata_by_date from `or` to `and`.  Many months have no
GPL (platform) records; before the fix, those months were never marked as
"done" because gpl_path was never written.  On each subsequent run the
and-based guard evaluated False → the month was reprocessed → thousands of
requests were fired at NCBI → ConnectError after 1 h 43 m.

The fix: write_geo_entity_worker now always writes all three files (even when
they are empty gzip archives), so the and-guard correctly skips processed
months on subsequent runs.
"""

import gzip
import io
from datetime import date

import anyio
import pytest
from anyio import create_memory_object_stream
from upath import UPath

import omicidx_etl.geo.extract as geo_extract


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_paths(tmp_path: UPath, start: date, end: date):
    """Return the three output paths for a month, as local UPaths."""
    old_output = geo_extract.OUTPUT_PATH
    geo_extract.OUTPUT_PATH = UPath(tmp_path)
    gse, gsm, gpl = geo_extract.get_result_paths(start, end)
    geo_extract.OUTPUT_PATH = old_output
    return gse, gsm, gpl


# ---------------------------------------------------------------------------
# Unit tests for get_result_paths
# ---------------------------------------------------------------------------

def test_get_result_paths_structure(tmp_path):
    """get_result_paths returns hive-partitioned paths under OUTPUT_PATH."""
    geo_extract.OUTPUT_PATH = UPath(tmp_path)
    start = date(2023, 4, 1)
    end = date(2023, 4, 30)
    gse, gsm, gpl = geo_extract.get_result_paths(start, end)

    assert "gse" in str(gse)
    assert "gsm" in str(gsm)
    assert "gpl" in str(gpl)
    assert "year=2023" in str(gse)
    assert "month=04" in str(gse)
    assert gse.name == "data_0.ndjson.gz"


# ---------------------------------------------------------------------------
# Skip-guard tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_skip_guard_skips_when_any_file_exists(tmp_path, monkeypatch):
    """geo_metadata_by_date returns immediately when any output file exists.

    Some months have no GSE records (only GSM/GPL), others have no GPL.
    Using `or` means any existing file is sufficient evidence the month was
    already processed.  The always-write fix (write_geo_entity_worker) ensures
    all three files are created on every future run, so the check is
    unambiguous going forward.
    """
    monkeypatch.setattr(geo_extract, "OUTPUT_PATH", UPath(tmp_path))

    # Test with only GSM present (no GSE, no GPL) — common for some months
    start = date(2020, 1, 1)
    end = date(2020, 1, 31)

    _gse, gsm, _gpl = geo_extract.get_result_paths(start, end)
    gsm.parent.mkdir(parents=True, exist_ok=True)
    gsm.touch()

    async def must_not_be_called(*args, **kwargs):
        raise AssertionError("prod1 (network) should not run when month is already processed")

    monkeypatch.setattr(geo_extract, "prod1", must_not_be_called)

    # Should return without raising — skip guard fires even with only GSM
    await geo_extract.geo_metadata_by_date(start, end, UPath(tmp_path))


@pytest.mark.anyio
async def test_skip_guard_does_not_skip_when_no_files_exist(tmp_path, monkeypatch):
    """geo_metadata_by_date processes months that have no output files at all."""
    monkeypatch.setattr(geo_extract, "OUTPUT_PATH", UPath(tmp_path))

    start = date(2020, 2, 1)
    end = date(2020, 2, 29)

    # No files created — month has never been processed
    called = []

    async def mock_prod1(send_stream, s, e):
        called.append(True)
        async with send_stream:
            pass  # send nothing

    monkeypatch.setattr(geo_extract, "prod1", mock_prod1)

    await geo_extract.geo_metadata_by_date(start, end, UPath(tmp_path))

    assert called, "prod1 should be invoked when no output files exist"


# ---------------------------------------------------------------------------
# write_geo_entity_worker tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_write_geo_entity_worker_always_writes_three_files(tmp_path, monkeypatch):
    """write_geo_entity_worker writes all 3 output files even when GPL has no records.

    Regression test: before the fix, only files with at least one record were
    written.  Months with no GPL updates never got a gpl_path, causing the
    and-based skip guard to reprocess them on every run.
    """
    monkeypatch.setattr(geo_extract, "OUTPUT_PATH", UPath(tmp_path))

    start = date(2021, 3, 1)
    end = date(2021, 3, 31)

    # Empty stream — no records of any type
    send, receive = create_memory_object_stream(10)
    async with send:
        pass  # close immediately so the worker can finish

    await geo_extract.write_geo_entity_worker(receive, start, end, UPath(tmp_path))

    gse, gsm, gpl = geo_extract.get_result_paths(start, end)
    assert gse.exists(), "GSE file must always be written"
    assert gsm.exists(), "GSM file must always be written"
    assert gpl.exists(), "GPL file must always be written (even when empty) — regression check"


@pytest.mark.anyio
async def test_write_geo_entity_worker_empty_files_are_valid_gzip(tmp_path, monkeypatch):
    """Files written for empty months are valid (possibly empty) gzip archives."""
    monkeypatch.setattr(geo_extract, "OUTPUT_PATH", UPath(tmp_path))

    start = date(2021, 4, 1)
    end = date(2021, 4, 30)

    send, receive = create_memory_object_stream(10)
    async with send:
        pass

    await geo_extract.write_geo_entity_worker(receive, start, end, UPath(tmp_path))

    gse, gsm, gpl = geo_extract.get_result_paths(start, end)
    for path in (gse, gsm, gpl):
        data = path.read_bytes()
        # gzip.decompress should not raise for a valid (even empty) archive
        decompressed = gzip.decompress(data)
        # An empty ndjson.gz has no lines
        assert decompressed == b"", f"{path.name} should be an empty gzip archive"

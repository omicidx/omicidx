from pathlib import Path

from omicidx_etl.biosample.extract import OUTPUT_SUFFIX, cleanup_old_files
from omicidx_etl.geo.extract import entrezid_to_geo
from omicidx_etl.sra.mirror import SRAMirrorEntry
from omicidx_etl.sra.schema import get_pyarrow_schema


def test_sra_mirror_entry_parses_expected_fields():
    entry = SRAMirrorEntry(
        "https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/"
        "NCBI_SRA_Mirroring_20250101_Full/meta_study_set.xml.gz"
    )
    assert entry.entity == "study"
    assert entry.is_full is True
    assert str(entry.date) == "2025-01-01"
    assert entry.in_current_batch is False


def test_sra_schema_lookup_returns_populated_schema():
    run_schema = get_pyarrow_schema("run")
    assert len(run_schema) > 0
    assert run_schema.field("accession").type is not None


def test_geo_entrez_mapping():
    assert entrezid_to_geo("200123") == "GSE123"
    assert entrezid_to_geo("100456") == "GPL456"
    assert entrezid_to_geo("300789") == "GSM789"


def test_biosample_cleanup_old_files_removes_jsonl_gz_only(tmp_path: Path):
    stale = tmp_path / f"old{OUTPUT_SUFFIX}"
    keep = tmp_path / "keep.txt"
    stale.write_text("x")
    keep.write_text("y")

    cleanup_old_files(tmp_path, "biosample")

    assert not stale.exists()
    assert keep.exists()

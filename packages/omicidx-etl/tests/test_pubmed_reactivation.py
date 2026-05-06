from click.testing import CliRunner
from upath import UPath

from omicidx_etl.cli import cli
from omicidx_etl.etl import pubmed as pubmed_module


def test_pubmed_command_is_registered():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "pubmed" in result.output


def test_resolve_output_path_from_explicit_base():
    output_path = pubmed_module.resolve_output_path("s3://example-bucket")
    assert output_path == UPath("s3://example-bucket/pubmed/raw")


def test_resolve_output_path_from_env_base(monkeypatch):
    monkeypatch.setenv("PUBLISH_DIRECTORY", "s3://example-bucket")
    output_path = pubmed_module.resolve_output_path(None)
    assert "pubmed/raw" in str(output_path)


def test_get_needed_ids_without_network(monkeypatch):
    monkeypatch.setattr(
        pubmed_module,
        "load_available_urls",
        lambda: {
            "pubmed25n0001": UPath("https://example.com/pubmed25n0001.xml.gz"),
            "pubmed25n0002": UPath("https://example.com/pubmed25n0002.xml.gz"),
        },
    )
    monkeypatch.setattr(
        pubmed_module,
        "load_existing_urls",
        lambda output_path: {
            "pubmed25n0001": UPath("s3://bucket/pubmed/raw/pubmed25n0001.parquet")
        },
    )

    needed = pubmed_module.get_needed_ids(UPath("s3://bucket/pubmed/raw"), replace=False)
    assert needed == {"pubmed25n0002"}

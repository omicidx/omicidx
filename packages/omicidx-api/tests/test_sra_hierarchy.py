"""Routing/shape tests for the SRA hierarchy endpoints (#100).

These mock the async session, so they exercise routing, query-param parsing,
response envelope, and cursor round-tripping — not the underlying SQL.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient
from omicidx.api.db import get_session
from omicidx.api.main import app
from omicidx.api.pagination import decode_cursor, encode_cursor

client = TestClient(app)


def _override_session(execute_side_effect):
    """Install a session whose execute() returns a result with the given scalars."""
    sess = AsyncMock()

    def _execute(_stmt):
        scalars = execute_side_effect.pop(0) if execute_side_effect else []
        result = MagicMock()
        result.scalars.return_value.all.return_value = scalars
        return result

    sess.execute = AsyncMock(side_effect=_execute)

    async def override():
        yield sess

    app.dependency_overrides[get_session] = override
    return sess


def _clear_overrides():
    app.dependency_overrides.clear()


# -- /studies/{srp}/samples ----------------------------------------------------


def test_study_samples_ids_default():
    _override_session([["SRS1", "SRS2", "SRS3"]])
    try:
        r = client.get("/v1/sra/studies/SRP123/samples?limit=10")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == [
            {"accession": "SRS1"},
            {"accession": "SRS2"},
            {"accession": "SRS3"},
        ]
        assert body["meta"]["count"] == 3
        assert body["meta"]["cursor"]["next"] is None
        assert body["links"]["self"] == "/v1/sra/studies/SRP123/samples?limit=10"
    finally:
        _clear_overrides()


def test_study_samples_pagination_emits_next_cursor():
    # limit+1 returned → has_next is true; cursor encodes the limit-th accession
    _override_session([["SRS1", "SRS2", "SRS3"]])
    try:
        r = client.get("/v1/sra/studies/SRP123/samples?limit=2")
        assert r.status_code == 200
        body = r.json()
        assert [d["accession"] for d in body["data"]] == ["SRS1", "SRS2"]
        next_cursor = body["meta"]["cursor"]["next"]
        assert next_cursor is not None
        assert decode_cursor(next_cursor).after == "SRS2"
    finally:
        _clear_overrides()


def test_study_samples_summary_returns_full_rows():
    rows = [
        SimpleNamespace(
            accession="SRS1",
            data={"accession": "SRS1", "organism": "Homo sapiens"},
        ),
        SimpleNamespace(
            accession="SRS2",
            data={"accession": "SRS2", "organism": "Mus musculus"},
        ),
    ]
    _override_session([rows])
    try:
        r = client.get("/v1/sra/studies/SRP123/samples?hydrate=summary")
        assert r.status_code == 200
        body = r.json()
        assert body["data"][0]["organism"] == "Homo sapiens"
        assert body["data"][1]["organism"] == "Mus musculus"
    finally:
        _clear_overrides()


def test_study_samples_empty():
    _override_session([[]])
    try:
        r = client.get("/v1/sra/studies/SRP_NONE/samples")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["meta"]["count"] == 0
    finally:
        _clear_overrides()


# -- /studies/{srp}/experiments ------------------------------------------------


def test_study_experiments_ids():
    _override_session([["SRX1", "SRX2"]])
    try:
        r = client.get("/v1/sra/studies/SRP123/experiments")
        assert r.status_code == 200
        assert [d["accession"] for d in r.json()["data"]] == ["SRX1", "SRX2"]
    finally:
        _clear_overrides()


# -- /studies/{srp}/runs -------------------------------------------------------


def test_study_runs_ids():
    _override_session([["SRR1", "SRR2"]])
    try:
        r = client.get("/v1/sra/studies/SRP123/runs")
        assert r.status_code == 200
        assert [d["accession"] for d in r.json()["data"]] == ["SRR1", "SRR2"]
    finally:
        _clear_overrides()


def test_study_runs_cursor_round_trip():
    cursor = encode_cursor("SRR123")
    _override_session([["SRR456"]])
    try:
        r = client.get(f"/v1/sra/studies/SRP123/runs?cursor={cursor}&limit=25")
        assert r.status_code == 200
        body = r.json()
        # cursor_param is preserved in the self link
        assert f"cursor={cursor}" in body["links"]["self"]
    finally:
        _clear_overrides()


# -- /samples/{srs}/experiments ------------------------------------------------


def test_sample_experiments_ids():
    _override_session([["SRX1", "SRX2"]])
    try:
        r = client.get("/v1/sra/samples/SRS123/experiments")
        assert r.status_code == 200
        assert [d["accession"] for d in r.json()["data"]] == ["SRX1", "SRX2"]
    finally:
        _clear_overrides()


# -- /samples/{srs}/runs -------------------------------------------------------


def test_sample_runs_ids():
    _override_session([["SRR1"]])
    try:
        r = client.get("/v1/sra/samples/SRS123/runs")
        assert r.status_code == 200
        assert r.json()["data"] == [{"accession": "SRR1"}]
    finally:
        _clear_overrides()


# -- /experiments/{srx}/runs ---------------------------------------------------


def test_experiment_runs_ids():
    _override_session([["SRR1", "SRR2", "SRR3"]])
    try:
        r = client.get("/v1/sra/experiments/SRX123/runs?limit=2")
        assert r.status_code == 200
        body = r.json()
        assert [d["accession"] for d in body["data"]] == ["SRR1", "SRR2"]
        assert body["meta"]["cursor"]["next"] is not None
    finally:
        _clear_overrides()


def test_experiment_runs_summary():
    rows = [
        SimpleNamespace(
            accession="SRR1",
            data={"accession": "SRR1", "total_spots": 1000},
        ),
    ]
    _override_session([rows])
    try:
        r = client.get("/v1/sra/experiments/SRX123/runs?hydrate=summary")
        assert r.status_code == 200
        assert r.json()["data"][0]["total_spots"] == 1000
    finally:
        _clear_overrides()


# -- entity GET relationships block --------------------------------------------


def test_get_study_includes_collection_relationships():
    row = SimpleNamespace(
        accession="SRP123",
        bioproject="PRJNA123",
        data={"accession": "SRP123", "title": "Test study"},
    )
    sess = AsyncMock()
    sess.get = AsyncMock(return_value=row)

    async def override():
        yield sess

    app.dependency_overrides[get_session] = override
    try:
        r = client.get("/v1/sra/studies/SRP123")
        assert r.status_code == 200
        rels = r.json()["relationships"]
        assert rels["bioproject"]["accession"] == "PRJNA123"
        assert rels["samples"]["href"] == "/v1/sra/studies/SRP123/samples"
        assert rels["experiments"]["href"] == "/v1/sra/studies/SRP123/experiments"
        assert rels["runs"]["href"] == "/v1/sra/studies/SRP123/runs"
        # CollectionRelationship omits accession
        assert "accession" not in rels["samples"]
    finally:
        _clear_overrides()


def test_get_sample_includes_collection_relationships():
    row = SimpleNamespace(
        accession="SRS123",
        biosample="SAMN123",
        data={"accession": "SRS123"},
    )
    sess = AsyncMock()
    sess.get = AsyncMock(return_value=row)

    async def override():
        yield sess

    app.dependency_overrides[get_session] = override
    try:
        r = client.get("/v1/sra/samples/SRS123")
        rels = r.json()["relationships"]
        assert rels["biosample"]["accession"] == "SAMN123"
        assert rels["experiments"]["href"] == "/v1/sra/samples/SRS123/experiments"
        assert rels["runs"]["href"] == "/v1/sra/samples/SRS123/runs"
    finally:
        _clear_overrides()


def test_get_experiment_includes_runs_collection():
    row = SimpleNamespace(
        accession="SRX123",
        sample_accession="SRS1",
        study_accession="SRP1",
        data={"accession": "SRX123"},
    )
    sess = AsyncMock()
    sess.get = AsyncMock(return_value=row)

    async def override():
        yield sess

    app.dependency_overrides[get_session] = override
    try:
        r = client.get("/v1/sra/experiments/SRX123")
        rels = r.json()["relationships"]
        assert rels["sample"]["accession"] == "SRS1"
        assert rels["study"]["accession"] == "SRP1"
        assert rels["runs"]["href"] == "/v1/sra/experiments/SRX123/runs"
    finally:
        _clear_overrides()

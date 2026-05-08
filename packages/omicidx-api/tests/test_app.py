"""Test the FastAPI app routes and health endpoint.

These tests use FastAPI's TestClient and don't require a database connection
for the non-DB endpoints.
"""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from omicidx.api.main import app

client = TestClient(app)


def test_health():
    response = client.get("/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_root():
    response = client.get("/v1/")
    data = response.json()
    assert data["name"] == "OmicIDX API"
    assert "bioproject" in data["entities"]
    assert "biosample" in data["entities"]


def test_docs_available():
    response = client.get("/docs")
    assert response.status_code == 200


def test_bioproject_not_found():
    with patch("omicidx.api.routers.bioproject.get_session") as mock_session:
        mock_sess = AsyncMock()
        mock_sess.get.return_value = None
        mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_sess)

        # Override the dependency
        from omicidx.api.db import get_session

        async def override_session():
            yield mock_sess

        app.dependency_overrides[get_session] = override_session
        try:
            response = client.get("/v1/bioproject/NONEXISTENT")
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()
        finally:
            app.dependency_overrides.clear()


def test_biosample_not_found():
    from omicidx.api.db import get_session

    mock_sess = AsyncMock()
    mock_sess.get.return_value = None

    async def override_session():
        yield mock_sess

    app.dependency_overrides[get_session] = override_session
    try:
        response = client.get("/v1/biosample/NONEXISTENT")
        assert response.status_code == 404
    finally:
        app.dependency_overrides.clear()


def test_sra_study_not_found():
    from omicidx.api.db import get_session

    mock_sess = AsyncMock()
    mock_sess.get.return_value = None

    async def override_session():
        yield mock_sess

    app.dependency_overrides[get_session] = override_session
    try:
        response = client.get("/v1/sra/studies/NONEXISTENT")
        assert response.status_code == 404
    finally:
        app.dependency_overrides.clear()


def test_pubmed_not_found():
    from omicidx.api.db import get_session

    mock_sess = AsyncMock()
    mock_sess.get.return_value = None

    async def override_session():
        yield mock_sess

    app.dependency_overrides[get_session] = override_session
    try:
        response = client.get("/v1/pubmed/99999999")
        assert response.status_code == 404
    finally:
        app.dependency_overrides.clear()

"""Tests for rate limiting via slowapi."""

from unittest.mock import patch

from fastapi.testclient import TestClient


def _make_app(rate_limit: str = "5/minute"):
    """Create a fresh app with the given rate limit for testing."""
    with patch("omicidx.api.config.Settings.model_post_init", lambda *a, **kw: None):
        pass

    # Patch settings before importing app modules
    with patch("omicidx.api.config.settings") as mock_settings:
        mock_settings.rate_limit = rate_limit
        mock_settings.database_url = "postgresql://test@localhost/test"
        mock_settings.async_database_url = "postgresql+asyncpg://test@localhost/test"
        mock_settings.db_pool_size = 5
        mock_settings.db_max_overflow = 10
        mock_settings.default_page_size = 25
        mock_settings.max_page_size = 500

        # Force reimport of the limiter with new settings
        import importlib

        import omicidx.api.middleware.ratelimit as rl_mod

        importlib.reload(rl_mod)

        import omicidx.api.main as main_mod

        importlib.reload(main_mod)

        return main_mod.app


def test_health_exempt_from_rate_limit():
    from omicidx.api.main import app

    client = TestClient(app)
    # Health endpoint should never return 429
    for _ in range(20):
        response = client.get("/v1/health")
        assert response.status_code == 200


def test_rate_limit_headers_on_root():
    from omicidx.api.main import app

    client = TestClient(app)
    response = client.get("/v1/")
    assert response.status_code == 200
    # slowapi with headers_enabled adds X-RateLimit-Limit and X-RateLimit-Remaining
    assert "X-RateLimit-Limit" in response.headers
    assert "X-RateLimit-Remaining" in response.headers


def test_rate_limit_exceeded_returns_429():
    app = _make_app(rate_limit="2/minute")
    client = TestClient(app)

    # First two requests should succeed
    for _ in range(2):
        resp = client.get("/v1/")
        assert resp.status_code == 200

    # Third request should be rate limited
    resp = client.get("/v1/")
    assert resp.status_code == 429
    assert "Retry-After" in resp.headers


def test_rate_limit_remaining_decrements():
    app = _make_app(rate_limit="10/minute")
    client = TestClient(app)

    resp1 = client.get("/v1/")
    remaining1 = int(resp1.headers["X-RateLimit-Remaining"])

    resp2 = client.get("/v1/")
    remaining2 = int(resp2.headers["X-RateLimit-Remaining"])

    assert remaining2 == remaining1 - 1


def test_rate_limit_configurable_via_env():
    app = _make_app(rate_limit="3/minute")
    client = TestClient(app)

    assert client.get("/v1/").status_code == 200
    assert client.get("/v1/").status_code == 200
    assert client.get("/v1/").status_code == 200
    assert client.get("/v1/").status_code == 429

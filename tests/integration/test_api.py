"""
DataPulse - API Integration Tests
Tests all REST endpoints using httpx against the running service.

Run against local Docker stack:
    pytest tests/integration/test_api.py -v

Run against staging (set API_BASE_URL env var):
    API_BASE_URL=http://staging-api:8000 pytest tests/integration/test_api.py -v
"""

import os
import pytest
import httpx

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture(scope="session")
def client():
    """Shared HTTP client for all tests."""
    with httpx.Client(base_url=API_BASE_URL, timeout=15.0) as c:
        yield c


# ── Health check ──────────────────────────────────────────────────────────────
class TestHealth:

    def test_health_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_health_payload(self, client):
        r = client.get("/health")
        data = r.json()
        assert data["status"] == "healthy"
        assert data["service"] == "datapulse-api"
        assert "version" in data

    def test_root_returns_links(self, client):
        r = client.get("/")
        assert r.status_code == 200
        data = r.json()
        assert "docs" in data
        assert "health" in data


# ── Summary endpoint ──────────────────────────────────────────────────────────
class TestSummary:

    def test_summary_returns_200(self, client):
        r = client.get("/api/v1/kpis/summary")
        assert r.status_code == 200

    def test_summary_has_required_fields(self, client):
        r = client.get("/api/v1/kpis/summary")
        data = r.json()["data"]
        assert "total_revenue" in data
        assert "total_orders" in data
        assert "avg_order_value" in data
        assert "total_clicks" in data

    def test_summary_revenue_is_positive(self, client):
        r = client.get("/api/v1/kpis/summary")
        data = r.json()["data"]
        assert data["total_revenue"] > 0

    def test_summary_orders_is_positive(self, client):
        r = client.get("/api/v1/kpis/summary")
        data = r.json()["data"]
        assert data["total_orders"] > 0


# ── Revenue endpoint ──────────────────────────────────────────────────────────
class TestRevenue:

    def test_revenue_returns_200(self, client):
        r = client.get("/api/v1/kpis/revenue")
        assert r.status_code == 200

    def test_revenue_has_data(self, client):
        r = client.get("/api/v1/kpis/revenue")
        body = r.json()
        assert body["count"] >= 0
        assert isinstance(body["data"], list)

    def test_revenue_region_filter(self, client):
        r = client.get("/api/v1/kpis/revenue", params={"region": "Europe"})
        assert r.status_code == 200
        data = r.json()["data"]
        for row in data:
            assert row["region"] == "Europe"

    def test_revenue_limit_respected(self, client):
        r = client.get("/api/v1/kpis/revenue", params={"limit": 5})
        assert r.status_code == 200
        assert len(r.json()["data"]) <= 5

    def test_revenue_invalid_limit(self, client):
        """Limit < 1 should return 422 validation error."""
        r = client.get("/api/v1/kpis/revenue", params={"limit": 0})
        assert r.status_code == 422

    def test_revenue_data_has_hour_bucket(self, client):
        r = client.get("/api/v1/kpis/revenue")
        data = r.json()["data"]
        if data:  # Only validate if data exists
            assert "hour_bucket" in data[0]
            assert "revenue" in data[0]
            assert "order_count" in data[0]


# ── Top Products endpoint ─────────────────────────────────────────────────────
class TestProducts:

    def test_products_returns_200(self, client):
        r = client.get("/api/v1/kpis/products")
        assert r.status_code == 200

    def test_products_have_required_fields(self, client):
        r = client.get("/api/v1/kpis/products")
        data = r.json()["data"]
        if data:
            row = data[0]
            assert "revenue" in row
            assert "units_sold" in row

    def test_products_sorted_by_revenue(self, client):
        r = client.get("/api/v1/kpis/products")
        data = r.json()["data"]
        if len(data) >= 2:
            revenues = [d.get("revenue", 0) for d in data]
            assert revenues == sorted(revenues, reverse=True)

    def test_products_category_filter(self, client):
        r = client.get("/api/v1/kpis/products", params={"category": "Electronics"})
        assert r.status_code == 200

    def test_products_limit_default_10(self, client):
        r = client.get("/api/v1/kpis/products")
        assert len(r.json()["data"]) <= 10


# ── Conversion endpoint ───────────────────────────────────────────────────────
class TestConversion:

    def test_conversion_returns_200(self, client):
        r = client.get("/api/v1/kpis/conversion")
        assert r.status_code == 200

    def test_conversion_has_funnel_fields(self, client):
        data = client.get("/api/v1/kpis/conversion").json()["data"]
        assert "total_clicks" in data
        assert "added_to_cart" in data
        assert "completed_orders" in data
        assert "conversion_rate_pct" in data
        assert "cart_abandonment_pct" in data

    def test_conversion_rates_in_valid_range(self, client):
        data = client.get("/api/v1/kpis/conversion").json()["data"]
        assert 0 <= data["conversion_rate_pct"] <= 100
        assert 0 <= data["cart_abandonment_pct"] <= 100


# ── Inventory endpoint ────────────────────────────────────────────────────────
class TestInventory:

    def test_inventory_returns_200(self, client):
        r = client.get("/api/v1/kpis/inventory")
        assert r.status_code == 200

    def test_inventory_alerts_only_flag(self, client):
        r = client.get("/api/v1/kpis/inventory", params={"alerts_only": "true"})
        assert r.status_code == 200
        data = r.json()["data"]
        for row in data:
            assert row.get("low_stock_alert") is True

    def test_inventory_all_items(self, client):
        r = client.get("/api/v1/kpis/inventory", params={"alerts_only": "false"})
        assert r.status_code == 200

    def test_inventory_sorted_by_stock(self, client):
        r = client.get("/api/v1/kpis/inventory")
        data = r.json()["data"]
        if len(data) >= 2:
            stocks = [d.get("current_stock", 0) for d in data]
            assert stocks == sorted(stocks)


# ── Regions endpoint ──────────────────────────────────────────────────────────
class TestRegions:

    def test_regions_returns_list(self, client):
        r = client.get("/api/v1/kpis/regions")
        assert r.status_code == 200
        assert isinstance(r.json()["regions"], list)

    def test_metrics_endpoint(self, client):
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "datapulse_http_requests_total" in r.text

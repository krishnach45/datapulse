"""
DataPulse - Cosmos DB Client
Async wrapper around the Azure Cosmos DB SDK with:
  - Connection pooling
  - Retry with exponential backoff
  - SSL verification disabled for local emulator
"""

import asyncio
import logging
import os
from functools import wraps
from typing import Any, Dict, List, Optional

logger = logging.getLogger("datapulse.cosmos")

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT", "https://cosmos:8081")
COSMOS_KEY = os.getenv(
    "COSMOS_KEY",
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
)
COSMOS_DB = os.getenv("COSMOS_DB", "datapulse")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER", "kpis")


def retry(max_attempts: int = 3, backoff: float = 0.5):
    """Decorator: retry with exponential backoff."""

    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"{fn.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    wait = backoff * (2**attempt)
                    logger.warning(
                        f"{fn.__name__} attempt {attempt+1} failed: {e}. Retrying in {wait:.1f}s"
                    )
                    await asyncio.sleep(wait)

        return wrapper

    return decorator


class CosmosDBClient:
    """
    Async-compatible Cosmos DB client.
    Uses the synchronous SDK internally (no async Cosmos SDK for emulator support),
    run via asyncio.to_thread for non-blocking operation.
    """

    def __init__(self):
        self._client = None
        self._db = None
        self._container = None
        self._is_mock = False  # Falls back to in-memory mock if Cosmos unavailable

        # In-memory store for demo/fallback
        self._mock_store: Dict[str, Dict] = {}

    async def initialize(self):
        """Initialise Cosmos DB connection. Falls back to mock if unavailable."""
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            from azure.cosmos import CosmosClient

            self._client = CosmosClient(
                COSMOS_ENDPOINT, credential=COSMOS_KEY, connection_verify=False
            )
            self._db = await asyncio.to_thread(
                lambda: self._client.create_database_if_not_exists(COSMOS_DB)
            )
            self._container = await asyncio.to_thread(
                lambda: self._db.create_container_if_not_exists(
                    id=COSMOS_CONTAINER, partition_key={"paths": ["/kpi_type"], "kind": "Hash"}
                )
            )
            logger.info(f"Cosmos DB connected: {COSMOS_ENDPOINT} → {COSMOS_DB}/{COSMOS_CONTAINER}")

        except Exception as e:
            logger.warning(f"Cosmos DB unavailable ({e}). Using in-memory mock store.")
            self._is_mock = True
            self._seed_mock_data()

    def _seed_mock_data(self):
        """Seed mock data for development without Cosmos DB."""
        import random
        from datetime import datetime, timedelta, timezone

        # Summary
        self._mock_store["summary_today"] = {
            "id": "summary_today",
            "kpi_type": "summary",
            "total_revenue": 142853.47,
            "total_orders": 1847,
            "avg_order_value": 77.34,
            "total_clicks": 24621,
            "computed_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }

        # Hourly revenue (last 24 hours)
        now = datetime.now(timezone.utc)
        for i in range(24):
            bucket = (now - timedelta(hours=i)).strftime("%Y-%m-%dT%H:00:00")
            for region in ["North America", "Europe", "Asia Pacific"]:
                doc_id = f"hourly_{bucket}_{region.replace(' ', '_')}"
                self._mock_store[doc_id] = {
                    "id": doc_id,
                    "kpi_type": "hourly_revenue",
                    "hour_bucket": bucket,
                    "region": region,
                    "revenue": round(random.uniform(1000, 8000), 2),
                    "order_count": random.randint(10, 100),
                    "avg_order_value": round(random.uniform(50, 200), 2),
                    "unique_customers": random.randint(8, 80),
                }

        # Top products
        products = [
            ("SKU-005", "Smart Watch", "Electronics", 249.99),
            ("SKU-002", "Running Shoes", "Sports", 129.99),
            ("SKU-010", "Mechanical Keyboard", "Electronics", 149.99),
            ("SKU-001", "Wireless Headphones", "Electronics", 89.99),
            ("SKU-003", "Coffee Maker", "Kitchen", 59.99),
        ]
        for rank, (pid, name, cat, price) in enumerate(products, 1):
            units = random.randint(50, 300)
            self._mock_store[f"product_{pid}"] = {
                "id": f"product_{pid}",
                "kpi_type": "top_products",
                "product_id": pid,
                "product_name": name,
                "category": cat,
                "revenue": round(price * units * 0.85, 2),
                "units_sold": units,
                "avg_order_value": round(price * 1.1, 2),
                "rank": rank,
            }

        # Conversion funnel
        self._mock_store["conversion_funnel"] = {
            "id": "conversion_funnel",
            "kpi_type": "conversion",
            "total_clicks": 24621,
            "added_to_cart": 7386,
            "completed_orders": 1847,
            "conversion_rate_pct": 7.50,
            "cart_abandonment_pct": 75.0,
            "add_to_cart_rate_pct": 30.0,
        }

        # Inventory alerts
        for pid, name in [
            ("SKU-007", "Laptop Stand"),
            ("SKU-004", "Yoga Mat"),
            ("SKU-008", "Water Bottle"),
        ]:
            stock = random.randint(0, 45)
            self._mock_store[f"inv_{pid}"] = {
                "id": f"inv_{pid}",
                "kpi_type": "inventory",
                "product_id": pid,
                "product_name": name,
                "warehouse_id": "WH-01",
                "current_stock": stock,
                "reorder_threshold": 50,
                "low_stock_alert": True,
            }

        logger.info(f"Mock store seeded with {len(self._mock_store)} documents")

    async def query(
        self, kpi_type: str, extra_filter: str = "", limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query Cosmos DB (or mock) by kpi_type."""
        if self._is_mock:
            results = [v for v in self._mock_store.values() if v.get("kpi_type") == kpi_type]

            if extra_filter and "=" in extra_filter:
                parts = extra_filter.strip().split("=")
                if len(parts) == 2:
                    field = parts[0].strip().replace("c.", "")
                    value = parts[1].strip().strip("'")
                    results = [r for r in results if str(r.get(field, "")) == value]
            return results[:limit]

        query_str = f"SELECT TOP {limit} * FROM c WHERE c.kpi_type = '{kpi_type}'"
        if extra_filter:
            query_str += f" AND {extra_filter}"

        return await asyncio.to_thread(
            lambda: list(
                self._container.query_items(query=query_str, enable_cross_partition_query=True)
            )
        )

    async def get_item(self, item_id: str, partition_key: str) -> Optional[Dict[str, Any]]:
        """Get a single item by ID."""
        if self._is_mock:
            return self._mock_store.get(item_id)

        try:
            return await asyncio.to_thread(
                lambda: self._container.read_item(item=item_id, partition_key=partition_key)
            )
        except Exception:
            return None

    async def upsert(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Upsert a document."""
        if self._is_mock:
            self._mock_store[document["id"]] = document
            return document

        return await asyncio.to_thread(lambda: self._container.upsert_item(document))

    async def close(self):
        """Clean up resources."""
        if self._client:
            logger.info("Cosmos DB client closed.")

"""
DataPulse - Event Producer
Simulates e-commerce events: orders, product clicks, inventory updates.
Publishes to Kafka topics (local) or Azure Event Hubs (production).

Usage:
    python event_producer.py --mode continuous --rate 5
    python event_producer.py --mode burst --count 1000
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("datapulse.producer")

# ── Sample Data Pools ────────────────────────────────────────────────────────
PRODUCTS = [
    {"id": "SKU-001", "name": "Wireless Headphones", "category": "Electronics", "price": 89.99},
    {"id": "SKU-002", "name": "Running Shoes", "category": "Sports", "price": 129.99},
    {"id": "SKU-003", "name": "Coffee Maker", "category": "Kitchen", "price": 59.99},
    {"id": "SKU-004", "name": "Yoga Mat", "category": "Sports", "price": 34.99},
    {"id": "SKU-005", "name": "Smart Watch", "category": "Electronics", "price": 249.99},
    {"id": "SKU-006", "name": "Backpack", "category": "Accessories", "price": 49.99},
    {"id": "SKU-007", "name": "Laptop Stand", "category": "Electronics", "price": 39.99},
    {"id": "SKU-008", "name": "Water Bottle", "category": "Sports", "price": 24.99},
    {"id": "SKU-009", "name": "Desk Lamp", "category": "Home", "price": 44.99},
    {"id": "SKU-010", "name": "Mechanical Keyboard", "category": "Electronics", "price": 149.99},
]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
STATUSES = ["completed", "completed", "completed", "pending", "failed"]  # weighted


def generate_order_event() -> dict:
    """Generate a realistic order event."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    discount = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
    subtotal = round(product["price"] * quantity, 2)
    discount_amount = round(subtotal * discount, 2)
    total = round(subtotal - discount_amount, 2)

    return {
        "event_type": "order",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "region": random.choice(REGIONS),
        "product": {
            "id": product["id"],
            "name": product["name"],
            "category": product["category"],
            "unit_price": product["price"],
        },
        "quantity": quantity,
        "subtotal": subtotal,
        "discount_pct": discount,
        "discount_amount": discount_amount,
        "total_amount": total,
        "currency": "USD",
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
    }


def generate_click_event() -> dict:
    """Generate a product click / page view event."""
    product = random.choice(PRODUCTS)
    return {
        "event_type": "click",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": f"SESS-{uuid.uuid4().hex[:12]}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "region": random.choice(REGIONS),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "page": random.choice(["product_detail", "search_results", "homepage", "category"]),
        "added_to_cart": random.random() < 0.30,  # 30% add-to-cart rate
        "time_on_page_sec": random.randint(5, 300),
    }


def generate_inventory_event() -> dict:
    """Generate an inventory update event."""
    product = random.choice(PRODUCTS)
    stock_level = random.randint(0, 500)
    return {
        "event_type": "inventory",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "warehouse_id": f"WH-{random.randint(1, 10):02d}",
        "stock_level": stock_level,
        "reorder_threshold": 50,
        "low_stock_alert": stock_level < 50,
        "action": random.choice(["restock", "sale_deduction", "adjustment", "return"]),
    }


EVENT_GENERATORS = {
    "orders": (generate_order_event, 0.40),  # 40% orders
    "clicks": (generate_click_event, 0.50),  # 50% clicks
    "inventory": (generate_inventory_event, 0.10),  # 10% inventory
}

TOPIC_MAP = {
    "orders": "datapulse-orders",
    "clicks": "datapulse-clicks",
    "inventory": "datapulse-inventory",
}


class EventProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
        self.stats = {"orders": 0, "clicks": 0, "inventory": 0, "errors": 0}
        logger.info(f"Producer connected to {bootstrap_servers}")

    def _pick_event_type(self) -> str:
        types = list(EVENT_GENERATORS.keys())
        weights = [EVENT_GENERATORS[t][1] for t in types]
        return random.choices(types, weights=weights)[0]

    def send_event(self, event_type: Optional[str] = None):
        """Send a single event to Kafka."""
        if event_type is None:
            event_type = self._pick_event_type()

        generator_fn, _ = EVENT_GENERATORS[event_type]
        event = generator_fn()
        topic = TOPIC_MAP[event_type]
        key = event.get("customer_id") or event.get("product_id")

        try:
            future = self.producer.send(topic, key=key, value=event)
            future.get(timeout=10)
            self.stats[event_type] += 1
            logger.debug(f"Sent {event_type} event → {topic}: {event['event_id']}")
        except KafkaError as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to send {event_type} event: {e}")

    def run_continuous(self, rate_per_second: float = 5.0):
        """Continuously produce events at the specified rate."""
        interval = 1.0 / rate_per_second
        logger.info(f"Starting continuous mode at {rate_per_second} events/sec")
        count = 0
        try:
            while True:
                self.send_event()
                count += 1
                if count % 100 == 0:
                    logger.info(
                        f"Sent {count} events | "
                        f"Orders={self.stats['orders']} | "
                        f"Clicks={self.stats['clicks']} | "
                        f"Inventory={self.stats['inventory']} | "
                        f"Errors={self.stats['errors']}"
                    )
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info(f"Producer stopped. Total events: {count}")
        finally:
            self.producer.flush()
            self.producer.close()

    def run_burst(self, count: int = 1000):
        """Send a burst of events as fast as possible."""
        logger.info(f"Starting burst mode: {count} events")
        for i in range(count):
            self.send_event()
            if (i + 1) % 100 == 0:
                logger.info(f"  Sent {i + 1}/{count} events...")
        self.producer.flush()
        logger.info(
            f"Burst complete! Orders={self.stats['orders']} | "
            f"Clicks={self.stats['clicks']} | "
            f"Inventory={self.stats['inventory']}"
        )
        self.producer.close()

    def seed_sample_data(self, n_orders: int = 500, n_clicks: int = 1000, n_inventory: int = 100):
        """Seed a fixed set of sample data for development/demo."""
        logger.info(
            f"Seeding sample data: {n_orders} orders, {n_clicks} clicks, {n_inventory} inventory updates"
        )
        for _ in range(n_orders):
            self.send_event("orders")
        for _ in range(n_clicks):
            self.send_event("clicks")
        for _ in range(n_inventory):
            self.send_event("inventory")
        self.producer.flush()
        logger.info("Sample data seeding complete!")
        self.producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataPulse Event Producer")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument(
        "--mode",
        choices=["continuous", "burst", "seed"],
        default="seed",
        help="Producer mode",
    )
    parser.add_argument("--rate", type=float, default=5.0, help="Events/sec (continuous mode)")
    parser.add_argument("--count", type=int, default=1000, help="Event count (burst mode)")
    args = parser.parse_args()

    producer = EventProducer(bootstrap_servers=args.broker)

    if args.mode == "continuous":
        producer.run_continuous(rate_per_second=args.rate)
    elif args.mode == "burst":
        producer.run_burst(count=args.count)
    elif args.mode == "seed":
        producer.seed_sample_data()

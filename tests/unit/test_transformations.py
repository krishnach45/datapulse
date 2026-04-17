def make_order(
    event_id="EVT-001",
    order_id="ORD-001",
    customer_id="CUST-1",
    region="Europe",
    total_amount=50.0,
    discount_pct=0.0,
    status="completed",
    payment_method="paypal",
    kafka_offset=1,
):
    return dict(
        event_id=event_id,
        order_id=order_id,
        customer_id=customer_id,
        region=region,
        total_amount=total_amount,
        discount_pct=discount_pct,
        status=status,
        payment_method=payment_method,
        kafka_offset=kafka_offset,
    )


def make_click(
    event_id="EVT-001",
    session_id="SESS-001",
    customer_id="CUST-1",
    product_id="SKU-001",
    added_to_cart=True,
    time_on_page_sec=120,
):
    return dict(
        event_id=event_id,
        session_id=session_id,
        customer_id=customer_id,
        product_id=product_id,
        added_to_cart=added_to_cart,
        time_on_page_sec=time_on_page_sec,
    )


def filter_valid_orders(orders):
    return [
        o
        for o in orders
        if o["event_id"]
        and o["order_id"]
        and o["customer_id"]
        and o["total_amount"] > 0
        and o["status"]
    ]


def normalise_order(order):
    return {
        **order,
        "customer_id": order["customer_id"].strip().upper(),
        "status": order["status"].strip().lower(),
        "is_high_value": order["total_amount"] >= 200.0,
        "is_discounted": order["discount_pct"] > 0,
    }


def deduplicate(rows, key="event_id"):
    seen = {}
    for row in rows:
        k = row[key]
        if k not in seen or row["kafka_offset"] > seen[k]["kafka_offset"]:
            seen[k] = row
    return list(seen.values())


def filter_valid_clicks(clicks):
    return [c for c in clicks if c["event_id"] and c["session_id"] and c["product_id"]]


def fix_time_on_page(click):
    return {**click, "time_on_page_sec": abs(click["time_on_page_sec"])}


def compute_revenue(orders):
    return sum(o["total_amount"] for o in orders if o["status"] == "completed")


def compute_conversion_rate(total_clicks, total_orders):
    return round(total_orders / total_clicks * 100, 2) if total_clicks > 0 else 0.0


def compute_cart_abandonment(added_to_cart, completed_orders):
    return round((1 - completed_orders / added_to_cart) * 100, 2) if added_to_cart > 0 else 0.0


class TestOrderCleaning:
    def test_null_event_id_is_filtered(self):
        orders = [make_order(event_id=None), make_order(event_id="EVT-002", order_id="ORD-002")]
        assert len(filter_valid_orders(orders)) == 1

    def test_zero_amount_is_filtered(self):
        orders = [make_order(total_amount=0.0), make_order(event_id="EVT-002", total_amount=50.0)]
        assert len(filter_valid_orders(orders)) == 1

    def test_negative_amount_is_filtered(self):
        assert filter_valid_orders([make_order(total_amount=-10.0)]) == []

    def test_status_normalisation(self):
        assert normalise_order(make_order(status="  COMPLETED  "))["status"] == "completed"

    def test_customer_id_uppercased(self):
        assert normalise_order(make_order(customer_id="cust-1234"))["customer_id"] == "CUST-1234"

    def test_deduplication_keeps_latest_offset(self):
        orders = [
            make_order(event_id="EVT-DUP", kafka_offset=1),
            make_order(event_id="EVT-DUP", kafka_offset=5),
        ]
        result = deduplicate(orders)
        assert len(result) == 1
        assert result[0]["kafka_offset"] == 5

    def test_high_value_flag(self):
        assert normalise_order(make_order(total_amount=250.0))["is_high_value"] is True
        assert normalise_order(make_order(total_amount=99.0))["is_high_value"] is False

    def test_discounted_flag(self):
        assert normalise_order(make_order(discount_pct=0.10))["is_discounted"] is True
        assert normalise_order(make_order(discount_pct=0.0))["is_discounted"] is False


class TestClickCleaning:
    def test_null_session_filtered(self):
        clicks = [
            make_click(event_id=None, session_id=None),
            make_click(event_id="EVT-002", session_id="SESS-002"),
        ]
        assert len(filter_valid_clicks(clicks)) == 1

    def test_negative_time_on_page_handled(self):
        assert fix_time_on_page(make_click(time_on_page_sec=-30))["time_on_page_sec"] == 30

    def test_valid_clicks_pass_through(self):
        assert (
            len(
                filter_valid_clicks(
                    [make_click(), make_click(event_id="EVT-002", session_id="SESS-002")]
                )
            )
            == 2
        )


class TestGoldAggregation:
    def test_revenue_sum_completed_only(self):
        orders = [
            make_order(total_amount=100.0, status="completed"),
            make_order(event_id="EVT-002", total_amount=200.0, status="failed"),
            make_order(event_id="EVT-003", total_amount=50.0, status="completed"),
        ]
        assert compute_revenue(orders) == 150.0

    def test_conversion_rate_calculation(self):
        assert compute_conversion_rate(1000, 75) == 7.5

    def test_cart_abandonment_calculation(self):
        assert compute_cart_abandonment(300, 75) == 75.0

    def test_zero_clicks_returns_zero_rate(self):
        assert compute_conversion_rate(0, 0) == 0.0

    def test_full_conversion_rate(self):
        assert compute_conversion_rate(100, 100) == 100.0


class TestEdgeCases:
    def test_empty_order_list(self):
        assert filter_valid_orders([]) == []

    def test_all_orders_valid(self):
        orders = [make_order(), make_order(event_id="EVT-002", order_id="ORD-002")]
        assert len(filter_valid_orders(orders)) == 2

    def test_dedup_single_record(self):
        assert len(deduplicate([make_order()])) == 1

    def test_revenue_empty_list(self):
        assert compute_revenue([]) == 0.0

    def test_all_failed_orders_zero_revenue(self):
        orders = [make_order(status="failed"), make_order(event_id="EVT-002", status="pending")]
        assert compute_revenue(orders) == 0.0

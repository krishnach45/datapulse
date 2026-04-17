"""
DataPulse - Gold Aggregation Job
Reads Silver Delta tables, computes business KPIs, and writes
Gold aggregates to:
  1. Delta Lake Gold tables (for historical querying)
  2. Azure Cosmos DB (for real-time API serving)

KPIs computed:
  - Hourly revenue & order counts by region
  - Top products by revenue and units sold
  - Conversion rate (clicks → orders)
  - Cart abandonment rate
  - Inventory low-stock alerts

Medallion Architecture:
  Kafka → Bronze → Silver → [Gold] → Cosmos DB

Run:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 gold_aggregate.py
"""

import os
import json
import logging
import ssl
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, round as spark_round, hour, date_trunc,
    current_timestamp, lit, when, countDistinct, window
)
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datapulse.gold")

# ── Config ───────────────────────────────────────────────────────────────────
DELTA_BASE_PATH  = os.getenv("DELTA_BASE_PATH",  "/tmp/datapulse/delta")
COSMOS_ENDPOINT  = os.getenv("COSMOS_ENDPOINT",  "https://localhost:8081")
COSMOS_KEY       = os.getenv("COSMOS_KEY",       "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
COSMOS_DB        = os.getenv("COSMOS_DB",        "datapulse")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER", "kpis")


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("DataPulse-Gold-Aggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── KPI 1: Hourly Revenue by Region ──────────────────────────────────────────
def compute_hourly_revenue(orders_df: DataFrame) -> DataFrame:
    """Hourly revenue and order metrics, partitioned by region."""
    return (
        orders_df
        .filter(col("status") == "completed")
        .withColumn("hour_bucket", date_trunc("hour", col("event_ts")))
        .groupBy("hour_bucket", "region")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("revenue"),
            count("order_id").alias("order_count"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            spark_round(spark_sum("discount_amount"), 2).alias("total_discounts"),
            countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("kpi_type",       lit("hourly_revenue"))
        .withColumn("computed_at",    current_timestamp())
    )


# ── KPI 2: Top Products ───────────────────────────────────────────────────────
def compute_top_products(orders_df: DataFrame) -> DataFrame:
    """Top 10 products by revenue, including units sold and AOV."""
    return (
        orders_df
        .filter(col("status") == "completed")
        .groupBy("product.id", "product.name", "product.category")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("revenue"),
            spark_sum("quantity").alias("units_sold"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            count("order_id").alias("order_count"),
        )
        .orderBy(col("revenue").desc())
        .limit(10)
        .withColumn("kpi_type",    lit("top_products"))
        .withColumn("computed_at", current_timestamp())
    )


# ── KPI 3: Conversion Rate ────────────────────────────────────────────────────
def compute_conversion_rate(orders_df: DataFrame, clicks_df: DataFrame) -> DataFrame:
    """
    Conversion rate = completed orders / total product page clicks.
    Also computes cart_to_order rate.
    """
    total_clicks    = clicks_df.count()
    added_to_cart   = clicks_df.filter(col("added_to_cart") == True).count()
    total_orders    = orders_df.filter(col("status") == "completed").count()

    conversion_rate     = round(total_orders / total_clicks * 100, 2) if total_clicks > 0 else 0.0
    cart_abandonment    = round((1 - total_orders / added_to_cart) * 100, 2) if added_to_cart > 0 else 0.0
    add_to_cart_rate    = round(added_to_cart / total_clicks * 100, 2) if total_clicks > 0 else 0.0

    spark = SparkSession.getActiveSession()
    data = [{
        "kpi_type":           "conversion_funnel",
        "total_clicks":       total_clicks,
        "added_to_cart":      added_to_cart,
        "completed_orders":   total_orders,
        "conversion_rate_pct": conversion_rate,
        "cart_abandonment_pct": cart_abandonment,
        "add_to_cart_rate_pct": add_to_cart_rate,
    }]
    return spark.createDataFrame(data).withColumn("computed_at", current_timestamp())


# ── KPI 4: Inventory Alerts ───────────────────────────────────────────────────
def compute_inventory_alerts(inventory_df: DataFrame) -> DataFrame:
    """Current stock levels and low-stock alerts per product."""
    return (
        inventory_df
        .groupBy("product_id", "product_name", "warehouse_id")
        .agg(
            spark_max("stock_level").alias("current_stock"),
            spark_max("reorder_threshold").alias("reorder_threshold"),
            spark_max("low_stock_alert").alias("low_stock_alert"),
        )
        .withColumn("kpi_type",    lit("inventory_alerts"))
        .withColumn("computed_at", current_timestamp())
    )


# ── KPI 5: Summary Dashboard ──────────────────────────────────────────────────
def compute_summary(orders_df: DataFrame, clicks_df: DataFrame) -> DataFrame:
    """Single-row overall summary for the dashboard header."""
    completed   = orders_df.filter(col("status") == "completed")
    total_rev   = completed.agg(spark_sum("total_amount")).collect()[0][0] or 0
    total_ord   = completed.count()
    avg_ord_val = round(total_rev / total_ord, 2) if total_ord > 0 else 0
    total_clk   = clicks_df.count()

    spark = SparkSession.getActiveSession()
    data = [{
        "id":                str("summary_" + datetime.now(timezone.utc).strftime("%Y%m%d")),
        "kpi_type":          "summary",
        "total_revenue":     round(total_rev, 2),
        "total_orders":      total_ord,
        "avg_order_value":   avg_ord_val,
        "total_clicks":      total_clk,
        "computed_date":     datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }]
    return spark.createDataFrame(data).withColumn("computed_at", current_timestamp())


# ── Cosmos DB Writer ──────────────────────────────────────────────────────────
def write_to_cosmos(df: DataFrame, kpi_type: str):
    """Write Gold DataFrame to Cosmos DB using Python SDK (driver-side collect)."""
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        from azure.cosmos import CosmosClient, exceptions

        client    = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY, connection_verify=False)
        database  = client.get_database_client(COSMOS_DB)
        container = database.get_container_client(COSMOS_CONTAINER)

        rows = df.collect()
        upserted = 0
        for row in rows:
            doc = row.asDict(recursive=True)
            # Convert timestamps to ISO strings for JSON serialisation
            for k, v in doc.items():
                if hasattr(v, "isoformat"):
                    doc[k] = v.isoformat()
            # Cosmos DB requires an 'id' field
            if "id" not in doc:
                doc["id"] = f"{kpi_type}_{doc.get('hour_bucket', doc.get('product_id', 'row'))}_{doc.get('region', '')}"
            doc["kpi_type"] = kpi_type

            container.upsert_item(doc)
            upserted += 1

        logger.info(f"Cosmos DB upsert complete: {upserted} documents → {kpi_type}")

    except Exception as e:
        logger.error(f"Cosmos DB write failed for {kpi_type}: {e}")
        logger.info("Falling back: writing to local JSON for debugging")
        _write_json_fallback(df, kpi_type)


def _write_json_fallback(df: DataFrame, kpi_type: str):
    """Write to local JSON when Cosmos DB is unavailable."""
    out_path = f"/tmp/datapulse/gold_output/{kpi_type}"
    df.write.mode("overwrite").json(out_path)
    logger.info(f"Fallback JSON written → {out_path}")


# ── Write Gold Delta ──────────────────────────────────────────────────────────
def write_gold_delta(df: DataFrame, kpi_type: str):
    gold_path = f"{DELTA_BASE_PATH}/gold/{kpi_type}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(gold_path)
    )
    logger.info(f"Gold Delta written → {gold_path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Loading Silver data...")
    orders_df    = spark.read.format("delta").load(f"{DELTA_BASE_PATH}/silver/orders")
    clicks_df    = spark.read.format("delta").load(f"{DELTA_BASE_PATH}/silver/clicks")
    inventory_df = spark.read.format("delta").load(f"{DELTA_BASE_PATH}/silver/inventory")

    jobs = [
        ("hourly_revenue",  compute_hourly_revenue(orders_df)),
        ("top_products",    compute_top_products(orders_df)),
        ("conversion",      compute_conversion_rate(orders_df, clicks_df)),
        ("inventory",       compute_inventory_alerts(inventory_df)),
        ("summary",         compute_summary(orders_df, clicks_df)),
    ]

    for kpi_type, df in jobs:
        logger.info(f"Processing Gold KPI: {kpi_type}")
        df.cache()
        write_gold_delta(df, kpi_type)
        write_to_cosmos(df, kpi_type)
        df.unpersist()

    logger.info("Gold aggregation complete!")


if __name__ == "__main__":
    main()

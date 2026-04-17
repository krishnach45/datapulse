"""
DataPulse - Bronze Ingestion Job
Reads raw JSON events from Kafka (Event Hubs) using Structured Streaming
and writes them as-is to Delta Lake Bronze layer with minimal schema enforcement.

Medallion Architecture:
  Kafka → [Bronze] → Silver → Gold → Cosmos DB

Run locally:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    io.delta:delta-spark_2.12:3.1.0 bronze_ingest.py
"""

import logging
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datapulse.bronze")

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "/tmp/datapulse/delta")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/datapulse/checkpoints")

TOPICS = {
    "orders": "datapulse-orders",
    "clicks": "datapulse-clicks",
    "inventory": "datapulse-inventory",
}

# ── Schemas ───────────────────────────────────────────────────────────────────
ORDER_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField(
            "product",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("unit_price", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField("quantity", IntegerType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

CLICK_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("page", StringType(), True),
        StructField("added_to_cart", BooleanType(), True),
        StructField("time_on_page_sec", IntegerType(), True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("stock_level", IntegerType(), True),
        StructField("reorder_threshold", IntegerType(), True),
        StructField("low_stock_alert", BooleanType(), True),
        StructField("action", StringType(), True),
    ]
)

SCHEMA_MAP = {
    "orders": ORDER_SCHEMA,
    "clicks": CLICK_SCHEMA,
    "inventory": INVENTORY_SCHEMA,
}


def create_spark_session() -> SparkSession:
    """Create a Spark session with Delta Lake and Kafka support."""
    builder = (
        SparkSession.builder.appName("DataPulse-Bronze-Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_kafka_stream(spark: SparkSession, topic: str):
    """Read a Kafka topic as a streaming DataFrame."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_write_bronze(spark: SparkSession, event_type: str, schema, topic: str):
    """
    Parse raw Kafka messages against schema and write to Delta Bronze.
    Bronze = raw data with ingestion metadata appended, minimal transformation.
    """
    raw_stream = read_kafka_stream(spark, topic)

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("offset").alias("kafka_offset"),
        col("partition").alias("kafka_partition"),
        col("timestamp").alias("kafka_timestamp"),
    ).select(
        "data.*",
        "kafka_offset",
        "kafka_partition",
        "kafka_timestamp",
        current_timestamp().alias("ingested_at"),
        lit(event_type).alias("source_topic"),
    )

    bronze_path = f"{DELTA_BASE_PATH}/bronze/{event_type}"
    checkpoint = f"{CHECKPOINT_BASE}/bronze/{event_type}"

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("source_topic")
        .trigger(processingTime="10 seconds")
        .start(bronze_path)
    )

    logger.info(f"Bronze stream started → {bronze_path} (checkpoint: {checkpoint})")
    return query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting DataPulse Bronze ingestion jobs...")

    queries = []
    for event_type, topic in TOPICS.items():
        schema = SCHEMA_MAP[event_type]
        q = parse_and_write_bronze(spark, event_type, schema, topic)
        queries.append(q)

    logger.info(f"Running {len(queries)} bronze stream(s). Press Ctrl+C to stop.")

    # Wait for all streams
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()

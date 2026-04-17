"""
DataPulse - Silver Cleaning Job
Reads from Delta Bronze, applies:
  - Schema validation & null filtering
  - Deduplication by event_id
  - Type casting and standardisation
  - Bad record quarantine to _quarantine table

Medallion Architecture:
  Kafka → Bronze → [Silver] → Gold → Cosmos DB

Run locally:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 silver_clean.py
"""

import logging
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    to_timestamp,
    trim,
    upper,
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datapulse.silver")

# ── Config ───────────────────────────────────────────────────────────────────
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "/tmp/datapulse/delta")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/datapulse/checkpoints")


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("DataPulse-Silver-Cleaning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Order cleaning ────────────────────────────────────────────────────────────
def clean_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Clean orders DataFrame.
    Returns (clean_df, quarantine_df).
    """
    # Identify bad records
    bad = df.filter(
        col("event_id").isNull()
        | col("order_id").isNull()
        | col("customer_id").isNull()
        | (col("total_amount") <= 0)
        | col("status").isNull()
    ).withColumn("quarantine_reason", lit("missing_required_fields_or_invalid_amount"))

    # Clean records
    clean = (
        df.filter(
            col("event_id").isNotNull()
            & col("order_id").isNotNull()
            & col("customer_id").isNotNull()
            & (col("total_amount") > 0)
            & col("status").isNotNull()
        )
        # Standardise text fields
        .withColumn("customer_id", upper(trim(col("customer_id"))))
        .withColumn("region", trim(col("region")))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("payment_method", lower(trim(col("payment_method"))))
        .withColumn("currency", upper(trim(col("currency"))))
        # Parse timestamp
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        # Enrich: flag high-value orders
        .withColumn("is_high_value", col("total_amount") >= 200.0)
        # Enrich: flag discounted orders
        .withColumn("is_discounted", col("discount_pct") > 0)
        # Silver metadata
        .withColumn("silver_processed_at", current_timestamp())
    )

    # Dedup by event_id (keep latest kafka_offset)
    window = Window.partitionBy("event_id").orderBy(col("kafka_offset").desc())
    clean = (
        clean.withColumn("_row_num", F.row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return clean, bad


# ── Click cleaning ────────────────────────────────────────────────────────────
def clean_clicks(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    bad = df.filter(
        col("event_id").isNull() | col("session_id").isNull() | col("product_id").isNull()
    ).withColumn("quarantine_reason", lit("missing_required_click_fields"))

    clean = (
        df.filter(
            col("event_id").isNotNull()
            & col("session_id").isNotNull()
            & col("product_id").isNotNull()
        )
        .withColumn("customer_id", upper(trim(col("customer_id"))))
        .withColumn("region", trim(col("region")))
        .withColumn("category", trim(col("category")))
        .withColumn("page", lower(trim(col("page"))))
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("time_on_page_sec", spark_abs(col("time_on_page_sec")))
        .withColumn("silver_processed_at", current_timestamp())
    )

    window = Window.partitionBy("event_id").orderBy(col("kafka_offset").desc())
    clean = (
        clean.withColumn("_row_num", F.row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return clean, bad


# ── Inventory cleaning ────────────────────────────────────────────────────────
def clean_inventory(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    bad = df.filter(
        col("event_id").isNull() | col("product_id").isNull() | (col("stock_level") < 0)
    ).withColumn("quarantine_reason", lit("invalid_inventory_event"))

    clean = (
        df.filter(
            col("event_id").isNotNull() & col("product_id").isNotNull() & (col("stock_level") >= 0)
        )
        .withColumn("product_id", upper(trim(col("product_id"))))
        .withColumn("warehouse_id", upper(trim(col("warehouse_id"))))
        .withColumn("action", lower(trim(col("action"))))
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("silver_processed_at", current_timestamp())
    )

    window = Window.partitionBy("event_id").orderBy(col("kafka_offset").desc())
    clean = (
        clean.withColumn("_row_num", F.row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return clean, bad


CLEANER_MAP = {
    "orders": clean_orders,
    "clicks": clean_clicks,
    "inventory": clean_inventory,
}


def process_silver(spark: SparkSession, event_type: str):
    """Read bronze, clean, write silver + quarantine."""
    bronze_path = f"{DELTA_BASE_PATH}/bronze/{event_type}"
    silver_path = f"{DELTA_BASE_PATH}/silver/{event_type}"
    quarantine_path = f"{DELTA_BASE_PATH}/silver/_quarantine/{event_type}"

    bronze_df = spark.read.format("delta").load(bronze_path)

    cleaner = CLEANER_MAP[event_type]
    clean_df, quarantine_df = cleaner(bronze_df)

    # Write silver
    (
        clean_df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("status" if event_type == "orders" else "source_topic")
        .save(silver_path)
    )
    logger.info(f"Silver written → {silver_path} ({clean_df.count()} records)")

    # Write quarantine
    bad_count = quarantine_df.count()
    if bad_count > 0:
        (quarantine_df.write.format("delta").mode("append").save(quarantine_path))
        logger.warning(f"Quarantined {bad_count} bad {event_type} records → {quarantine_path}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting DataPulse Silver cleaning jobs...")
    for event_type in ["orders", "clicks", "inventory"]:
        logger.info(f"Processing silver: {event_type}")
        process_silver(spark, event_type)

    logger.info("Silver processing complete!")


if __name__ == "__main__":
    main()

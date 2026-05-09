from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, expr, lit, max as spark_max

CATALOG = "shopflow"
SILVER_TABLE = f"{CATALOG}.silver.orders"
ORDERS_BY_STATUS_TABLE = f"{CATALOG}.gold.orders_by_status"
PIPELINE_LATENCY_TABLE = f"{CATALOG}.gold.pipeline_latency"
STATUS_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/gold_orders_status"
LATENCY_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/gold_pipeline_latency"


spark = (
    SparkSession.builder
    .appName("shopflow-gold-orders")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ORDERS_BY_STATUS_TABLE} (
        order_status STRING,
        total_orders LONG,
        gold_updated_at TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PIPELINE_LATENCY_TABLE} (
        table_name STRING,
        avg_latency_seconds DOUBLE,
        max_latency_seconds DOUBLE,
        events_measured LONG,
        gold_updated_at TIMESTAMP
    )
    USING iceberg
""")

orders_stream = (
    spark.readStream
    .format("iceberg")
    .load(SILVER_TABLE)
    .filter(col("is_deleted") == lit(False))
)

orders_by_status = (
    orders_stream
    .filter(col("order_status").isNotNull())
    .groupBy("order_status")
    .agg(count("*").cast("long").alias("total_orders"))
    .withColumn("gold_updated_at", current_timestamp())
)

pipeline_latency = (
    orders_stream
    .filter(col("source_ts_ms").isNotNull())
    .withColumn(
        "latency_seconds",
        expr("CAST(unix_timestamp(silver_processed_at) - (source_ts_ms / 1000.0) AS DOUBLE)"),
    )
    .groupBy()
    .agg(
        avg("latency_seconds").cast("double").alias("avg_latency_seconds"),
        spark_max("latency_seconds").cast("double").alias("max_latency_seconds"),
        count("*").cast("long").alias("events_measured"),
    )
    .withColumn("table_name", expr("'orders'"))
    .withColumn("gold_updated_at", current_timestamp())
    .select(
        "table_name",
        "avg_latency_seconds",
        "max_latency_seconds",
        "events_measured",
        "gold_updated_at",
    )
)

status_query = (
    orders_by_status.writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", STATUS_CHECKPOINT)
    .toTable(ORDERS_BY_STATUS_TABLE)
)

latency_query = (
    pipeline_latency.writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", LATENCY_CHECKPOINT)
    .toTable(PIPELINE_LATENCY_TABLE)
)

print("Gold orders streaming job started.")
print(f"Reading silver table: {SILVER_TABLE}")
print(f"Writing status aggregate to: {ORDERS_BY_STATUS_TABLE}")
print(f"Writing latency aggregate to: {PIPELINE_LATENCY_TABLE}")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping gold orders streaming job...")
    for query in spark.streams.active:
        query.stop()
finally:
    spark.stop()

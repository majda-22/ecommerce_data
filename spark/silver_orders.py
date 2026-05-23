from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    to_timestamp,
    when,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

CATALOG = "shopflow"
BRONZE_TABLE = f"{CATALOG}.bronze.cdc_events"
SILVER_TABLE = f"{CATALOG}.silver.orders"
DLQ_TABLE = f"{CATALOG}.dlq.invalid_orders"
SILVER_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/silver_orders_pdf_v2"
DLQ_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/dlq_orders_pdf_v2"


spark = (
    SparkSession.builder
    .appName("shopflow-silver-orders")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.dlq")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        order_id STRING,
        customer_id STRING,
        order_status STRING,
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        operation STRING,
        source_ts_ms LONG,
        is_deleted BOOLEAN,
        kafka_timestamp TIMESTAMP,
        bronze_ingested_at TIMESTAMP,
        silver_processed_at TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
        raw_payload STRING,
        error_reason STRING,
        kafka_timestamp TIMESTAMP,
        dlq_ingested_at TIMESTAMP
    )
    USING iceberg
""")

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("order_approved_at", StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType()),
])

cdc_schema = StructType([
    StructField("before", order_schema),
    StructField("after", order_schema),
    StructField("op", StringType()),
    StructField("source", StructType([
        StructField("ts_ms", LongType()),
    ])),
])

bronze_orders = (
    spark.readStream
    .format("iceberg")
    .load(BRONZE_TABLE)
    .filter(col("source_table") == "orders")
    .filter(col("raw_payload").isNotNull())
)

parsed = (
    bronze_orders
    .withColumn("cdc", from_json(col("raw_payload"), cdc_schema))
    .withColumn("order_record", when(col("cdc.op") == "d", col("cdc.before")).otherwise(col("cdc.after")))
    .withColumn(
        "error_reason",
        when(col("cdc").isNull(), lit("invalid_json"))
        .when(col("cdc.op").isNull(), lit("missing_operation"))
        .when(col("order_record.order_id").isNull(), lit("missing_order_id"))
        .otherwise(lit(None).cast("string")),
    )
)

silver_orders = parsed.filter(col("error_reason").isNull()).select(
    col("order_record.order_id").alias("order_id"),
    col("order_record.customer_id").alias("customer_id"),
    col("order_record.order_status").alias("order_status"),
    to_timestamp(col("order_record.order_purchase_timestamp")).alias("order_purchase_timestamp"),
    to_timestamp(col("order_record.order_approved_at")).alias("order_approved_at"),
    to_timestamp(col("order_record.order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
    to_timestamp(col("order_record.order_delivered_customer_date")).alias("order_delivered_customer_date"),
    to_timestamp(col("order_record.order_estimated_delivery_date")).alias("order_estimated_delivery_date"),
    col("cdc.op").alias("operation"),
    col("cdc.source.ts_ms").alias("source_ts_ms"),
    (col("cdc.op") == "d").alias("is_deleted"),
    col("kafka_timestamp"),
    col("bronze_ingested_at"),
    current_timestamp().alias("silver_processed_at"),
)

invalid_orders = parsed.filter(col("error_reason").isNotNull()).select(
    col("raw_payload"),
    col("error_reason"),
    col("kafka_timestamp"),
    current_timestamp().alias("dlq_ingested_at"),
)

valid_query = (
    silver_orders.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .toTable(SILVER_TABLE)
)

dlq_query = (
    invalid_orders.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", DLQ_CHECKPOINT)
    .toTable(DLQ_TABLE)
)

print("Silver orders streaming job started.")
print(f"Reading bronze table: {BRONZE_TABLE}")
print(f"Writing valid rows to: {SILVER_TABLE}")
print(f"Writing invalid rows to: {DLQ_TABLE}")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping silver orders streaming job...")
    for query in spark.streams.active:
        query.stop()
finally:
    spark.stop()

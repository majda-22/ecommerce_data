from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, date_trunc, from_json, lit, sum as spark_sum, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

CATALOG = "shopflow"
BRONZE_TABLE = f"{CATALOG}.bronze.cdc_events"

PAYMENTS_BY_PERIOD_TABLE = f"{CATALOG}.gold.payments_by_period"
RETURNS_SUMMARY_TABLE = f"{CATALOG}.gold.returns_summary"
CUSTOMER_SEGMENTS_TABLE = f"{CATALOG}.gold.customer_segments"
ORDERS_BY_CHANNEL_TABLE = f"{CATALOG}.gold.orders_by_channel"

PAYMENTS_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/gold_payments_by_period_pdf_v2"
RETURNS_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/gold_returns_summary_pdf_v2"
SEGMENTS_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/gold_customer_segments_pdf_v2"
CHANNEL_CHECKPOINT = "s3a://shopflow-lakehouse/checkpoints/gold_orders_by_channel_pdf_v2"


spark = (
    SparkSession.builder
    .appName("shopflow-gold-business-metrics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PAYMENTS_BY_PERIOD_TABLE} (
        payment_period TIMESTAMP,
        payment_type STRING,
        total_payments LONG,
        total_value DOUBLE,
        gold_updated_at TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RETURNS_SUMMARY_TABLE} (
        return_status STRING,
        return_reason STRING,
        total_returns LONG,
        gold_updated_at TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CUSTOMER_SEGMENTS_TABLE} (
        customer_segment STRING,
        total_customers LONG,
        gold_updated_at TIMESTAMP
    )
    USING iceberg
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ORDERS_BY_CHANNEL_TABLE} (
        sales_channel STRING,
        total_orders LONG,
        gold_updated_at TIMESTAMP
    )
    USING iceberg
""")

payment_schema = StructType([
    StructField("payment_type", StringType()),
    StructField("payment_value", DoubleType()),
])

return_schema = StructType([
    StructField("return_status", StringType()),
    StructField("return_reason", StringType()),
])

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_segment", StringType()),
])

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("sales_channel", StringType()),
])

cdc_schema = lambda record_schema: StructType([
    StructField("before", record_schema),
    StructField("after", record_schema),
    StructField("op", StringType()),
    StructField("source", StructType([
        StructField("ts_ms", LongType()),
    ])),
])

bronze_stream = (
    spark.readStream
    .format("iceberg")
    .load(BRONZE_TABLE)
    .filter(col("raw_payload").isNotNull())
)

payments = (
    bronze_stream
    .filter(col("source_table") == "payments")
    .withColumn("cdc", from_json(col("raw_payload"), cdc_schema(payment_schema)))
    .filter(col("cdc.op") != lit("d"))
    .filter(col("cdc.after.payment_type").isNotNull())
    .groupBy(
        date_trunc("day", col("kafka_timestamp")).alias("payment_period"),
        col("cdc.after.payment_type").alias("payment_type"),
    )
    .agg(
        count("*").cast("long").alias("total_payments"),
        spark_sum(col("cdc.after.payment_value")).cast("double").alias("total_value"),
    )
    .withColumn("gold_updated_at", current_timestamp())
)

returns = (
    bronze_stream
    .filter(col("source_table") == "returns")
    .withColumn("cdc", from_json(col("raw_payload"), cdc_schema(return_schema)))
    .withColumn("return_record", when(col("cdc.op") == "d", col("cdc.before")).otherwise(col("cdc.after")))
    .filter(col("return_record.return_status").isNotNull())
    .groupBy(
        col("return_record.return_status").alias("return_status"),
        col("return_record.return_reason").alias("return_reason"),
    )
    .agg(count("*").cast("long").alias("total_returns"))
    .withColumn("gold_updated_at", current_timestamp())
)

customer_segments = (
    bronze_stream
    .filter(col("source_table") == "customers")
    .withColumn("cdc", from_json(col("raw_payload"), cdc_schema(customer_schema)))
    .filter(col("cdc.op") != lit("d"))
    .filter(col("cdc.after.customer_segment").isNotNull())
    .groupBy(col("cdc.after.customer_segment").alias("customer_segment"))
    .agg(count("*").cast("long").alias("total_customers"))
    .withColumn("gold_updated_at", current_timestamp())
)

orders_by_channel = (
    bronze_stream
    .filter(col("source_table") == "orders")
    .withColumn("cdc", from_json(col("raw_payload"), cdc_schema(order_schema)))
    .filter(col("cdc.op") != lit("d"))
    .filter(col("cdc.after.sales_channel").isNotNull())
    .groupBy(col("cdc.after.sales_channel").alias("sales_channel"))
    .agg(count("*").cast("long").alias("total_orders"))
    .withColumn("gold_updated_at", current_timestamp())
)

payments_query = (
    payments.writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", PAYMENTS_CHECKPOINT)
    .toTable(PAYMENTS_BY_PERIOD_TABLE)
)

returns_query = (
    returns.writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", RETURNS_CHECKPOINT)
    .toTable(RETURNS_SUMMARY_TABLE)
)

segments_query = (
    customer_segments.writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", SEGMENTS_CHECKPOINT)
    .toTable(CUSTOMER_SEGMENTS_TABLE)
)

channel_query = (
    orders_by_channel.writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", CHANNEL_CHECKPOINT)
    .toTable(ORDERS_BY_CHANNEL_TABLE)
)

print("Gold business metrics streaming job started.")
print(f"Reading bronze table: {BRONZE_TABLE}")
print(f"Writing payments to: {PAYMENTS_BY_PERIOD_TABLE}")
print(f"Writing returns to: {RETURNS_SUMMARY_TABLE}")
print(f"Writing segments to: {CUSTOMER_SEGMENTS_TABLE}")
print(f"Writing channels to: {ORDERS_BY_CHANNEL_TABLE}")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping gold business metrics streaming job...")
    for query in spark.streams.active:
        query.stop()
finally:
    spark.stop()

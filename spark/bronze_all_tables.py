from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_extract

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
CATALOG = "shopflow"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_events"
CHECKPOINT_LOCATION = "s3a://shopflow-lakehouse/checkpoints/bronze_all_tables"

SOURCE_TABLES = [
    "customers",
    "products",
    "sellers",
    "product_category_translation",
    "orders",
    "order_items",
    "payments",
    "reviews",
]

TOPICS = ",".join(f"shopflow.public.{table}" for table in SOURCE_TABLES)


spark = (
    SparkSession.builder
    .appName("shopflow-bronze-all-tables")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        topic STRING,
        source_table STRING,
        kafka_partition INT,
        kafka_offset LONG,
        kafka_timestamp TIMESTAMP,
        message_key STRING,
        raw_payload STRING,
        bronze_ingested_at TIMESTAMP
    )
    USING iceberg
""")

raw_events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze_events = raw_events.select(
    col("topic").cast("string").alias("topic"),
    regexp_extract(
        col("topic").cast("string"),
        r"^shopflow\.public\.([^.]+)$",
        1,
    ).alias("source_table"),
    col("partition").cast("int").alias("kafka_partition"),
    col("offset").cast("long").alias("kafka_offset"),
    col("timestamp").cast("timestamp").alias("kafka_timestamp"),
    col("key").cast("string").alias("message_key"),
    col("value").cast("string").alias("raw_payload"),
    current_timestamp().alias("bronze_ingested_at"),
)

query = (
    bronze_events.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .toTable(BRONZE_TABLE)
)

print(f"Bronze streaming job started.")
print(f"Reading Kafka topics: {TOPICS}")
print(f"Writing Iceberg table: {BRONZE_TABLE}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping bronze streaming job...")
    query.stop()
finally:
    spark.stop()

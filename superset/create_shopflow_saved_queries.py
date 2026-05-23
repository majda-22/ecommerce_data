from superset import db, security_manager
from superset.app import create_app


DATABASE_NAME = "ShopFlow Iceberg"

SAVED_QUERIES = [
    {
        "label": "ShopFlow - CDC events by table and operation",
        "schema": "bronze",
        "description": "Counts Debezium CDC events per source table and operation.",
        "sql": """
SELECT
  source_table,
  CASE json_extract_scalar(raw_payload, '$.op')
    WHEN 'c' THEN 'INSERT'
    WHEN 'r' THEN 'SNAPSHOT'
    WHEN 'u' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    ELSE COALESCE(json_extract_scalar(raw_payload, '$.op'), 'UNKNOWN')
  END AS operation,
  COUNT(*) AS total_events,
  MAX(bronze_ingested_at) AS last_ingested_at
FROM bronze.cdc_events
WHERE raw_payload IS NOT NULL
GROUP BY 1, 2
ORDER BY source_table, total_events DESC
""",
    },
    {
        "label": "ShopFlow - CDC timeline last hour",
        "schema": "bronze",
        "description": "Minute-level CDC volume for recent streaming activity.",
        "sql": """
SELECT
  date_trunc('minute', kafka_timestamp) AS event_minute,
  source_table,
  COUNT(*) AS total_events
FROM bronze.cdc_events
WHERE kafka_timestamp >= current_timestamp - INTERVAL '60' MINUTE
GROUP BY 1, 2
ORDER BY event_minute ASC, source_table
""",
    },
    {
        "label": "ShopFlow - Recent CDC events",
        "schema": "bronze",
        "description": "Latest raw CDC events captured from Kafka into Bronze.",
        "sql": """
SELECT
  source_table,
  CASE json_extract_scalar(raw_payload, '$.op')
    WHEN 'c' THEN 'INSERT'
    WHEN 'r' THEN 'SNAPSHOT'
    WHEN 'u' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    ELSE COALESCE(json_extract_scalar(raw_payload, '$.op'), 'UNKNOWN')
  END AS operation,
  kafka_timestamp,
  bronze_ingested_at,
  message_key
FROM bronze.cdc_events
WHERE raw_payload IS NOT NULL
ORDER BY kafka_timestamp DESC, kafka_offset DESC
LIMIT 100
""",
    },
    {
        "label": "ShopFlow - Order status gold KPI",
        "schema": "gold",
        "description": "Gold order status aggregate for order evolution and cancellation rate.",
        "sql": """
SELECT
  order_status,
  total_orders,
  ROUND(
    100.0 * total_orders / NULLIF(SUM(total_orders) OVER (), 0),
    2
  ) AS pct_of_orders,
  gold_updated_at
FROM gold.orders_by_status
ORDER BY total_orders DESC
""",
    },
    {
        "label": "ShopFlow - Pipeline latency",
        "schema": "gold",
        "description": "Average and maximum CDC pipeline latency by source table.",
        "sql": """
SELECT
  table_name,
  avg_latency_seconds,
  max_latency_seconds,
  events_measured,
  gold_updated_at
FROM gold.pipeline_latency
ORDER BY avg_latency_seconds DESC
""",
    },
    {
        "label": "ShopFlow - Payments by period",
        "schema": "gold",
        "description": "Payment count and value by payment period and type.",
        "sql": """
SELECT
  payment_period,
  payment_type,
  total_payments,
  total_value,
  gold_updated_at
FROM gold.payments_by_period
ORDER BY payment_period DESC, total_value DESC
""",
    },
    {
        "label": "ShopFlow - Returns and cancellations",
        "schema": "gold",
        "description": "Return/cancellation totals by status and reason.",
        "sql": """
SELECT
  return_status,
  return_reason,
  total_returns,
  ROUND(
    100.0 * total_returns / NULLIF(SUM(total_returns) OVER (), 0),
    2
  ) AS pct_of_returns,
  gold_updated_at
FROM gold.returns_summary
ORDER BY total_returns DESC
""",
    },
    {
        "label": "ShopFlow - Customer segments",
        "schema": "gold",
        "description": "Customer distribution by segment.",
        "sql": """
SELECT
  customer_segment,
  total_customers,
  ROUND(
    100.0 * total_customers / NULLIF(SUM(total_customers) OVER (), 0),
    2
  ) AS pct_of_customers,
  gold_updated_at
FROM gold.customer_segments
ORDER BY total_customers DESC
""",
    },
    {
        "label": "ShopFlow - Orders by sales channel",
        "schema": "gold",
        "description": "Order distribution by sales channel.",
        "sql": """
SELECT
  sales_channel,
  total_orders,
  ROUND(
    100.0 * total_orders / NULLIF(SUM(total_orders) OVER (), 0),
    2
  ) AS pct_of_orders,
  gold_updated_at
FROM gold.orders_by_channel
ORDER BY total_orders DESC
""",
    },
    {
        "label": "ShopFlow - Silver latest orders",
        "schema": "silver",
        "description": "Latest non-deleted Silver order state, useful for validation.",
        "sql": """
WITH ranked_orders AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    kafka_timestamp,
    silver_processed_at,
    is_deleted,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY silver_processed_at DESC, kafka_timestamp DESC
    ) AS rn
  FROM silver.orders
)
SELECT
  order_id,
  customer_id,
  order_status,
  kafka_timestamp,
  silver_processed_at
FROM ranked_orders
WHERE rn = 1
  AND NOT is_deleted
ORDER BY silver_processed_at DESC
LIMIT 100
""",
    },
]


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset.models.core import Database
        from superset.models.sql_lab import SavedQuery

        admin = security_manager.find_user(username="admin")
        if admin is None:
            raise RuntimeError("Admin user does not exist. Start Superset first.")

        database = db.session.query(Database).filter(Database.database_name == DATABASE_NAME).one_or_none()
        if database is None:
            raise RuntimeError(f"Superset database {DATABASE_NAME!r} was not found.")

        for query_def in SAVED_QUERIES:
            query = (
                db.session.query(SavedQuery)
                .filter(
                    SavedQuery.label == query_def["label"],
                    SavedQuery.db_id == database.id,
                )
                .one_or_none()
            )
            if query is None:
                query = SavedQuery(label=query_def["label"], db_id=database.id, user_id=admin.id)
                db.session.add(query)

            query.schema = query_def["schema"]
            query.catalog = None
            query.description = query_def["description"]
            query.sql = query_def["sql"].strip()
            query.template_parameters = "{}"
            query.extra_json = "{}"
            query.rows = None
            query.user_id = admin.id
            query.created_by = admin
            query.changed_by = admin

        db.session.commit()
        print(f"Created/updated {len(SAVED_QUERIES)} saved queries for {DATABASE_NAME}.")


if __name__ == "__main__":
    main()

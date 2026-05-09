from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.trino_client import run_query

app = FastAPI(title="ShopFlow CDC API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def query_or_503(query_fn: Callable[[], Any]) -> Any:
    try:
        return query_fn()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Trino query failed: {exc}",
        ) from exc


@app.get("/")
def root():
    return {"status": "ok", "service": "ShopFlow CDC API"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/business/orders-by-status")
def orders_by_status():
    return query_or_503(lambda: run_query("""
        SELECT order_status, total_orders, gold_updated_at
        FROM iceberg.gold.orders_by_status
        ORDER BY total_orders DESC
    """))


@app.get("/api/business/total-orders")
def total_orders():
    rows = query_or_503(lambda: run_query("""
        SELECT COALESCE(SUM(total_orders), 0) AS total_orders
        FROM iceberg.gold.orders_by_status
    """))

    return rows[0] if rows else {"total_orders": 0}


@app.get("/api/business/cancellation-rate")
def cancellation_rate():
    rows = query_or_503(lambda: run_query("""
        SELECT COALESCE(
            ROUND(
                100.0 * SUM(CASE WHEN order_status = 'canceled' THEN total_orders ELSE 0 END)
                / NULLIF(SUM(total_orders), 0),
                2
            ),
            0
        ) AS cancellation_rate
        FROM iceberg.gold.orders_by_status
    """))

    return rows[0] if rows else {"cancellation_rate": 0}


@app.get("/api/business/top-categories")
def top_categories():
    return query_or_503(lambda: run_query("""
        WITH product_events AS (
            SELECT
                json_extract_scalar(raw_payload, '$.after.product_category_name') AS category
            FROM iceberg.bronze.cdc_events
            WHERE source_table = 'products'
              AND json_extract_scalar(raw_payload, '$.after.product_category_name') IS NOT NULL
        )
        SELECT
            replace(category, '_', ' ') AS category,
            COUNT(*) AS total_events
        FROM product_events
        GROUP BY category
        ORDER BY total_events DESC
        LIMIT 4
    """))


@app.get("/api/business/review-score")
def review_score():
    rows = query_or_503(lambda: run_query("""
        SELECT
            COALESCE(
                ROUND(
                    AVG(CAST(json_extract_scalar(raw_payload, '$.after.review_score') AS DOUBLE)),
                    2
                ),
                0
            ) AS review_score
        FROM iceberg.bronze.cdc_events
        WHERE source_table = 'reviews'
          AND json_extract_scalar(raw_payload, '$.after.review_score') IS NOT NULL
    """))

    return rows[0] if rows else {"review_score": 0}


@app.get("/api/business/revenue-summary")
def revenue_summary():
    rows = query_or_503(lambda: run_query("""
        SELECT
            COALESCE(
                SUM(CAST(json_extract_scalar(raw_payload, '$.after.payment_value') AS DOUBLE)),
                0
            ) AS total_revenue,
            COUNT(*) AS payment_events
        FROM iceberg.bronze.cdc_events
        WHERE source_table = 'payments'
          AND json_extract_scalar(raw_payload, '$.after.payment_value') IS NOT NULL
          AND COALESCE(json_extract_scalar(raw_payload, '$.op'), '') <> 'd'
    """))

    return rows[0] if rows else {"total_revenue": 0, "payment_events": 0}


@app.get("/api/business/payment-methods")
def payment_methods():
    return query_or_503(lambda: run_query("""
        SELECT
            COALESCE(json_extract_scalar(raw_payload, '$.after.payment_type'), 'unknown') AS payment_type,
            COUNT(*) AS total_events,
            COALESCE(
                SUM(CAST(json_extract_scalar(raw_payload, '$.after.payment_value') AS DOUBLE)),
                0
            ) AS total_value
        FROM iceberg.bronze.cdc_events
        WHERE source_table = 'payments'
          AND json_extract_scalar(raw_payload, '$.after.payment_type') IS NOT NULL
          AND COALESCE(json_extract_scalar(raw_payload, '$.op'), '') <> 'd'
        GROUP BY 1
        ORDER BY total_events DESC
        LIMIT 6
    """))


@app.get("/api/business/order-flow")
def order_flow():
    return query_or_503(lambda: run_query("""
        WITH latest_orders AS (
            SELECT
                order_id,
                order_status,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                is_deleted,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY silver_processed_at DESC, kafka_timestamp DESC
                ) AS rn
            FROM iceberg.silver.orders
        )
        SELECT 'Created' AS stage, COUNT(*) AS total_orders
        FROM latest_orders
        WHERE rn = 1 AND NOT is_deleted
        UNION ALL
        SELECT 'Approved' AS stage, COUNT(*) AS total_orders
        FROM latest_orders
        WHERE rn = 1 AND NOT is_deleted AND order_approved_at IS NOT NULL
        UNION ALL
        SELECT 'Processing' AS stage, COUNT(*) AS total_orders
        FROM latest_orders
        WHERE rn = 1 AND NOT is_deleted AND order_status IN ('processing', 'approved', 'invoiced')
        UNION ALL
        SELECT 'Shipped' AS stage, COUNT(*) AS total_orders
        FROM latest_orders
        WHERE rn = 1 AND NOT is_deleted AND order_delivered_carrier_date IS NOT NULL
        UNION ALL
        SELECT 'Delivered' AS stage, COUNT(*) AS total_orders
        FROM latest_orders
        WHERE rn = 1 AND NOT is_deleted AND order_delivered_customer_date IS NOT NULL
    """))


@app.get("/api/business/orders-by-state")
def orders_by_state():
    return query_or_503(lambda: run_query("""
        WITH latest_orders AS (
            SELECT
                order_id,
                customer_id,
                is_deleted,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY silver_processed_at DESC, kafka_timestamp DESC
                ) AS rn
            FROM iceberg.silver.orders
        ),
        latest_customers AS (
            SELECT
                json_extract_scalar(raw_payload, '$.after.customer_id') AS customer_id,
                json_extract_scalar(raw_payload, '$.after.customer_state') AS customer_state,
                ROW_NUMBER() OVER (
                    PARTITION BY json_extract_scalar(raw_payload, '$.after.customer_id')
                    ORDER BY kafka_timestamp DESC, kafka_offset DESC
                ) AS rn
            FROM iceberg.bronze.cdc_events
            WHERE source_table = 'customers'
              AND json_extract_scalar(raw_payload, '$.after.customer_id') IS NOT NULL
        )
        SELECT customer_state AS state, COUNT(*) AS total_orders
        FROM latest_orders orders
        JOIN latest_customers customers
          ON orders.customer_id = customers.customer_id
        WHERE orders.rn = 1
          AND customers.rn = 1
          AND NOT orders.is_deleted
          AND customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY total_orders DESC
        LIMIT 10
    """))


@app.get("/api/business/products-summary")
def products_summary():
    rows = query_or_503(lambda: run_query("""
        WITH product_events AS (
            SELECT
                json_extract_scalar(raw_payload, '$.after.product_id') AS product_id,
                json_extract_scalar(raw_payload, '$.after.product_category_name') AS category
            FROM iceberg.bronze.cdc_events
            WHERE source_table = 'products'
              AND json_extract_scalar(raw_payload, '$.after.product_id') IS NOT NULL
              AND COALESCE(json_extract_scalar(raw_payload, '$.op'), '') <> 'd'
        ),
        seller_events AS (
            SELECT json_extract_scalar(raw_payload, '$.after.seller_id') AS seller_id
            FROM iceberg.bronze.cdc_events
            WHERE source_table = 'sellers'
              AND json_extract_scalar(raw_payload, '$.after.seller_id') IS NOT NULL
              AND COALESCE(json_extract_scalar(raw_payload, '$.op'), '') <> 'd'
        ),
        top_category AS (
            SELECT category
            FROM product_events
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY COUNT(*) DESC
            LIMIT 1
        )
        SELECT
            (SELECT COUNT(DISTINCT product_id) FROM product_events) AS active_products,
            (SELECT COUNT(DISTINCT seller_id) FROM seller_events) AS active_sellers,
            COALESCE((SELECT replace(category, '_', ' ') FROM top_category), 'unknown') AS top_category
    """))

    return rows[0] if rows else {
        "active_products": 0,
        "active_sellers": 0,
        "top_category": "unknown",
    }


@app.get("/api/business/top-sellers")
def top_sellers():
    return query_or_503(lambda: run_query("""
        SELECT
            json_extract_scalar(raw_payload, '$.after.seller_id') AS seller_id,
            COUNT(*) AS item_events,
            COALESCE(
                SUM(CAST(json_extract_scalar(raw_payload, '$.after.price') AS DOUBLE)),
                0
            ) AS gross_value
        FROM iceberg.bronze.cdc_events
        WHERE source_table = 'order_items'
          AND json_extract_scalar(raw_payload, '$.after.seller_id') IS NOT NULL
          AND COALESCE(json_extract_scalar(raw_payload, '$.op'), '') <> 'd'
        GROUP BY 1
        ORDER BY gross_value DESC
        LIMIT 5
    """))


@app.get("/api/pipeline/latency")
def pipeline_latency():
    return query_or_503(lambda: run_query("""
        SELECT
            table_name,
            avg_latency_seconds,
            max_latency_seconds,
            events_measured,
            gold_updated_at
        FROM iceberg.gold.pipeline_latency
        ORDER BY gold_updated_at DESC
    """))


@app.get("/api/pipeline/freshness")
def pipeline_freshness():
    return query_or_503(lambda: run_query("""
        SELECT
            'orders_by_status' AS table_name,
            MAX(gold_updated_at) AS last_updated,
            ROUND(
                (to_unixtime(current_timestamp) - to_unixtime(MAX(gold_updated_at))) / 60.0,
                1
            ) AS minutes_since_update
        FROM iceberg.gold.orders_by_status
    """))


@app.get("/api/pipeline/event-distribution")
def event_distribution():
    return query_or_503(lambda: run_query("""
        SELECT
            CASE json_extract_scalar(raw_payload, '$.op')
                WHEN 'c' THEN 'INSERT'
                WHEN 'r' THEN 'SNAPSHOT'
                WHEN 'u' THEN 'UPDATE'
                WHEN 'd' THEN 'DELETE'
                ELSE COALESCE(json_extract_scalar(raw_payload, '$.op'), 'UNKNOWN')
            END AS operation,
            COUNT(*) AS total_events
        FROM iceberg.bronze.cdc_events
        WHERE raw_payload IS NOT NULL
        GROUP BY 1
        ORDER BY total_events DESC
    """))


@app.get("/api/pipeline/events-timeline")
def events_timeline():
    return query_or_503(lambda: run_query("""
        SELECT
            date_trunc('minute', kafka_timestamp) AS event_minute,
            COUNT(*) AS total_events
        FROM iceberg.bronze.cdc_events
        WHERE kafka_timestamp >= current_timestamp - INTERVAL '60' MINUTE
        GROUP BY 1
        ORDER BY event_minute ASC
    """))


@app.get("/api/pipeline/table-freshness")
def table_freshness():
    return query_or_503(lambda: run_query("""
        SELECT
            source_table AS table_name,
            MAX(bronze_ingested_at) AS last_updated,
            ROUND(
                (to_unixtime(current_timestamp) - to_unixtime(MAX(bronze_ingested_at))) / 60.0,
                1
            ) AS minutes_since_update,
            COUNT(*) AS total_events
        FROM iceberg.bronze.cdc_events
        GROUP BY source_table
        ORDER BY last_updated DESC
    """))


@app.get("/api/pipeline/dlq-summary")
def dlq_summary():
    return query_or_503(lambda: run_query("""
        SELECT
            error_reason,
            COUNT(*) AS invalid_records,
            MAX(dlq_ingested_at) AS last_seen
        FROM iceberg.dlq.invalid_orders
        GROUP BY error_reason
        ORDER BY invalid_records DESC
        LIMIT 5
    """))


@app.get("/api/cdc/recent")
def recent_cdc_events():
    return query_or_503(lambda: run_query("""
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
            bronze_ingested_at
        FROM iceberg.bronze.cdc_events
        WHERE raw_payload IS NOT NULL
        ORDER BY kafka_timestamp DESC, kafka_offset DESC
        LIMIT 12
    """))

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://shopflow:shopflow@localhost:5432/shopflow",
)

STATUSES = ["created", "approved", "processing", "shipped", "delivered", "canceled"]
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "GO", "CE"]
CITIES = ["sao paulo", "rio de janeiro", "belo horizonte", "curitiba", "salvador"]


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def seed_reference_row(conn) -> None:
    conn.execute(
        text("""
            INSERT INTO products (
                product_id,
                product_category_name,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
            )
            VALUES (
                'sim_product_001',
                'health_beauty',
                32,
                240,
                3,
                650,
                20,
                12,
                18
            )
            ON CONFLICT (product_id) DO NOTHING
        """)
    )

    conn.execute(
        text("""
            INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            VALUES ('sim_seller_001', 1001, 'sao paulo', 'SP')
            ON CONFLICT (seller_id) DO NOTHING
        """)
    )


def insert_order(conn) -> str:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    customer_id = new_id("sim_customer")
    order_id = new_id("sim_order")
    status = random.choice(STATUSES[:3])
    state = random.choice(STATES)

    conn.execute(
        text("""
            INSERT INTO customers (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
            )
            VALUES (:customer_id, :customer_unique_id, :zip_code, :city, :state)
        """),
        {
            "customer_id": customer_id,
            "customer_unique_id": new_id("sim_unique"),
            "zip_code": random.randint(1000, 99999),
            "city": random.choice(CITIES),
            "state": state,
        },
    )

    conn.execute(
        text("""
            INSERT INTO orders (
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_estimated_delivery_date,
                updated_at
            )
            VALUES (
                :order_id,
                :customer_id,
                :status,
                :purchase_ts,
                :approved_at,
                :estimated_delivery,
                CURRENT_TIMESTAMP
            )
        """),
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "status": status,
            "purchase_ts": now,
            "approved_at": now if status != "created" else None,
            "estimated_delivery": now + timedelta(days=random.randint(2, 8)),
        },
    )

    conn.execute(
        text("""
            INSERT INTO order_items (
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value
            )
            VALUES (
                :order_id,
                1,
                'sim_product_001',
                'sim_seller_001',
                :shipping_limit,
                :price,
                :freight
            )
        """),
        {
            "order_id": order_id,
            "shipping_limit": now + timedelta(days=2),
            "price": round(random.uniform(24, 420), 2),
            "freight": round(random.uniform(6, 48), 2),
        },
    )

    conn.execute(
        text("""
            INSERT INTO payments (
                order_id,
                payment_sequential,
                payment_type,
                payment_installments,
                payment_value
            )
            VALUES (:order_id, 1, :payment_type, :installments, :value)
        """),
        {
            "order_id": order_id,
            "payment_type": random.choice(["credit_card", "boleto", "voucher", "debit_card"]),
            "installments": random.randint(1, 6),
            "value": round(random.uniform(35, 520), 2),
        },
    )

    return order_id


def update_order(conn) -> str | None:
    row = conn.execute(
        text("""
            SELECT order_id, order_status
            FROM orders
            WHERE order_id LIKE 'sim_order_%'
              AND order_status <> 'delivered'
              AND order_status <> 'canceled'
            ORDER BY random()
            LIMIT 1
        """)
    ).fetchone()

    if row is None:
        return None

    order_id, current_status = row
    next_status = {
        "created": "approved",
        "approved": "processing",
        "processing": random.choice(["shipped", "canceled"]),
        "shipped": "delivered",
    }.get(current_status, "processing")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        text("""
            UPDATE orders
            SET order_status = :status,
                order_approved_at = COALESCE(order_approved_at, :now),
                order_delivered_carrier_date = CASE
                    WHEN :status IN ('shipped', 'delivered') THEN COALESCE(order_delivered_carrier_date, :now)
                    ELSE order_delivered_carrier_date
                END,
                order_delivered_customer_date = CASE
                    WHEN :status = 'delivered' THEN COALESCE(order_delivered_customer_date, :now)
                    ELSE order_delivered_customer_date
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE order_id = :order_id
        """),
        {"order_id": order_id, "status": next_status, "now": now},
    )

    return order_id


def insert_review(conn) -> str | None:
    row = conn.execute(
        text("""
            SELECT order_id
            FROM orders
            WHERE order_id LIKE 'sim_order_%'
              AND order_status = 'delivered'
            ORDER BY random()
            LIMIT 1
        """)
    ).fetchone()

    if row is None:
        return None

    order_id = row[0]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        text("""
            INSERT INTO reviews (
                review_id,
                order_id,
                review_score,
                review_comment_title,
                review_comment_message,
                review_creation_date,
                review_answer_timestamp
            )
            VALUES (
                :review_id,
                :order_id,
                :score,
                :title,
                :message,
                :created_at,
                :answered_at
            )
            ON CONFLICT (review_id, order_id) DO NOTHING
        """),
        {
            "review_id": new_id("sim_review"),
            "order_id": order_id,
            "score": random.randint(3, 5),
            "title": "simulated review",
            "message": random.choice(["fast delivery", "great seller", "as expected"]),
            "created_at": now,
            "answered_at": now + timedelta(minutes=random.randint(3, 45)),
        },
    )

    return order_id


def run(interval_seconds: float, events: int | None) -> None:
    engine = create_engine(DB_URL)
    produced = 0

    print("Starting ShopFlow realtime simulator")
    print(f"Database: {DB_URL}")
    print("Press Ctrl+C to stop")

    with engine.begin() as conn:
        seed_reference_row(conn)

    while events is None or produced < events:
        action = random.choices(
            ["insert_order", "update_order", "insert_review"],
            weights=[0.45, 0.45, 0.10],
            k=1,
        )[0]

        with engine.begin() as conn:
            if action == "insert_order":
                order_id = insert_order(conn)
            elif action == "update_order":
                order_id = update_order(conn)
            else:
                order_id = insert_review(conn)

        if order_id is None:
            continue

        produced += 1
        print(f"{produced:05d} {action:<13} {order_id}")
        time.sleep(interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate realtime CDC events for ShopFlow.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between simulated events.")
    parser.add_argument("--events", type=int, default=None, help="Stop after this many events.")
    args = parser.parse_args()

    run(interval_seconds=args.interval, events=args.events)


if __name__ == "__main__":
    main()

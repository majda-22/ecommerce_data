ALTER TABLE customers
ADD COLUMN IF NOT EXISTS customer_segment TEXT;

ALTER TABLE orders
ADD COLUMN IF NOT EXISTS sales_channel TEXT;

CREATE TABLE IF NOT EXISTS returns (
    return_id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(order_id),
    return_date TIMESTAMP,
    return_reason TEXT,
    return_status TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

UPDATE customers
SET customer_segment = CASE abs(hashtext(customer_id)) % 4
    WHEN 0 THEN 'consumer'
    WHEN 1 THEN 'corporate'
    WHEN 2 THEN 'home_office'
    ELSE 'small_business'
END
WHERE customer_segment IS NULL;

UPDATE orders
SET sales_channel = CASE abs(hashtext(order_id)) % 4
    WHEN 0 THEN 'web'
    WHEN 1 THEN 'mobile'
    WHEN 2 THEN 'marketplace'
    ELSE 'partner'
END
WHERE sales_channel IS NULL;

INSERT INTO returns (
    return_id,
    order_id,
    return_date,
    return_reason,
    return_status,
    updated_at
)
SELECT
    'ret_' || order_id AS return_id,
    order_id,
    COALESCE(order_delivered_customer_date, order_estimated_delivery_date, order_purchase_timestamp)
        + INTERVAL '1 day' AS return_date,
    CASE abs(hashtext(order_id)) % 4
        WHEN 0 THEN 'customer_cancelled'
        WHEN 1 THEN 'late_delivery'
        WHEN 2 THEN 'payment_issue'
        ELSE 'stock_unavailable'
    END AS return_reason,
    CASE order_status
        WHEN 'canceled' THEN 'approved'
        ELSE 'requested'
    END AS return_status,
    CURRENT_TIMESTAMP
FROM orders
WHERE order_status IN ('canceled', 'unavailable')
ON CONFLICT (return_id) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication_rel rel
        JOIN pg_publication pub ON pub.oid = rel.prpubid
        JOIN pg_class cls ON cls.oid = rel.prrelid
        WHERE pub.pubname = 'shopflow_pub'
          AND cls.relname = 'returns'
    ) THEN
        ALTER PUBLICATION shopflow_pub ADD TABLE returns;
    END IF;
END
$$;

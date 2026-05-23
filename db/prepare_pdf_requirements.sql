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

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

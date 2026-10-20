SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    LOWER(TRIM(status)) AS status
FROM {{ source('raw', 'raw_orders') }}
WHERE amount >= 0

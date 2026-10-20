
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.city,
    o.order_date,
    o.amount,
    o.status
FROM {{ ref('stg_orders') }} AS o
LEFT JOIN {{ ref('stg_customers') }} AS c
    ON o.customer_id = c.customer_id

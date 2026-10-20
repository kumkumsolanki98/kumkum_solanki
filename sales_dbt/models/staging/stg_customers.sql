SELECT
    customer_id,
    TRIM(customer_name) AS customer_name,
    INITCAP(TRIM(city)) AS city
FROM {{ source('raw', 'raw_customers') }}
WHERE customer_id IS NOT NULL

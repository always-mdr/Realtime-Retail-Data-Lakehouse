{{ config(
    materialized='table',
    pre_hook="SET unsafe_enable_version_guessing=true;"
) }}

SELECT
    product_id,
    count(*) as total_orders,
    sum(price) as total_revenue,
    max(timestamp) as last_order_time
FROM iceberg_scan('s3://retail-lake/iceberg_warehouse/retail/orders', allow_moved_paths=true)
GROUP BY product_id
ORDER BY total_revenue DESC
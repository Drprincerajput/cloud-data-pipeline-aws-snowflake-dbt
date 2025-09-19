WITH stg_sales AS (
    SELECT
        *
    FROM
        {{ ref('stg_sales') }}
)
SELECT
    ordered_at,
    product_id,
    SUM(quantity_sold) AS daily_quantity_sold,
    SUM(
        quantity_sold * unit_price
    ) AS daily_revenue
FROM
    stg_sales
GROUP BY
    1,
    2

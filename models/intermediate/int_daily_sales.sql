WITH daily_sales AS (
    SELECT
        *
    FROM
        {{ ref('int_sales_aggregated_to_daily') }}
),
date_spine AS (
    SELECT
        date_day
    FROM
        {{ ref('int_date_spine') }}
),
all_products AS (
    SELECT
        DISTINCT product_id
    FROM
        {{ ref('stg_products') }}
),
-- creating A base TABLE WITH every product for every DAY 
products_per_day AS (
    SELECT
        d.date_day,
        p.product_id
    FROM
        date_spine d
        CROSS JOIN all_products p
)
SELECT
    ppd.date_day,
    ppd.product_id,
    COALESCE(
        s.daily_quantity_sold,
        0
    ) AS daily_quantity_sold,
    COALESCE(
        s.daily_revenue,
        0
    ) AS daily_revenue
FROM
    products_per_day ppd
    LEFT JOIN daily_sales s
    ON ppd.date_day = s.ordered_at
    AND ppd.product_id = s.product_id

WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            's3_source',
            'raw_sales'
        ) }}
),
renamed AS (
    SELECT
        order_id,
        order_date :: DATE AS ordered_at,
        product_id,
        quantity_sold,
        unit_price :: DECIMAL(
            18,
            2
        ) AS unit_price
    FROM
        source
)
SELECT
    *
FROM
    renamed

WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            's3_source',
            'raw_products'
        ) }}
),
renamed AS (
    SELECT
        product_id,
        product_name,
        category
    FROM
        source
)
SELECT
    *
FROM
    renamed

WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            's3_source',
            'raw_marketing_events'
        ) }}
),
renamed AS (
    SELECT
        event_date :: DATE AS event_date,
        event_name,
        description AS event_description
    FROM
        source
)
SELECT
    *
FROM
    renamed

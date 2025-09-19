WITH stg_sales AS (
    SELECT
        *
    FROM
        {{ ref('stg_sales') }}
),
stg_marketing_events AS (
    SELECT
        *
    FROM
        {{ ref('stg_marketing_events') }}
)
SELECT
    d.date_day,
    e.event_name,
    e.event_description
FROM
    (
        -- this wraps THE macro output IN A subquery,isolating its WITH clause 
        {{ dbt_date.get_date_dimension(
            '2024-01-01',
            '2025-12-31'
            )
         }}
    ) AS d
    LEFT JOIN stg_marketing_events AS e
    ON d.date_day = e.event_date

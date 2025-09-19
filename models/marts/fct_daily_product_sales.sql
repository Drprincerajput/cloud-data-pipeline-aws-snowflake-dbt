with daily_sales as (
    select * from {{ ref('int_daily_sales') }}
),

date_spine as (
    select
        date_day,
        event_name
    from {{ ref('int_date_spine') }}
),

final as (
    select
        ds.date_day,
        ds.product_id,
        ds.daily_quantity_sold,
        ds.daily_revenue,
        d.event_name,

        
        lag(ds.daily_quantity_sold, 1) over (partition by ds.product_id order by ds.date_day) as sales_lag_1_day,
        lag(ds.daily_quantity_sold, 7) over (partition by ds.product_id order by ds.date_day) as sales_lag_7_days,
        lag(ds.daily_quantity_sold, 365) over (partition by ds.product_id order by ds.date_day) as sales_lag_1_year,

        
        avg(ds.daily_quantity_sold) over (partition by ds.product_id order by ds.date_day rows between 6 preceding and current row) as sales_rolling_avg_7_days,
        avg(ds.daily_quantity_sold) over (partition by ds.product_id order by ds.date_day rows between 27 preceding and current row) as sales_rolling_avg_28_days,

        sum(ds.daily_quantity_sold) over (partition by ds.product_id order by ds.date_day rows between 6 preceding and current row) as sales_rolling_sum_7_days,
        sum(ds.daily_quantity_sold) over (partition by ds.product_id order by ds.date_day rows between 27 preceding and current row) as sales_rolling_sum_28_days

    from daily_sales ds
    left join date_spine d on ds.date_day = d.date_day
)

select * from final

with bounds as (
    select min(event_date) as min_date, max(event_date) as max_date
    from {{ ref('stg_cowrie_events') }}
),

days as (
    select unnest(generate_series(min_date, max_date, interval 1 day))::date as date_day
    from bounds
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    monthname(date_day) as month_name,
    weekofyear(date_day) as iso_week,
    isodow(date_day) as day_of_week,-- 1 = luni, 7 = duminica
    dayname(date_day) as day_name,
    isodow(date_day) in (6, 7) as is_weekend
from days

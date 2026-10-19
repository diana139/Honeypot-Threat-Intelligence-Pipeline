-- dim_date is generated from min/max event_date, so it must be continuous (no missing days)
with bounds as (
    select
        count(*) as days,
        min(date_day) as min_day,
        max(date_day) as max_day
    from {{ ref('dim_date') }}
)

select *
from bounds
where days != date_diff('day', min_day, max_day) + 1

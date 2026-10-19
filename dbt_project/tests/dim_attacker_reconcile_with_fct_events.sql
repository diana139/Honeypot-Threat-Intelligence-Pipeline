-- every event belongs to exactly one attacker, so the total in dim_attacker
-- must equal the number of events in fct_events
with counts as (
    select
        (select count(*) from {{ ref('fct_events') }}) as events_expected,
        (select sum(events) from {{ ref('dim_attacker') }}) as events_actual
)

select *
from counts
where events_expected != events_actual

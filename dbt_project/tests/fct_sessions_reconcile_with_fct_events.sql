-- fct_sessions aggregates fct_events: every session in fct_events appears once,
-- and the sum of events over sessions equals the number of events
with counts as (
    select
        (select count(distinct session_id) from {{ ref('fct_events') }}) as fct_sessions_expected,
        (select count(*) from {{ ref('fct_sessions') }}) as fct_sessions_actual,
        (select count(*) from {{ ref('fct_events') }}) as events_expected,
        (select sum(events) from {{ ref('fct_sessions') }}) as events_actual
)

select *
from counts
where fct_sessions_expected != fct_sessions_actual
    or events_expected != events_actual

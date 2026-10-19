-- fct_sessions agrega fct_events: fiecare sesiune din fct_events apare o data,
-- iar suma evenimentelor pe sesiuni e egala cu numarul de evenimente
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
